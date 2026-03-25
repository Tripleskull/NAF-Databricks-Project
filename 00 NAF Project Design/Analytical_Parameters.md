# Analytical Parameters

> **Scope**: Every tuneable number in the analytical pipeline — Elo engine constants, tournament weighting, burn-in thresholds, minimum-games eligibility, window sizes, binning schemes.
> **Companion to**: `NAF_Design_Specification.md` (architecture and policies), `03_style_guides.md` (code formatting).
> **Supersedes**: `Rating_System_Spec.md` (retire after this file is adopted).
> **Config table**: `naf_catalog.gold_dim.analytical_config` — singleton table built in `310_NAF_gold_dim.py`. All parameters below are stored here and consumed by downstream notebooks via CTE or Python read.
> **Rule**: Changes to any parameter here constitute a model change. Update this document first, update the config table definition in 310, then re-run affected pipelines.

---

## 1. Elo Rating Engine

### 1.1 Constants

| Parameter | Value | Code location |
|---|---|---|
| Initial rating | `150.0` | `320_NAF_gold_fact.py` |
| Win probability scale | `150.0` | `320_NAF_gold_fact.py` |

A 150-point gap gives ~91% expected win probability — analogous to 400 in standard chess Elo but compressed to fit the Blood Bowl rating range.

### 1.2 Win probability

```
P(win) = 1 / (1 + 10^((R_opponent - R_self) / 150))
```

### 1.3 Rating update

```
R_new = R_old + k_value × (actual_result - expected_result)
```

`actual_result`: 1.0 (win), 0.5 (draw), 0.0 (loss).

### 1.4 Execution model

Ratings are computed sequentially on the Spark driver (not distributed) because each game depends on the prior game's output. Games are ordered by `event_timestamp` within scope.

### 1.5 Scopes

| Scope | `race_id` | Grain | Description |
|---|---|---|---|
| GLOBAL | `0` | One rating per coach | Updated by every game regardless of race played |
| RACE | `<> 0` | One rating per (coach, race) | Updated only for games where the coach played that race |

Output: `naf_catalog.gold_fact.rating_history_fact` — columns include `rating_before`, `rating_after`, `rating_delta`, `scope`, `rating_system`.

---

## 2. Tournament Parameters

Computed in `naf_catalog.gold_dim.tournament_parameters` (deterministic helper table — see `NAF_Design_Specification.md` §3.3).

### 2.1 Effective participants (`n_eff`)

| Tournament type | Formula |
|---|---|
| Major (`is_major_tournament = TRUE`) | `60` (fixed) |
| Minor | `LEAST(n_participant_coaches, 32)` |

`n_participant_coaches` = distinct coaches with at least one game in the tournament.

### 2.2 K-value

```
k_value = CAST(ROUND(2 × SQRT(n_eff)) AS INT)
```

Reference values:

| Scenario | n_eff | k_value |
|---|---|---|
| Major tournament | 60 | 15 |
| Minor, 100 coaches | 32 | 11 |
| Minor, 16 coaches | 16 | 8 |
| Minor, 4 coaches | 4 | 4 |

---

## 3. Coach Rating Phases

Phases define windows over a coach's rating history for computing distribution statistics. Applied in `331_NAF_gold_summary_coach.py`.

### 3.1 Burn-in thresholds

| Scope | Threshold | Rule |
|---|---|---|
| GLOBAL | 50 games | `game_number_asc > 50` (strictly greater-than) |
| RACE | 25 games | `game_number_asc > 25` (strictly greater-than) |

Strictly-greater-than is intentional: the burn-in games themselves are excluded from post-threshold statistics because the rating is still converging from the initial value.

### 3.2 Phase definitions

| Phase | Description | Population |
|---|---|---|
| ALL | Every game in scope | Always populated |
| POST_THRESHOLD | Games strictly after burn-in | Only for coaches with `games > threshold`; NULL otherwise |
| LAST_50 | Most recent `min(50, total_games)` | Always populated; equals ALL when `total_games < 50` |

### 3.3 Last-N window

| Scope | Window size | Code location |
|---|---|---|
| GLOBAL | 50 games | `331_NAF_gold_summary_coach.py` |
| RACE | 50 games | `331_NAF_gold_summary_coach.py` |

Optional UI toggle: `require_full_last_50_window` can gate display to coaches with >= 50 games in scope.

### 3.4 Distribution statistics (metric types)

| Metric type | Computation | Notes |
|---|---|---|
| PEAK | `MAX(rating_after)` within phase | Highest rating achieved |
| MEAN | `AVG(rating_after)` within phase | Average over all rated games in phase |
| MEDIAN | `PERCENTILE_APPROX(rating_after, 0.5)` within phase | Approximate median |

Pre-computed in `gold_summary`. Never recomputed downstream.

### 3.5 Column naming pattern

GLOBAL (from `coach_rating_global_elo_summary`): `global_elo_{peak|mean|median}_{all|post_threshold|last_50}`

RACE (from `coach_race_summary`): `elo_{peak|mean|median}_{all|post_threshold|last_50}`

---

## 4. Minimum-Games Eligibility

These thresholds gate whether a coach/nation qualifies for specific analytical outputs. They are separate from burn-in (which governs which games are included in a phase); eligibility governs whether the output is shown at all.

### 4.1 Coach-level eligibility

| Flag | Scope | Threshold | Rule |
|---|---|---|---|
| `is_valid_min_games_race_performance` | RACE | 10 games | `games_played >= 10` |
| `is_valid_min_games_race_elo` | RACE | 25 games | `elo_games_with_race > 25` |
| `is_valid_post_threshold_race_elo` | RACE | 25 games | Same as above (burn-in equivalent) |
| `is_valid_last_50_race_elo` | RACE | 50 games | `elo_games_with_race >= 50` |

GLOBAL scope uses `global_elo_games` count against `threshold_games` (50) and `last_n_games_window` (50) stored per row.

### 4.2 Nation-level eligibility

| Context | Threshold | Rule | Code location |
|---|---|---|---|
| Nation GLO stable sample | 50 games | `games_played >= 50` | `332_NAF_gold_summary_nation.py` |
| Nation GLO coach inclusion | 50 games | `coach_game_number >= 50` | `332_NAF_gold_summary_nation.py` |
| Nation race-level metrics | 5 games | `games_played >= 5` | `332_NAF_gold_summary_nation.py` |

The `is_valid_glo` flag marks nations/coaches that pass the stable-sample threshold.

---

## 5. Binning Schemes

### 5.1 Opponent GLO bins (fixed 4-bin scheme)

Used for "binned result by opponent strength" charts at both coach and nation level.

| Bin index | Range | Label |
|---|---|---|
| 1 | 0–150 | 0–150 |
| 2 | 150–200 | 150–200 |
| 3 | 200–250 | 200–250 |
| 4 | 250+ | 250+ |

Objects using this scheme:

| Object | Schema | Grain | Purpose |
|---|---|---|---|
| `coach_opponent_median_glo_bin_summary` | `gold_summary` | (coach_id, bin_index) | Coach-level opponent bin W/D/L (331) |
| `coach_opponent_glo_bin_summary` | `gold_summary` | (coach_id, bin_index) | Coach-level opponent bin helper for nation views (332) |
| `nation_opponent_elo_bin_wdl` | `gold_summary` | (nation_id, metric_type, bin_index) | Nation-level opponent bin W/D/L (332) |

Bins are defined inline via `VALUES` clauses — no configurable bin framework.

### 5.2 GLO rating distribution bins (fixed 25-point width)

Used for density/histogram charts comparing nations vs World.

| Parameter | Value |
|---|---|
| Bin width | 25 GLO points |
| Example bins | 100–125, 125–150, …, 275–300, 300+ |

Object: `nation_glo_binned_distribution` (`gold_summary`, 332).

### 5.3 Race Elo bins

Not yet implemented. When needed, mirror the global pattern with bin edges calibrated to the (typically narrower) race rating distribution.

### 5.4 Elite rivalry threshold

| Parameter | Value | Config column | Purpose |
|---|---|---|---|
| Elite GLO median threshold | 200.0 | `elite_glo_median_threshold` | Minimum GLO median for both coaches in a game to qualify as "elite" |

Used by `nation_elite_rivalry_summary` (332). Only games where both participants have `glo_median >= threshold` are included in the elite rivalry scoring.

---

## 6. Parameter Summary (quick reference)

| Parameter | Value | Config column | Section |
|---|---|---|---|
| Initial Elo rating | 150.0 | `elo_initial_rating` | §1.1 |
| Elo scale | 150.0 | `elo_scale` | §1.1 |
| n_eff (major) | 60 | `n_eff_major` | §2.1 |
| n_eff (minor cap) | 32 | `n_eff_minor_cap` | §2.1 |
| k_value | round(2 × √n_eff) | *(derived in tournament_parameters)* | §2.2 |
| Burn-in GLOBAL | 50 games (strict >) | `threshold_global_elo` | §3.1 |
| Burn-in RACE | 25 games (strict >) | `threshold_race_elo` | §3.1 |
| Last-N window (all scopes) | 50 games | `last_n_games_window` | §3.3 |
| Min games: race performance | 10 | `min_games_race_performance` | §4.1 |
| Min games: race Elo | 25 | `threshold_race_elo` | §4.1 |
| Min games: last-50 race Elo | 50 | `last_n_games_window` | §4.1 |
| Min games: nation GLO | 50 | `min_games_nation_glo` | §4.2 |
| Min games: nation race | 5 | `min_games_nation_race` | §4.2 |
| Rivalry games cap | 100 | `rivalry_games_cap` | *(unused — future)* |
| Rivalry weight: games | 1/6 | `rivalry_w_games` | *(unused — future)* |
| Rivalry weight: closeness | 2/3 | `rivalry_w_closeness` | *(unused — future)* |
| Rivalry weight: share | 1/6 | `rivalry_w_share` | *(unused — future)* |
| Elite GLO median threshold | 200 | `elite_glo_median_threshold` | §5.4 |
| Form window games | 50 | `form_window_games` | §8.1 |
| Form min games for pctl | 50 | `form_min_games_for_pctl` | §8.1 |
| SSM v1 prior sigma | 50.0 | `ssm1_prior_sigma` | §9.1 |
| SSM v1 phi | 1.000 | `ssm1_phi` | §9.1 |
| SSM v1 σ² process | 2.0 | `ssm1_sigma2_process` | §9.1 |
| SSM v1 σ² obs | 0.02 | `ssm1_sigma2_obs` | §9.1 |
| SSM v2 σ² obs | 0.10 | `ssm2_sigma2_obs` | §10.1 |
| SSM v2 q_time | 2.00 | `ssm2_q_time` | §10.1 |
| SSM v2 q_game | 0.025 | `ssm2_q_game` | §10.1 |
| SSM v2 v_scale | 24.0 | `ssm2_v_scale` | §10.1 |
| SSM v2 prior sigma | 50.0 | `ssm2_prior_sigma` | §10.2 |
| SSM v2 max days | 180.0 | `ssm2_max_days` | §10.2 |
| SSM v2 v_base | 0.25 | `ssm2_v_base` | §10.2 |
| SSM v2 v_decay | 0.90 | `ssm2_v_decay` | §10.2 |
| SSM v2 v_min | 0.00 | `ssm2_v_min` | §10.2 |
| SSM v2 v_max | 16.0 | `ssm2_v_max` | §10.2 |

---

## 8. Form Score (Phase 1)

### 8.1 Parameters

| Parameter | Value | Config column | Purpose |
|---|---|---|---|
| Form window games | 50 | `form_window_games` | Number of most-recent GLOBAL games used to compute form score |
| Form min games for pctl | 50 | `form_min_games_for_pctl` | Minimum games in form window to be eligible for percentile ranking |

### 8.2 Computation

**Form score** measures how much a coach has recently over- or under-performed relative to Elo expectations:

```
form_score = SUM(result_numeric - score_expected) over last N GLOBAL games
```

Where `result_numeric` is 1 (win) / 0.5 (draw) / 0 (loss) and `score_expected` is the Elo-predicted probability.

A positive form score means the coach is winning more than expected; negative means losing more than expected.

### 8.3 Percentile ranking

Coaches with `form_games_in_window >= form_min_games_for_pctl` are ranked by `PERCENT_RANK()` on `form_score`, scaled to 0–100.

Coaches below the threshold get `form_pctl = NULL` and `form_label = NULL`.

### 8.4 Form labels (fixed — not tuneable)

| Percentile range | Label |
|---|---|
| >= 90 | Strong Form |
| 70–89 | Good Form |
| 30–69 | Neutral |
| 10–29 | Poor Form |
| < 10 | Weak Form |

### 8.5 Object

| Object | Schema | Grain | Purpose |
|---|---|---|---|
| `coach_form_summary` | `gold_summary` | 1 row per `coach_id` | Form score, percentile, and label |

---

## 9. SSM v1 Rating Engine (Random-Walk EKF)

### 9.1 Parameters

| Parameter | Value | Config column | Purpose |
|---|---|---|---|
| Prior sigma | 50.0 | `ssm1_prior_sigma` | Prior SD for new coaches (P₀ = σ²) |
| Phi (AR coefficient) | 1.000 | `ssm1_phi` | Mean-reversion coefficient (1.0 = no reversion) |
| Process noise (σ²) | 2.0 | `ssm1_sigma2_process` | Per-game process noise variance |
| Observation noise (σ²) | 0.02 | `ssm1_sigma2_obs` | Observation noise on logistic scale |

### 9.2 State model

```
mu_predict = phi × mu_prev + (1 - phi) × MU_GLOBAL
P_predict  = phi² × P_prev + sigma2_process
```

### 9.3 Observation model

Uses the same logistic link as Elo: `P(win) = 1 / (1 + 10^((θ_opp − θ_self) / elo_scale))`.

Linearised via the extended Kalman filter (EKF). The Jacobian `H = ln(10)/elo_scale × p × (1 − p)` feeds into the standard Kalman update.

Output: `naf_catalog.gold_fact.ssm_rating_history_fact`

---

## 10. SSM v2 Rating Engine (Time-Aware + Adaptive Volatility)

### 10.1 Tunable hyperparameters

| Parameter | Value | Config column | Purpose |
|---|---|---|---|
| Observation noise (σ²_obs) | 0.10 | `ssm2_sigma2_obs` | Observation noise on logistic scale |
| Time process noise (q_time) | 2.00 | `ssm2_q_time` | Variance added per √day of inactivity |
| Game process noise (q_game) | 0.025 | `ssm2_q_game` | Baseline per-game process noise |
| Volatility scale (v_scale) | 24.0 | `ssm2_v_scale` | Scale factor for adaptive volatility |

### 10.2 Structural parameters

| Parameter | Value | Config column | Purpose |
|---|---|---|---|
| Prior sigma | 50.0 | `ssm2_prior_sigma` | Prior SD for new coaches |
| Max days | 180.0 | `ssm2_max_days` | Cap for days-since-last-game in √day scaling |
| Volatility baseline | 0.25 | `ssm2_v_base` | Base volatility added to process variance |
| Volatility decay (EWMA) | 0.90 | `ssm2_v_decay` | Exponential decay for shock tracker |
| Volatility floor | 0.00 | `ssm2_v_min` | Minimum adaptive volatility |
| Volatility ceiling | 16.0 | `ssm2_v_max` | Maximum adaptive volatility |

### 10.3 State model

No mean reversion. Process variance is time-aware with adaptive volatility:
```
P_pred = P_prev + q_time × √(min(Δt, max_days)) + q_game + volatility
volatility = clip(v_base + v_scale × shock_ewma, v_min, v_max)
```

### 10.4 Observation model

Same logistic link and EKF linearisation as SSM v1 (§9.3).

### 10.5 Uncertainty-aware predictions (probit approximation)

Used in evaluation (322) and potentially in future dashboards:
```
S_eff = S × √(1 + UA_COEFF × (σ²_self + σ²_opp))
UA_COEFF = π/8 × (ln10 / elo_scale)²
```

Output: `naf_catalog.gold_fact.ssm2_rating_history_fact`

---

## 7. Change Policy

Any change to a parameter in this document is a model change that affects downstream analytics. Protocol:

1. Update this document.
2. Update the corresponding value in the `analytical_config` table definition in `310_NAF_gold_dim.py`.
3. Re-run 310 to rebuild the config table.
4. Re-run all dependent notebooks (320 → 321 → 331/332 → presentation).
