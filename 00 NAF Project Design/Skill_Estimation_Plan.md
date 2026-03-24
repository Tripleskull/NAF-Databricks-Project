# Skill Estimation — Design Options

## Problem Statement

The current GLO (Global NAF_ELO) rating is a point estimate with no uncertainty quantification. It treats all coaches identically regardless of game count, gives permanent weight to noisy early games, and cannot distinguish between "confidently rated at 190" and "somewhere between 160 and 220." A better skill estimate would provide both a point estimate and a credible uncertainty band.

## Options Considered

### Option 1: Fixed-Window Median Elo

Take the median Elo over the last N games (e.g., Last 50, already configurable via `analytical_config`).

**Pros:** Simple, robust to outliers, infrastructure already exists.
**Cons:** N games spans very different time periods depending on activity level — incomparable across coaches. No uncertainty measure. Hard cutoff discards older data abruptly.

### Option 2: Dynamic-Window Median Elo

Window size grows with total games played (e.g., `min(total, max(30, sqrt(total) * k))`).

**Pros:** Adapts to game count; rookies use full history, veterans use a meaningful recent slice.
**Cons:** Ad hoc formula with a tuning parameter but no clear optimisation target. Still no uncertainty. Still a hard cutoff.

### Option 3: Exponentially Weighted Mean Elo

Weight each Elo observation by `λ^(games_ago)` where `λ ∈ (0,1)`. Weighted mean = `SUM(rating × λ^games_ago) / SUM(λ^games_ago)`.

**Pros:** No hard cutoff — old games decay smoothly. Single tunable parameter (λ). Easy to implement in SQL window functions. λ goes into `analytical_config`.
**Cons:** No principled uncertainty. Weighted SD can be computed alongside, but it conflates real skill change with game-level noise (opponent strength, dice luck). A coach drawing every game against evenly matched opponents shows low SD, but that reflects stable inputs, not genuine certainty about skill.

#### Option 3b: Exponentially Weighted Mean Elo + Analytical Uncertainty (JCR Variant)

Same exponential weighting as Option 3, but with uncertainty derived analytically from the effective sample size rather than from weighted SD.

The exponential weights give an **effective N**: `n_eff = SUM(w)² / SUM(w²)`. With λ = 0.98, a coach with 100+ games has an effective sample of roughly 50 games. Uncertainty is then modelled as a weighted coin-flip problem: given `n_eff` weighted games with an observed win rate, what is the variance of the estimate? Variance is highest when results are 50:50 (hardest to distinguish from random) and lowest when heavily skewed.

The primary use case for this uncertainty is a **lower confidence bound** for team selection: rank coaches by `estimated_skill - k × SD` rather than the point estimate alone. This penalises coaches with few games or volatile results — a conservative "we're confident this coach is at least this good" ranking.

**Pros:** Simple to compute alongside Option 3. No model fitting required. Gives a principled uncertainty for the ranking/selection use case. Easy to explain.
**Cons:** Opponent strength enters only implicitly (via the Elo values being averaged). Cannot distinguish between 50 games against strong opponents (more informative, should give tighter uncertainty) vs 50 games against weak opponents (less informative). The SSM handles this naturally because each observation update accounts for the opponent's current skill and uncertainty. Also does not model skill trajectory (improving/declining) — only "how confident are we right now."

### Option 4: State-Space Model with Kalman Filter (Recommended)

Model each coach's latent skill as an AR(1) mean-reverting process observed through noisy game results.

**State equation (skill evolution between games):**
```
θ_t = μ + φ(θ_{t-1} - μ) + η_t     where η_t ~ N(0, σ²_process)
```

**Observation equation (game outcome):**
```
P(win) = 1 / (1 + 10^((θ_j - θ_i) / ELO_SCALE))
```

The Kalman state per coach is `(θ, P)` — point estimate and variance. After each game, both coaches' states update based on the observed result.

**Key design choices:**

- **Elo-scale compatibility:** The SSM operates on the same rating scale as the existing Elo engine. Initial state `θ₀ = 150` (matching `elo_initial_rating`), logistic scaling factor = 150 (matching `elo_scale`). SSM output is directly comparable in magnitude to current Elo ratings — a coach at SSM 200 means the same as Elo 200.
- **Flat observation noise (no tournament modulation):** The existing Elo engine uses per-tournament `k_value` as a proxy for game importance (larger events get higher K). For the SSM this is unnecessary — the informativeness of a game is already captured by the opponent's skill (`θ_j`) and uncertainty (`P_j`). A win against a well-estimated strong opponent is automatically more informative than one against a poorly-estimated weak opponent, regardless of tournament size. This avoids the problem that some small events have elite fields (e.g., a 6-person invitational with top coaches) while some large events have mixed skill levels. A single flat `σ²_obs` for all games keeps the model simple and principled.
- **Very slow mean-reversion (φ close to 1, e.g., 0.995):** Nearly invisible over 50–100 games, but prevents skill estimates from drifting to infinity and provides long-run stability. Only matters for coaches with extreme ratings or long gaps.
- **Game-indexed, not time-indexed:** The process evolves per game, not per calendar day. Inactivity as a concept doesn't apply — a coach with 200 games who hasn't played in 2 years looks the same as one who played yesterday.
- **Non-Gaussian observations:** Game results are W/D/L, not Gaussian. Requires an Extended Kalman Filter (EKF) to linearise the logistic observation model around the current estimate. This is a few extra lines of code, not a fundamental obstacle.
- **Early rating noise handled automatically:** The filter starts with wide prior variance (`P₀ = 50²`). Early games have high Kalman gain (the filter knows it's still learning). As games accumulate, variance shrinks and the estimate stabilises. Noisy early ratings get low effective weight naturally — no special handling needed.

**Pros:** Principled separation of process noise (real skill change) from observation noise (game-level luck). Uncertainty grows when there's no data and shrinks with observations. Handles early-career noise automatically. Mean-reversion provides long-run stability. Produces a predictive distribution per game (useful for tournament predictions and team selection confidence). Opponent strength and uncertainty drive informativeness — no need for tournament-level heuristics.
**Cons:** More complex to implement than Options 1–3. Sequential processing per coach (not parallelisable across games within a coach, but easily parallelisable across coaches). Three tunable parameters (φ, σ²_process, σ²_observation).

## Existing Elo Engine Parameters

From `analytical_config` (310) and the Elo engine (320):

- `elo_initial_rating = 150.0` — starting rating for new coaches
- `elo_scale = 150.0` — logistic scaling factor: `P(win) = 1 / (1 + 10^((r_opp - r_you) / 150))`
- `k_value` — per-tournament, from `tournament_parameters` (varies by tournament size/format)
- `n_eff` — effective number of games per tournament (stored per game in `rating_history_fact`)

The Elo update rule: `r_new = r_old + k_value × (result - expected_score)`

The SSM replaces the fixed K-factor with an adaptive Kalman gain, but uses the same logistic observation model and scale. The per-tournament `k_value` is not needed in the SSM — opponent skill and uncertainty already capture game informativeness, making tournament-level modulation redundant (and potentially misleading for small elite events).

## Recommendation

Two viable approaches remain. Options 1–2 were rejected (hard cutoffs, no uncertainty). The choice is between 3b and 4.

### Option 3b — Exponentially Weighted + Analytical Uncertainty

The pragmatic choice. Simple, no model fitting, gives a useful lower confidence bound for team selection. Implements in SQL. The main limitation is that opponent strength only enters implicitly (baked into the Elo values being averaged) — 50 games against strong opponents and 50 games against weak opponents produce the same uncertainty, which is wrong in principle.

If the primary use case is team selection ranking with a variance penalty, this may be sufficient.

### Option 4 — State-Space Model with Extended Kalman Filter

The principled choice. Separates process noise from observation noise, accounts for opponent strength and uncertainty explicitly in each update, models skill trajectory over time, and handles early-career noise automatically. The observation model `P(win) = logistic(θ_i - θ_j)` means a win against a strong opponent with tight uncertainty is more informative than a win against a weak or poorly-estimated opponent — this is the key advantage over 3b.

More complex to implement (Python EKF, sequential processing, three hyperparameters to tune), but the data volume is manageable and the implementation is a one-time cost.

### Decision: Option 4 (implemented)

Option 4 was selected and implemented in notebook `321_NAF_gold_fact_ssm.py`. Two versions exist:

**SSM v1** (`ssm_rating_history_fact`): Game-indexed random walk with EKF. Originally designed with AR(1) mean reversion (φ=0.995), but tuning showed mean reversion was harmful — it pulled SSM μ far below Elo over hundreds of games. Final v1 uses φ=1.0 (pure random walk). Constant process noise per game. Opponent uncertainty propagated into innovation variance.

**SSM v2** (`ssm2_rating_history_fact`): Time-aware uncertainty growth with adaptive volatility. No mean reversion. Process variance depends on time gap (`q_time × √(min(Δt, 180))`), a baseline per-game term (`q_game`), and a coach-level volatility term driven by EWMA of squared innovations. See `ssm_model_outline_v2_with_suggestions.md` for full design spec.

Both models are in active tuning. Calibration target: Elo should fall inside SSM ±2σ approximately 95% of the time for coaches with 100+ games.

## External Input (Jan Christian Refsgaard, 2026-03-09)

JCR independently arrived at the same exponential weighting idea (λ = 0.98, which he framed as "49/50 weight on previous 50 games"). His key contributions:

- **Analytical uncertainty via effective sample size:** Rather than weighted SD, compute `n_eff` from the weights and model uncertainty as a weighted coin-flip variance. This avoids the problem of weighted SD conflating signal and noise.
- **Lower evidence bound for team selection:** Rank coaches by `estimated_skill - k × SD` to penalise high-uncertainty estimates. Coaches with few games or volatile results get ranked lower — conservative "we're confident this coach is at least this good."
- **Opponent strength matters for uncertainty:** 50 games against hard opponents should give different uncertainty than 50 against easy opponents, because variance is highest on a 50:50 coin. This is a limitation of the analytical approach and a strength of the SSM.
- **Visualisation:** Suggested using KDE plots more; acknowledged they're less intuitive for general audiences than box/histogram plots.

## Implementation (Actual)

### Notebook: `321_NAF_gold_fact_ssm.py`

**Position**: Runs after 320 (Elo engine), before 331 (coach summaries). Can run in parallel with 331+.

**Input**: `game_feed_for_ratings_fact` (same ordered feed as the Elo engine) + `analytical_config` (for initial rating and Elo scale).

**Implementation**: Pure Python sequential loop over all games in `game_index` order. Both coaches in each game are predicted first, then observation updates run symmetrically using each other's predicted states. No `applyInPandas` — the global game ordering requires cross-coach state, so it processes all coaches in a single pass.

**Output**:
- `ssm_rating_history_fact` — v1, 1 row per (game_id, coach_id)
- `ssm2_rating_history_fact` — v2, 1 row per (game_id, coach_id)

Both contain: mu/sigma before and after, opponent mu/sigma, innovation, Kalman gain, score_expected, result_numeric, plus diagnostic columns (v2 adds days_since_prev, time_scale, process_variance_added, volatility before/after, shock_ewma).

**Hyperparameters**: Currently hardcoded in the notebook. Plan to move to `analytical_config` once tuning stabilises.

### Key Lessons from Tuning

1. **Mean reversion is harmful** when tracking Elo (which has no mean reversion). φ=0.995 looked harmless but caused cumulative drag of ~0.5% × N games, pulling SSM μ far below Elo for active coaches.
2. **σ²_obs must be small** (0.01–0.05). Large σ²_obs dominates the innovation variance S, making every game uninformative and preventing σ from shrinking.
3. **Prior σ should be empirically grounded** — set from SD of median-career Elo for coaches with 50+ games.
4. **Opponent uncertainty propagation** (H²×P_opp in S) is a deliberate design choice that makes games against poorly-estimated opponents less informative.

### Target Visualisation

Per-coach skill trajectory plot:
- X-axis: game number (1, 2, 3, ...)
- Y-axis: rating (Elo-compatible scale, starting at 150)
- Solid line: SSM skill estimate `θ_t`
- Shaded band: `θ_t ± 2σ_t` (95% confidence interval)
- Optional: raw Elo trajectory as dotted line for comparison

The plot should show wide uncertainty at the start (few games), narrowing as the filter learns, and potentially widening again if results become volatile. The rating values should be directly comparable to existing Elo — same axis, same scale, same interpretation.

### Open Questions

- **330 core summary notebook:** The world-level reference objects (`world_race_elo_quantiles`, `world_glo_metric_quantiles`) and potentially SSR-related world rankings need a shared home that runs before 331/332. Parked for next week.
- **Race-level SSM:** Should the SSM also apply to race-specific Elo, or only GLO? Race Elo has sparser data per coach — wider uncertainties, potentially less stable.
- **Dashboard UX:** How to display uncertainty bands — error bars, shaded regions, confidence intervals in tables?
- **Opponent uncertainty propagation:** Should the SSM account for the opponent's uncertainty (wide `P_j`) when computing the observation update? This would make wins against poorly-estimated opponents less informative. Adds complexity but is more principled — and is the mechanism that replaces tournament-level observation noise modulation. Leaning yes.
