# SSM Rating Engine — Design Document

> **Status:** Implemented. SSM v1 and v2 are in `321_NAF_gold_fact_ssm.py`. Evaluation in `322_NAF_gold_fact_ssm_eval.py`.
> **See also:** `Analytical_Parameters.md` §9–10 for all tuned parameter values.

---

## 1. Problem Statement

The GLO (Global NAF_ELO) rating is a point estimate with no uncertainty quantification. It treats all coaches identically regardless of game count, gives permanent weight to noisy early games, and cannot distinguish between "confidently rated at 190" and "somewhere between 160 and 220." A better skill estimate would provide both a point estimate and a credible uncertainty band.

---

## 2. Options Considered

### Option 1: Fixed-Window Median Elo

Take the median Elo over the last N games (e.g. Last 50, already configurable via `analytical_config`).

**Pros:** Simple, robust to outliers, infrastructure already exists.
**Cons:** N games spans very different time periods depending on activity level — incomparable across coaches. No uncertainty measure. Hard cutoff discards older data abruptly.

**Rejected:** hard cutoff, no uncertainty.

### Option 2: Dynamic-Window Median Elo

Window size grows with total games played (e.g. `min(total, max(30, sqrt(total) * k))`).

**Pros:** Adapts to game count.
**Cons:** Ad hoc formula with no clear optimisation target. Still no uncertainty.

**Rejected:** same fundamental limitations as Option 1.

### Option 3: Exponentially Weighted Mean Elo

Weight each Elo observation by `λ^(games_ago)` where `λ ∈ (0,1)`. Weighted mean = `SUM(rating × λ^games_ago) / SUM(λ^games_ago)`.

**Pros:** No hard cutoff — old games decay smoothly. Single tunable parameter (λ).
**Cons:** No principled uncertainty. Weighted SD conflates real skill change with game-level noise.

### Option 3b: Exponentially Weighted + Analytical Uncertainty

Same exponential weighting as Option 3, but with uncertainty derived analytically from the effective sample size: `n_eff = SUM(w)² / SUM(w²)`. Uncertainty is modelled as a weighted coin-flip problem. Primary use case is a **lower confidence bound** for team selection: rank coaches by `estimated_skill - k × SD` to penalise high-uncertainty estimates.

**Pros:** Simple to compute. No model fitting. Principled uncertainty for ranking/selection.
**Cons:** Opponent strength enters only implicitly (via the Elo values being averaged). Cannot distinguish between 50 games against strong opponents (more informative) vs 50 games against weak opponents (less informative).

### Option 4: State-Space Model with Kalman Filter (Selected)

Model each coach's latent skill as a stochastic process observed through noisy game results. The Kalman state per coach is `(θ, P)` — point estimate and variance. After each game, both coaches' states update based on the observed result.

**Key design choices:**

- **Elo-scale compatibility:** Initial state `θ₀ = 150` (matching `elo_initial_rating`), logistic scaling factor = 150 (matching `elo_scale`). SSM output is directly comparable to Elo.
- **Flat observation noise:** The SSM does not use per-tournament `k_value`. Game informativeness is already captured by the opponent's skill and uncertainty. A win against a well-estimated strong opponent is automatically more informative than one against a poorly-estimated weak opponent, regardless of tournament size.
- **Non-Gaussian observations:** Game results are W/D/L, not Gaussian. An Extended Kalman Filter (EKF) linearises the logistic observation model around the current estimate.
- **Early rating noise handled automatically:** Wide prior variance (`P₀ = 50²`). Early games have high Kalman gain; as games accumulate, variance shrinks and the estimate stabilises.

**Pros:** Principled separation of process noise from observation noise. Uncertainty grows with no data, shrinks with observations. Handles early-career noise automatically. Opponent strength and uncertainty drive informativeness.
**Cons:** More complex. Sequential processing. Multiple hyperparameters to tune.

---

## 3. Decision and Implementation

Option 4 was selected and implemented in `321_NAF_gold_fact_ssm.py`. Two versions exist.

### 3.1 SSM v1 — Game-Indexed Random Walk

Output: `ssm_rating_history_fact` — 1 row per (game_id, coach_id).

Originally designed with AR(1) mean reversion (φ=0.995), but tuning showed mean reversion was harmful — it pulled SSM μ far below Elo over hundreds of games. Final v1 uses φ=1.0 (pure random walk). Constant process noise per game. Opponent uncertainty propagated into innovation variance.

**State model:**
```
mu_predict = phi × mu_prev + (1 - phi) × MU_GLOBAL
P_predict  = phi² × P_prev + sigma2_process
```

**Observation model:**
```
P(win) = 1 / (1 + 10^((θ_opp − θ_self) / elo_scale))
H = ln(10)/elo_scale × p × (1 − p)
```

Standard EKF update with Jacobian H feeding into Kalman gain.

### 3.2 SSM v2 — Time-Aware + Adaptive Volatility

Output: `ssm2_rating_history_fact` — 1 row per (game_id, coach_id).

No mean reversion. Process variance is time-aware with adaptive volatility.

**State per coach:** `mu_i` (estimated strength), `P_i` (uncertainty), `v_i` (volatility term), `t_i_prev` (timestamp of previous game).

**Prediction step:**
```
mu_pred = mu_prev                                   (no mean reversion)
P_pred  = P_prev + q_time × √(min(Δt, max_days)) + q_game + volatility
```

**Volatility dynamics (EWMA-based):**
```
shock_ewma_new = v_decay × shock_ewma_old + (1 - v_decay) × innovation²
volatility = clip(v_base + v_scale × shock_ewma_new, v_min, v_max)
```

Volatility rises after surprising results and decays gradually. The EWMA provides natural memory/decay, and `v_base + v_scale × shock_ewma` maps directly to "baseline noise plus recent surprise level."

**EKF update (same as v1):**
```
H = score_expected × (1 - score_expected) × ln(10) / scale
S = H² × P_self_pred + H² × P_opp_pred + σ²_obs
K = H × P_self_pred / S
mu_post = mu_pred + K × innovation
P_post  = max((1 - K × H) × P_pred, eps)
```

**Uncertainty-aware predictions (probit approximation):**
```
S_eff = S × √(1 + UA_COEFF × (σ²_self + σ²_opp))
UA_COEFF = π/8 × (ln10 / elo_scale)²
```

---

## 4. Implementation Details

**Notebook:** `321_NAF_gold_fact_ssm.py`. Runs after 320 (Elo engine), before 331 (coach summaries). Can run in parallel with 331+.

**Input:** `game_feed_for_ratings_fact` (same ordered feed as the Elo engine) + `analytical_config` (initial rating, Elo scale, all SSM hyperparameters).

**Processing:** Pure Python sequential loop over all games in `game_index` order. Both coaches in each game are predicted first, then observation updates run symmetrically using each other's predicted states. The global game ordering requires cross-coach state, so it processes all coaches in a single pass.

**Output columns:** mu/sigma before and after, opponent mu/sigma, innovation, Kalman gain, score_expected, result_numeric, plus diagnostic columns (v2 adds days_since_prev, time_scale, process_variance_added, volatility before/after, shock_ewma).

---

## 5. Key Lessons from Tuning

1. **Mean reversion is harmful** when tracking Elo (which has no mean reversion). φ=0.995 looked harmless but caused cumulative drag of ~0.5% × N games, pulling SSM μ far below Elo for active coaches.

2. **σ²_obs controls learning rate.** Too large and every game is uninformative (σ won't shrink). Too small and the model over-reacts to individual results. Grid search found σ²_obs=0.10 optimal for v2 (higher than v1's 0.02, because v2's adaptive volatility handles surprises separately).

3. **Prior σ should be empirically grounded** — set from SD of median-career Elo for coaches with 50+ games.

4. **Opponent uncertainty propagation** (H²×P_opp in S) is a deliberate design choice that makes games against poorly-estimated opponents less informative.

5. **Grid search tuning (v2):** Two-pass grid search (256 coarse + 81 fine) calibrated against 50-game rolling median Elo inside ±2σ. Weighted objective: veteran 60%, established 25%, developing 10%, burn-in 5%. Key insight: nearly all process variance should come from time gaps and volatility, not baseline per-game noise (q_game dropped from 0.25 to 0.025).

---

## 6. Evaluation

Evaluation notebook: `322_NAF_gold_fact_ssm_eval.py` (research, not production).

Head-to-head comparison of SSM v2, Elo, and a naive baseline (always predict 0.5) on a held-out test window (last 12 months of games). Metrics: log-loss, Brier score, accuracy, and calibration curves. Both systems were trained on all prior games when making each prediction — the realistic use case, not a re-trained split.

Additionally tests whether SSM uncertainty predicts prediction quality: games where both coaches have low σ (well-established) should have lower Brier score than games involving high-σ newcomers.

Calibration target: 50-game rolling median Elo should fall inside SSM ±2σ approximately 95% of the time for coaches with 100+ games.

---

## 7. Target Visualisation

Per-coach skill trajectory plot: X-axis = game number, Y-axis = rating (Elo-compatible scale). Solid line for SSM skill estimate, shaded band for ±2σ (95% confidence interval), optional dotted line for raw Elo trajectory. Wide uncertainty at the start narrowing as the filter learns, potentially widening again if results become volatile.

---

## 8. Resolved Design Questions

| Question | Resolution |
|---|---|
| Mean reversion | Removed (φ=1.0). Harmful when tracking Elo. |
| Game-indexed vs time-indexed | v1: game-indexed. v2: time-aware (time gap drives process variance). |
| Tournament-level observation noise | Not used. Opponent skill/uncertainty already captures informativeness. |
| Opponent uncertainty propagation | Yes — propagated into innovation variance via H²×P_opp in S. |
| Volatility dynamics | EWMA of squared innovations. Simpler and more interpretable than mean-reverting alternatives. |
| Time function | `f(Δt) = √(min(Δt, 180))` — sublinear growth, capped at 180 days. |
