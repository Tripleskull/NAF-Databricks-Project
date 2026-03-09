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

### Option 4: State-Space Model with Kalman Filter (Recommended)

Model each coach's latent skill as an AR(1) mean-reverting process observed through noisy game results.

**State equation (skill evolution between games):**
```
θ_t = μ + φ(θ_{t-1} - μ) + η_t     where η_t ~ N(0, σ²_process)
```

**Observation equation (game outcome):**
```
P(win) = logistic(θ_i - θ_j)
```

The Kalman state per coach is `(θ, P)` — point estimate and variance. After each game, both coaches' states update based on the observed result.

**Key design choices:**

- **Very slow mean-reversion (φ close to 1, e.g., 0.995):** Nearly invisible over 50–100 games, but prevents skill estimates from drifting to infinity and provides long-run stability. Only matters for coaches with extreme ratings or long gaps.
- **Game-indexed, not time-indexed:** The process evolves per game, not per calendar day. Inactivity as a concept doesn't apply — a coach with 200 games who hasn't played in 2 years looks the same as one who played yesterday.
- **Non-Gaussian observations:** Game results are W/D/L, not Gaussian. Requires an Extended Kalman Filter (EKF) to linearise the logistic observation model around the current estimate. This is a few extra lines of code, not a fundamental obstacle.
- **Early rating noise handled automatically:** The filter starts with wide prior variance. Early games have high Kalman gain (the filter knows it's still learning). As games accumulate, variance shrinks and the estimate stabilises. Noisy early ratings get low effective weight naturally — no special handling needed.

**Pros:** Principled separation of process noise (real skill change) from observation noise (game-level luck). Uncertainty grows when there's no data and shrinks with observations. Handles early-career noise automatically. Mean-reversion provides long-run stability. Produces a predictive distribution per game (useful for tournament predictions and team selection confidence).
**Cons:** More complex to implement than Options 1–3. Sequential processing per coach (not parallelisable across games within a coach, but easily parallelisable across coaches). Three tunable parameters (φ, σ²_process, σ²_observation).

## Recommendation

**Option 4 — State-Space Model with Extended Kalman Filter.**

The simpler options (1–3) were considered and rejected during discussion because they all fail to separate signal from noise in the Elo trajectory. The weighted SD from Option 3 looked promising but conflates stable inputs with genuine certainty — a fundamental limitation that can't be patched without moving to a probabilistic model.

The SSM addresses every concern raised:

- Uncertainty that reflects actual information content (not just Elo volatility)
- No sensitivity to noisy early games
- Very slow mean-reversion for long-run stability without pulling active coaches
- Game-indexed (no time/inactivity modelling needed)
- Clean tuneable parameters that fit naturally into `analytical_config`

## Implementation Sketch

### Phase 1: Prototype (standalone notebook)

1. **New notebook:** `360_NAF_gold_ssm_prototype.py` (or a separate Python notebook outside the pipeline)
2. **Input:** `rating_history_fact` — one row per (coach, game) with `rating_before`, `opponent_rating_before`, `result_numeric`, game sequence
3. **Implementation:** Python EKF loop per coach via `groupBy(coach_id).applyInPandas()` or pure pandas if data fits in memory
4. **Output:** Per coach per game: `skill_estimate`, `skill_variance` (or `skill_sd`), `kalman_gain`
5. **Validation:** Backtest against held-out games — compare log-likelihood and prediction accuracy vs current Elo
6. **Parameter estimation:** Fit φ, σ²_process, σ²_observation by maximising log-likelihood across all coaches, or set from domain knowledge and iterate

### Phase 2: Pipeline Integration (if prototype validates)

1. Wire into gold layer as a new fact or summary table
2. Add φ, σ²_process, σ²_observation to `analytical_config`
3. Expose `skill_estimate ± skill_sd` in presentation views
4. Update dashboards to show confidence bands where relevant

### Dependencies

- `rating_history_fact` (game-level Elo history, already exists)
- Python: `numpy`, `scipy` (for logistic function). Optional: `filterpy` for off-the-shelf EKF
- Databricks: `applyInPandas` for parallel processing across coaches

### Open Questions

- **330 core summary notebook:** The world-level reference objects (`world_race_elo_quantiles`, `world_glo_metric_quantiles`) and potentially SSR-related world rankings need a shared home that runs before 331/332. Parked for next week.
- **Race-level SSM:** Should the SSM also apply to race-specific Elo, or only GLO? Race Elo has sparser data per coach — wider uncertainties, potentially less stable.
- **Dashboard UX:** How to display uncertainty bands — error bars, shaded regions, confidence intervals in tables?
