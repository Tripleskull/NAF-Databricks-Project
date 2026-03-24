# SSM Rating Model v2 — Outline with Suggested Improvements

## Purpose
Estimate coach strength sequentially with:
- no mean reversion
- time-aware uncertainty growth
- small baseline game noise
- adaptive volatility
- expected-score observation model (`win=1`, `draw=0.5`, `loss=0`)

## State per coach
For each coach `i`, maintain:
- `mu_i`: current estimated strength
- `P_i`: uncertainty about `mu_i`
- `v_i`: volatility term
- `t_i_prev`: timestamp of previous game

## Interpretation
- `mu_i` = current rating estimate
- `P_i` = uncertainty in that estimate
- `v_i` = temporary instability / form volatility

## Prediction step
Let `Δt_i` be elapsed time since the coach's previous game.

Use:

```text
mu_i,pred = mu_i,prev
P_i,pred  = P_i,prev + q_time * f(Δt_i) + q_game + v_i
```

where:
- `f(Δt)` is a chosen time-growth function, e.g. `sqrt(Δt)`, `log(1+Δt)`, or capped linear time
- `q_time` controls uncertainty growth from inactivity
- `q_game` is a small baseline per-game noise to avoid overconfidence during dense game clusters

## Observation model
Expected-score approach:

```text
result ∈ {1, 0.5, 0}
score_expected = 1 / (1 + 10^((mu_opp - mu_self)/scale))
innovation = result - score_expected
```

This keeps draws treated as half a win, which is considered a reasonable approximation here.

## EKF-style update
Use the same EKF-style logistic linearisation as before:

```text
H = score_expected * (1 - score_expected) * ln(10) / scale
S = H² * P_self,pred + H² * P_opp,pred + sigma2_obs
K = H * P_self,pred / S

mu_self,post = mu_self,pred + K * innovation
P_self,post  = max((1 - K * H) * P_self,pred, eps)
```

## Volatility dynamics
Volatility should rise after surprising results and decay gradually.

### Implemented design (EWMA-based)

The implementation uses an EWMA of squared innovations rather than the originally suggested mean-reverting formula:

```text
shock_ewma_new = v_decay * shock_ewma_old + (1 - v_decay) * innovation²
v_i,new = clip(v_base + v_scale * shock_ewma_new, v_min, v_max)
```

where:
- `v_decay` = EWMA decay (0.90 = ~10-game effective window)
- `v_base` = baseline volatility level
- `v_scale` = sensitivity multiplier on shock memory
- `clip(...)` = keeps volatility within [v_min, v_max]

This is simpler and more interpretable than the original `alpha * (v_old - v_base)` formulation. The EWMA provides natural memory/decay, and `v_base + v_scale * shock_ewma` maps directly to "baseline noise plus recent surprise level."

### Original suggestion (for reference)

```text
v_i,new = clip(v_base + alpha * (v_i,old - v_base) + beta * g(innovation_i²), v_min, v_max)
```

## Suggested refinements
1. **Nonlinear time effect**
   - Prefer `f(Δt)` over raw linear time.

2. **Baseline game noise**
   - Keep a small `q_game` even when time is the main driver.

3. **Stable volatility design**
   - Use baseline + decay + floor/cap.

4. **Smoothed volatility update**
   - Consider EWMA-style surprise memory rather than only last game.

5. **Newcomer handling**
   - Use possibly larger `P0` and/or `v0` for new coaches.

6. **Outlier robustness**
   - Temper very large innovations so one extreme result does not dominate.

7. **Evaluation framework**
   - Tune on predictive performance and calibration, not visual appeal alone.

## Diagnostics to plot
- `mu_before`, `mu_after`
- `sigma_before`, `sigma_after`
- `v_i` over time
- innovation over time
- days since previous game
- calibration by expected-score bins
- behaviour after inactivity gaps

## Tuning order (original plan)
1. Choose `f(Δt)`
2. Tune `q_time`
3. Tune small `q_game`
4. Tune `sigma2_obs`
5. Tune volatility parameters: `v_base`, `alpha`, `beta`, bounds

## Tuning approach (actual, 2026-03-24)
Simultaneous 4D grid search over `sigma2_obs`, `q_time`, `q_game`, `v_scale`.
Fixed structural params: `prior_sigma=50`, `max_days=180`, `v_decay=0.90`, `v_base=0.25`, `v_min=0.0`, `v_max=16.0`.
Calibration target: 50-game rolling median Elo inside ±2σ ~95%, weighted toward veteran coaches (60% vet, 25% est, 10% dev, 5% burn-in).
Two-pass: coarse grid (4^4=256) then fine grid (3^4=81) around best coarse result.
Result: `sigma2_obs=0.10`, `q_time=2.0`, `q_game=0.025`, `v_scale=24.0`.

## Summary
This v2 model is a:
- no-reversion random-walk rating model
- with time-aware uncertainty growth
- with baseline per-game noise
- with explicit adaptive volatility
- and with expected-score handling of draws
