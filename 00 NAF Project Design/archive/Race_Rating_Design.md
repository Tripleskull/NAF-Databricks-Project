# Race-Aware Rating Model — Design Document

> **Status:** Stage 1 implemented in `323_NAF_gold_fact_race_rating.py`. Evaluated in `324_NAF_gold_fact_race_rating_eval.py`.
> **See also:** `Analytical_Parameters.md` §11 for all tuned parameter values.

---

## 1. Purpose

Build a race-aware rating system for Blood Bowl that:

- keeps ratings on the same Elo-style scale as the global model
- starts every coach at **150** for the global rating and every race rating
- decomposes coach strength into global skill and race-specific deviations
- can produce full **coach × race** tables, including estimates for races with sparse data
- is evaluated primarily by **Brier score** in a chronological forecasting setup

---

## 2. Core Design

The model should not be framed as 30 separate, unrelated race ratings. Instead, define a coach's strength with race `r` as:

```
θ_{i,r,t} = g_{i,t} + d_{i,r,t}
```

Where:
- `g_{i,t}` = global coach skill at time `t`
- `d_{i,r,t}` = coach-specific deviation for race `r`
- `θ_{i,r,t}` = full race rating used in prediction

### Prior structure

- Global prior mean: **150**
- Race-deviation prior mean: **0**

So initially `θ_{i,r,0} = 150` for all coaches and all races.

### Identifiability

Race deviations are centred at zero by prior. `g` captures overall coach strength; `d` captures how the coach differs from their global level with each race. Without this constraint the model may confuse a globally strong coach with a coach who has positive deviations in every race.

---

## 3. Match Model

For a match between coach `i` using race `r` and coach `j` using race `s`:

```
p_{ij} = 1 / (1 + 10^(-[(g_i + d_{i,r}) - (g_j + d_{j,s})] / S))
```

Where `S` is the same Elo scale parameter used in the global model. Result encoding: loss=0.0, draw=0.5, win=1.0.

Primary optimisation target: **Brier score**. Secondary diagnostic: log-loss.

---

## 4. Update Mechanism

Each game updates `g` and the played race's `d` jointly via a 2D EKF step. Information is split between g and d proportionally to their variances — when d is uncertain (few games with that race), most of the update goes to d.

**Prediction step (no mean reversion):**
```
g_pred  = g_prev
d_pred  = d_prev
P_g_pred = P_g_prev + q_global
P_d_pred = P_d_prev + q_race
```

**Update step (joint EKF):**
```
h = ln(10)/S × p × (1-p)
S_innov = h² × (P_g + P_d) + σ²_obs
K_g = h × P_g / S_innov
K_d = h × P_d / S_innov
```

---

## 5. Cross-Race Updates

A core design goal is that one observed game should update beliefs about all of a coach's race ratings. If race deviations are correlated, an observation with race `r` also informs similar races strongly, weakly related races slightly, and unrelated races very little.

This is the mechanism that allows prediction for sparse race histories, first games with a race, and completely unplayed races.

### Covariance strategy

Do **not** use a raw empirical covariance matrix directly. Use it as an exploratory diagnostic and shrinkage target, then estimate a regularised structure.

**Candidate model forms:**

- **Option A: Full shrunk covariance matrix.** Interpretable race-to-race relationships. Direct covariance-based updating. Can be noisy and parameter-heavy.
- **Option B: Low-rank factor structure** (`d_{i,r,t} = λ_r^T f_{i,t} + ε_{i,r,t}`). More stable with sparse data, still induces race correlation, but less directly interpretable.

---

## 6. Staged Build Plan

### Stage 0: Benchmarks ✅
Establish: 0.5 baseline, best global-only model, race Elo benchmark.

### Stage 1: Independent Race Deviations ✅ (implemented in 323/324)
Global skill `g` + independent per-race deviations `d`. No cross-race covariance (each race deviation updated independently). Joint 2D EKF update for `(g, d_played)` per game.

Output: `race_rating_history_fact` — 1 row per (game_id, coach_id). Evaluated in 324 with chronological Brier score against 0.5 baseline, global Elo, race Elo, and sliced by race-experience depth (first game, sparse, moderate, established).

### Stage 2: Covariance Estimation and Diagnostics (future)
Empirical race-correlation matrix, shrunk covariance estimate, diagnostic plots of race-to-race similarity. Questions: which race links are stable, which are noisy, whether the covariance structure is predictive.

### Stage 3: Dynamic Race-State Extension (future)
Allow race deviations to evolve over time with time-aware process noise (mirroring SSM v2). Test whether this improves forecasting.

### Stage 4: Optional Extensions (future)
Only after the core model is validated: race baseline effects (`α_r`), matchup effects (`m_{r,s}`), latent-factor alternative to full covariance.

---

## 7. Future Directions Under Consideration

1. **Correlated prior / independent update hybrid:** Use the race-correlation matrix only to initialise unseen or sparse races, while keeping Stage 1-style independent updates afterwards.

2. **Low-rank coach × race factor model:** A lower-dimensional latent model that may preserve the benefits of unplayed-race estimates while improving speed and stability.

3. **Hierarchical shrinkage model with race main effects:** A more structured partial-pooling alternative to the current correlated model.

4. **Selection diagnostics for g:** Investigate whether the global rating g is partly capturing race-choice behaviour and therefore confounding interpretation.

---

## 8. Evaluation Design

### Principle
Chronological forecasting. Each prediction uses only information available before the match date. No leakage. Same style as the global SSM evaluation (322).

### Required model comparisons
1. 0.5 baseline
2. Best global-only model
3. Race Elo benchmark
4. Global + independent race effects (Stage 1)
5. Global + correlated race effects (Stage 2)

### Required evaluation slices
- **Overall:** Main Brier score table for all test games.
- **Sparse race-history:** Games where the coach has few prior games with the race.
- **First-games-with-race:** The most direct test of whether structure helps infer useful race ratings before much direct evidence exists.

### Primary success criterion
Better Brier score than race Elo in chronological out-of-sample prediction.

---

## 9. Key Risks

1. **Noisy covariance estimation.** Mitigation: shrinkage, regularisation, compare against simpler baselines.
2. **Too much complexity too early.** Mitigation: staged build, require each added layer to improve out-of-sample Brier.
3. **Confounding global and race skill.** Mitigation: centred race deviations, explicit benchmarks.
4. **Over-interpreting weak covariances.** Mitigation: assess predictive value, not just visual structure.
5. **g capturing race-choice behaviour.** Mitigation: selection diagnostics (future direction §7.4).

---

## 10. Data Products

### Fact output
`race_rating_history_fact` — one row per (game_id, coach_id). Contains global skill (g) and race deviation (d) before/after, uncertainty for each, opponent information, innovation, Kalman gains.

### Decision rule for model growth
Every extra layer must justify itself by prediction gain: independent race effects must beat global-only, correlated must beat independent, dynamic must beat simpler, matchup effects must beat no-matchup. Forecasting value over model elegance.
