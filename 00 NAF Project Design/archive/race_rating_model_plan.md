# Race Rating Model Plan

## Purpose
Build a race-aware rating system for Blood Bowl that:
- keeps ratings on the same Elo-style scale as the current global model
- starts every coach at **150** for the global rating and every race rating
- updates all race ratings when a coach plays, through an inferred race-covariance structure
- can produce full **coach × race** tables and plots, including estimates for races a coach has never played
- is evaluated primarily by **Brier score** in a chronological forecasting setup

---

## Core design choice
The model should not be framed as 30 separate, unrelated race ratings.

Instead, define a coach's strength with race `r` as:

\[
\theta_{i,r,t} = g_{i,t} + d_{i,r,t}
\]

Where:
- `g_{i,t}` = global coach skill at time `t`
- `d_{i,r,t}` = coach-specific deviation for race `r`
- `\theta_{i,r,t}` = full race rating used in prediction

This preserves the Elo-like interpretation while allowing race-specific skill.

### Prior structure
Use:
- global prior mean: **150**
- race-deviation prior mean: **0**

So initially:

\[
\theta_{i,r,0} = 150
\]

for all coaches and all races.

---

## Match model
For a match between coach `i` using race `r` and coach `j` using race `s`:

\[
p_{ij} = \frac{1}{1 + 10^{-[(g_{i,t} + d_{i,r,t}) - (g_{j,t} + d_{j,s,t})]/S}}
\]

Where `S` is the same Elo scale parameter used in the global model.

Observed result remains on the current expected-score scale:
- loss = `0.0`
- draw = `0.5`
- win = `1.0`

Primary optimisation target: **Brier score**.
Secondary diagnostic: log-loss.

---

## Why all race ratings should update after one game
This is a core requirement and it does make sense.

If a coach plays one game with race `r`, that game directly informs `d_{i,r,t}`.
If race deviations are correlated across races, the same observation should also update beliefs about:
- other similar races strongly
- weakly related races slightly
- unrelated races very little

So one observed race should update the full race vector through the covariance structure.

This is the mechanism that allows prediction for:
- sparse race histories
- first games with a race
- completely unplayed races

---

## Recommended covariance strategy

### Main recommendation
Do **not** use a raw empirical covariance matrix directly as the production covariance prior.
Use the empirical covariance as:
- an exploratory diagnostic
- an initial reference
- a candidate shrinkage target

Then estimate a **regularised covariance structure** for the actual model.

### Why not raw empirical covariance only
Even with all coaches pooled together, coach-race data will still be uneven:
- some races are common, some rare
- many coaches will have sparse race exposure
- empirical covariances may be noisy or unstable

Small empirical covariances should not simply be dropped to zero without testing. They may still carry weak but real information. Better options:
- shrink them toward zero
- regularise the covariance matrix
- compare against a lower-rank factor version

### Staged covariance approach
1. Compute an exploratory empirical race-correlation matrix from coach-level race residual patterns.
2. Use shrinkage / regularisation to stabilise it.
3. Compare against a latent-factor alternative if needed.

### Candidate model forms

#### Option A: full shrunk covariance matrix

\[
d_{i,t} \sim \mathcal{N}(0, \Sigma_{race})
\]

Where `d_{i,t}` is the vector of race deviations for coach `i`.

Pros:
- interpretable race-to-race relationships
- direct covariance-based updating

Cons:
- can be noisy
- parameter-heavy

#### Option B: low-rank factor structure

\[
d_{i,r,t} = \lambda_r^\top f_{i,t} + \epsilon_{i,r,t}
\]

Pros:
- more stable
- often better with sparse data
- still induces race correlation

Cons:
- less directly interpretable than a raw covariance matrix

### Recommendation for v1
Plan around a **dynamic correlated race model**, but estimate the race relationship with a **shrunk covariance structure first**.
Treat the empirical matrix as exploratory input, not final truth.

---

## Time dynamics
The preferred direction is dynamic, not static.

### Dynamic global state
Keep the best current global model structure as the starting point for `g_{i,t}`.

### Dynamic race state
Allow race deviations to evolve over time:

\[
d_{i,r,t+1} = d_{i,r,t} + \eta_{i,r,t}
\]

This allows:
- changing form with a race
- learning over time
- decay of certainty when a race is not played for a long time

### Practical caution
Dynamic race states greatly increase complexity.
So implementation should still be staged:
1. dynamic global + static/slow race layer prototype
2. fully dynamic race deviations if they improve prediction

Even if the target model is dynamic, evaluation should verify that the extra state complexity actually pays off.

---

## Race baseline effects
Race baseline effects should be treated as **data-driven candidates**, but not forced in v1 unless they help predictive performance.

Possible extension:

\[
\theta_{i,r,t} = g_{i,t} + d_{i,r,t} + \alpha_r
\]

Where `\alpha_r` is a race baseline effect.

This can help separate:
- coach skill with a race
- average strength of the race in the environment

But race baselines can also absorb structure that may later belong to matchup effects, so they should be introduced carefully and tested.

---

## Matchup effects
Race-vs-race matchup effects are desirable, but should be **excluded from v1** and treated as a future extension.

Reason:
- they add major complexity
- they are easy to confound with race effects and coach-race effects
- they can wait until the core race-rating model is validated

Roadmap for later:

\[
p_{ij} = \sigma\big((g_i + d_{i,r}) - (g_j + d_{j,s}) + m_{r,s}\big)
\]

Where `m_{r,s}` is a race matchup term.

---

## Identifiability constraints
The model must separate:
- global coach strength
- race-specific deviations
- possible race baselines

To avoid ambiguity, keep race deviations centred around zero:

\[
\mathbb{E}[d_{i,r,t}] = 0
\]

Interpretation:
- `g_{i,t}` = overall coach strength
- `d_{i,r,t}` = how that coach differs from their own global level when using race `r`

Without this, the model may confuse:
- globally strong coach
with
- coach who is positive in almost every race deviation

---

## Staged build plan

### Stage 0: benchmarks
Establish and preserve the following benchmarks:
- `0.5` baseline
- best global-only model
- race Elo benchmark

These are the minimum comparison points.

### Stage 1: first race-aware model
Build a race-aware model on the Elo scale using:
- global rating from the best current model
- race deviations
- no matchup effects yet
- correlated race structure

Output:
- full coach × race posterior table
- uncertainty per coach × race
- plots such as histograms by race

### Stage 2: covariance estimation and diagnostics
Create:
- empirical race-correlation matrix
- shrunk covariance estimate
- diagnostic plots of race-to-race similarity

Questions to answer:
- which race links are stable
- which are noisy
- whether the covariance structure is predictive

### Stage 3: dynamic race-state extension
Allow race deviations to evolve over time and test whether this improves forecasting.

### Stage 4: optional extensions
Only after the core model is validated:
- race baseline effects
- matchup effects
- latent-factor alternative to covariance model

---

## Test setup

### Evaluation principle
Use **chronological forecasting**.

That means each prediction must only use information available before the match date.
This matches the current global evaluation logic and should remain the main setup.

### Why chronological evaluation
This is the natural choice because the rating system is meant to forecast future games.
It avoids leakage and keeps the evaluation realistic.

### Validation and test windows
Use the same style as the current global setup:
- historical data for estimation / warm-up
- chronological validation window for tuning choices
- later chronological test window for locked comparison

Cross-validation is not the primary design here. The main objective is realistic forward prediction, not exchangeable resampling.

---

## Primary success criterion
The primary target is:
- **better Brier score than race Elo**, in chronological out-of-sample prediction

Secondary comparison targets:
- best global-only model
- `0.5` baseline

---

## Required model comparisons
At minimum compare:
1. `0.5` baseline
2. best global-only model
3. race Elo benchmark
4. global + independent race effects
5. global + correlated race effects

This allows you to answer two separate questions:
- does adding race information help at all?
- does modelling race correlation help beyond independent race ratings?

---

## Required evaluation slices
Overall accuracy alone is not enough, because one of the goals is predicting sparse and unseen races.

So in addition to overall Brier score, include these slices:

### A. Overall chronological test performance
Main score table for all test games.

### B. Sparse race-history slice
Evaluate games where the coach has only a small number of prior games with the race.

### C. First-games-with-race slice
Evaluate the first observed games with a race for each coach.

Reason:
This is the most direct test of whether the covariance structure helps infer useful race ratings before much direct evidence exists.

Even if overall Brier is the main metric, these slices are necessary to test the core modelling goal.

---

## Suggested outputs

### Tables
- full `coach × race` posterior table
- latest global rating table
- latest race rating table
- race covariance / correlation summary table
- evaluation summary tables by model and slice

### Plots
- histogram of race ratings for each race
- heatmap of race correlation matrix
- calibration plots by model
- Brier difference vs benchmark by race-history bin
- uncertainty distribution for seen vs unseen races

---

## Suggested data products

### Fact-like output
`race_rating_history_fact`
- one row per `(game_id, coach_id, race_id)` or equivalent prediction grain
- pre-match and post-match quantities
- uncertainty fields
- opponent information

### Latest-state view
`race_rating_latest_v`
- one row per `(coach_id, race_id)`
- latest posterior mean and uncertainty
- count of games with race
- flag for directly observed vs inferred-dominant estimate

### Diagnostic views
- race correlation matrix view
- sparse-race evaluation view
- first-race-games evaluation view

---

## Key risks and failure modes

### 1. Noisy covariance estimation
If the covariance structure is too noisy, unseen-race predictions may become unstable.
Mitigation:
- shrinkage
- regularisation
- compare against simpler baselines

### 2. Too much complexity too early
Dynamic global + dynamic correlated race states + matchup effects is too much for the first serious implementation.
Mitigation:
- stage the build
- require each added layer to improve out-of-sample Brier

### 3. Confounding global and race skill
Mitigation:
- centred race deviations
- clear parameter roles
- explicit benchmarks

### 4. Over-interpreting weak covariances
Mitigation:
- treat small correlations cautiously
- assess predictive value, not just visual structure

---

## Final recommendation
Proceed with a **chronological, Brier-focused, race-aware dynamic rating system** built around:

\[
\theta_{i,r,t} = g_{i,t} + d_{i,r,t}
\]

where:
- all ratings remain on the Elo scale
- all initial full race ratings are 150
- race deviations are correlated across races
- covariance is inferred from data using a **regularised** version of the empirical race structure
- performance is judged primarily by out-of-sample Brier score against race Elo, global-only, and `0.5` baseline

### Practical interpretation
A coach playing one race should update all of their race ratings through the inferred correlation structure.
That is not a bug. It is the intended mechanism that allows prediction for new and sparsely played races.

### Decision rule for model growth
Every extra layer must justify itself by prediction gain:
- independent race effects must beat global-only
- correlated race effects must beat independent race effects
- dynamic race states must beat simpler race states
- matchup effects must beat the no-matchup version

That keeps the project grounded in forecasting value rather than model elegance alone.
