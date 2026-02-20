# NAF Analytics — Expansion Plan (Final)

> Scope: All new features for the Nation Dashboard, Coach Dashboard, and a new Global Status Report Dashboard.
> Approach: Phased delivery. Each phase is independently deployable and testable.
> Status: **Plan finalised** — all design decisions resolved.

---

## Resolved Design Decisions

| Decision | Resolution | Rationale |
|----------|-----------|-----------|
| Rating metric default | **PEAK** everywhere. MEAN/MEDIAN available in coach-level data but not surfaced in nation dashboards. | Most intuitive ("how good at their best"). Simplifies nation-level aggregation. |
| Opponent rating context | Always **PEAK** for opponent strength. | Consistent with above. |
| Nation-level form | **Dropped**. No nation form metric. | "Last 50 games" is meaningless at nation grain — whose games? Time-series WDL (Phase 2) answers the "is the nation improving" question better. |
| Form window | **Last 50 games**. | 50 games is enough signal (~2–4 tournaments). |
| Form metric | **`form_score = SUM(actual - expected)`** over last 50 games. Show both raw number and categorical label (Strong/Good/Neutral/Poor/Weak Form based on percentile). | Adjusts for opponent strength. Raw number available but label is primary display to reduce over-interpretation risk. |
| Form rating system | **Deferred**. A true asymmetric form-Elo is parked. Form is measured by outperformance of Elo expectations, not by a separate rating. | Asymmetry breaks zero-sum. The form_score captures the same insight without the consistency problems. |
| Comparison baseline | **REST_OF_WORLD** for head-to-head comparisons. **WORLD** for distribution baselines (boxplots, quantiles). | Boxplots answer "where are we in the global picture". WDL answers "us vs everyone else". |
| Race gating | **Min games only (25+)**. No recency requirement. | Consistent with existing `is_valid_min_games_race_elo`. Simpler. |
| Opponent binning | **Both**: opponent-rating bins AND game-quality bins (average of both players' ratings). Same bin scale for both. | Game-quality bins answer "how do we do in elite matchups" — a different and complementary question. |
| Specialist thresholds | **World top 2%, Nation top 3** for a given race. Must be above world median AND meet 25-game minimum. | Tight world badge, meaningful national badge. |
| Domestic definition | **Both dimensions**: (1) tournament location (host nation = coach nation) and (2) opponent origin (opponent nation = coach nation). | Travel burden and international competition exposure are independent questions. |
| WDL time series | **Per-game cumulative** (same pattern as `coach_results_cumulative_series_v`). Exclude intra-nation games. | Most flexible. Dashboard can aggregate to any display grain. |
| Selector weights | **Fixed in config**: 0.30 rating, 0.25 form, 0.15 opponent strength, 0.15 versatility, 0.15 international. Document as tuneable. | Starting point. Tuned later based on what the data shows. |
| Specialist/generalist labels | **Contextual**: world specialist (top 2% globally) and national specialist (top 3 in nation) are separate badges. Generalist/polymath definition deferred — build raw data first, define composite later. | Need to see the data before committing to a formula. |
| Global status report | Include race-specific leaderboards. Add major tournament winners and tournament win counts. | Richer dashboard. Tournament wins also feed into coach profile eventually. |
| Nation power ranking | Rank nations by **top-8 selector score average** (i.e. the strength of a nation's best potential team). | Natural outcome of the team selector work. |

---

## Phase 0 — Cleanup

**Goal**: Resolve technical debt before building new features.

**Work items**:
1. Fix `nation_rivalry_summary` TABLE/VIEW duplicate in 332 — keep TABLE only.
2. Align dashboard parameter names: `:nation_name_display` everywhere. Remove `:nation_name`, `:nation_clean`.
3. Align `metric_type` values: uppercase (`PEAK`, `MEAN`, `MEDIAN`), never `glo_peak`.

**Files**: 332, 342, Nation Dashboard JSON.
**Delivers**: Clean baseline.

---

## Phase 1 — Opponent Strength Context + Form Score (R2, R9)

**Goal**: Add average opponent rating alongside all rating displays. Compute a form score that adjusts for opponent strength. These are foundational — later phases depend on both.

### 1a. Form Score Design

**Definition**: `form_score = SUM(actual_result - expected_result)` over the last 50 games (GLOBAL scope). This measures how much a coach outperformed (or underperformed) Elo expectations recently. A coach who beats opponents they're expected to beat gets form ≈ 0. A coach punching above their weight gets form > 0.

**Source data**: `rating_history_fact` already contains `result_numeric` (actual) and `score_expected` (expected) per game. Window to last 50 games by `game_index DESC`.

**Outputs** (both raw and bucketed):
- `form_score` — raw numeric (roughly -10 to +10 range; inspect actual distribution)
- `form_pctl` — percentile rank among all coaches with 50+ games (0–100)
- `form_label` — categorical bucket based on percentile: Strong Form (top 10%), Good Form (10–30%), Neutral (30–70%), Poor Form (70–90%), Weak Form (bottom 10%)

**Exploration step**: Before finalising the bucket thresholds, compute the distribution of form_score across all eligible coaches. Check: is it roughly normal? Are there outliers? Does the percentile bucketing produce sensible groupings? Adjust thresholds if the distribution is skewed.

**Visualisation**: Cumulative results chart, last 50 games. X = game number (1–50, most recent on right), Y = cumulative points. Same pattern as the existing career cumulative chart but windowed. The form_label appears as a badge; the raw form_score is visible on hover or in detail views.

**Risk — over-interpretation**: Form_score is a small-sample statistic. Mitigated by: (a) showing it as a label, not a precise number, in most contexts; (b) always paired with the cumulative chart so the user sees the shape, not just the summary; (c) documenting that 50 games is ~2–4 tournaments and results are noisy. Raw numbers are available for those who want them but the label is the primary display.

### 1b. Opponent Strength Context

**Extend `coach_performance_summary`** (331):
- Add `avg_opponent_glo_peak` — mean of opponent's GLO peak across all games.
- Add `avg_opponent_glo_peak_last_50` — same, restricted to last 50 games.
- Add `form_score`, `form_pctl`, `form_label`.
- Source: join `coach_games_fact` to `coach_rating_global_elo_summary` on opponent_coach_id; `rating_history_fact` for form computation.

### 1c. Nation Summary (332)

**Extend `nation_coach_glo_metrics`**:
- Add `avg_opponent_glo_peak` per coach.

**Extend `nation_overview_summary`**:
- Add `avg_opponent_glo_peak` — nation-level mean of coach-level opponent strength.

### 1d. Presentation (341, 342)

- Extend `coach_profile` with `avg_opponent_glo_peak`, `form_score`, `form_label`.
- Extend `nation_overview` with `avg_opponent_glo_peak`.

**Files**: 331, 332, 341, 342.
**Delivers**: R2 (opponent strength context), R9 (form score). Every rating display can now show "faced opponents rated X on average" and every coach has a form assessment.

---

## Phase 2 — Comparison Framework + WDL Time Series (R1, R8)

**Goal**: Build the comparison backbone and cumulative WDL series for nations.

### 2a. Comparison Framework

**Design rule**: All nation comparison tables use an `opponent_group` column:
- `'REST_OF_WORLD'` — opponents from different nations (pre-materialised)
- `'SELECTED'` — resolved at dashboard query time by filtering on `:compare_nations`

For distribution baselines (boxplots, quantiles), use WORLD (all coaches globally).

### 2b. Nation Cumulative WDL Series

**New table: `nation_results_cumulative_series`** (332)
- Grain: `(nation_id, game_sequence_number)`
- Source: `coach_games_fact` joined to `coach_dim` (both sides for nation IDs)
- **Exclude intra-nation games** (both coaches from same nation)
- Order: by `event_timestamp`, `game_id` within nation
- Columns:
  - `game_date`, `date_id`, `event_timestamp`
  - `result_numeric` (aggregated: nation's result in this game)
  - `cum_games`, `cum_wins`, `cum_draws`, `cum_losses`
  - `cum_win_frac`, `cum_points_frac`
  - `opponent_nation_id` (for filtering by opponent group)

**Note**: One nation game may involve multiple individual games (e.g. 5 Danish coaches each play a game at a tournament). Each individual game where a coach from nation N plays a coach from a different nation is one row. The cumulative series tracks the nation's aggregate running record.

### Presentation (342)

**New view: `nation_results_cumulative_display`**
- Joins nation_dim for display names.
- Dashboard can filter by opponent group (all ROW, or specific nations via `:compare_nations`).

**Files**: 332, 342, Nation Dashboard JSON.
**Delivers**: R1 (cumulative WDL), R8 (comparison pattern).

---

## Phase 3 — Race Strength Comparisons (R3)

**Goal**: Boxplots of nation race-level Elo PEAK vs world, for selected races.

### Summary (332)

**New table: `world_race_elo_quantiles`**
- Grain: `(race_id)`
- Source: `coach_race_summary` (all coaches globally), filtered by `is_valid_min_games_race_elo` (25+ games)
- Columns: `coaches_count`, `elo_peak_min`, `elo_peak_p10`, `elo_peak_p25`, `elo_peak_p50`, `elo_peak_p75`, `elo_peak_p90`, `elo_peak_max`
- PEAK only for v1.

**Existing `nation_race_elo_peak_summary`**: already has (nation_id, race_id) quantiles. Reuse as-is.

### Presentation (342)

**New view: `nation_race_strength_comparison`**
- UNION of nation quantiles + world quantiles per race
- Grain: `(nation_id, race_id, scope)` where scope ∈ {NATION, WORLD}
- Joins `race_dim` for race names
- Dashboard: `:race_selector` multi-select, defaults to all races

**Dashboard**: Grouped boxplot. X = race. Two boxes per race (nation vs world).

**Files**: 332, 342, Nation Dashboard JSON.
**Delivers**: R3.

---

## Phase 4 — Home/Abroad + Specialist Badges (R4, R5)

**Goal**: Two coach-level metrics that feed into the team selector.

### 4a. Home vs Abroad (two dimensions)

**New table: `coach_domestic_summary`** (332, nation-scoped)
- Grain: `(coach_id)`
- **Dimension 1 — tournament location**:
  - `games_home` (tournament host nation = coach nation)
  - `games_away` (tournament host nation ≠ coach nation)
  - `home_frac`, `win_frac_home`, `win_frac_away`
- **Dimension 2 — opponent origin**:
  - `games_vs_domestic` (opponent from same nation)
  - `games_vs_foreign` (opponent from different nation)
  - `domestic_opponent_frac`, `win_frac_vs_domestic`, `win_frac_vs_foreign`
- Coaches with unknown/fictitious nation: excluded entirely. They are "global citizens" and home/abroad is not defined for them.

### 4b. Specialist Badges

**New table: `coach_race_relative_strength`** (331)
- Grain: `(coach_id, race_id)`
- Source: `coach_race_summary` (elo_peak per race) + `world_race_elo_quantiles` (Phase 3)
- Columns:
  - `elo_peak`, `world_median_elo_peak`, `world_p98_elo_peak`
  - `relative_strength` — `(elo_peak - world_median_elo_peak) / world_median_elo_peak`
  - `is_eligible` — 25+ games with the race
  - `world_percentile` — percentile rank among all eligible coaches for this race
  - `is_world_specialist` — `world_percentile >= 98` (top 2%)

**New table: `coach_race_nation_rank`** (332, needs nation context)
- Grain: `(nation_id, coach_id, race_id)`
- `nation_race_rank` — DENSE_RANK within nation for this race by elo_peak DESC
- `is_national_specialist` — `nation_race_rank <= 3` AND `elo_peak > world_median_elo_peak` AND `is_eligible`

**Generalist / polymath**: Deferred to after we see the data. For now, surface:
- `races_played_eligible` — count of races where coach has 25+ games
- `races_above_world_median` — count of races where coach is above world median
- These two numbers, shown side by side, tell the story without forcing a composite score.

**Files**: 331, 332, Analytical_Parameters.md.
**Delivers**: R4, R5.

---

## Phase 5 — Opponent-Strength-Adjusted Win Rates (R6)

**Goal**: Nation-aggregate W/D/L by opponent rating bins AND by game-quality bins.

### 5a. Opponent Rating Bins

**New table: `nation_opponent_elo_bin_wdl`** (332)
- Grain: `(nation_id, bin_scheme_id, bin_index, opponent_group)`
- Source: `coach_games_fact` + opponent's GLO peak for binning + `global_elo_bin_scheme`
- Opponent group: REST_OF_WORLD (pre-materialised). Exclude intra-nation games.
- Columns: `games`, `wins`, `draws`, `losses`, `win_frac`, `avg_td_diff`

### 5b. Game-Quality Bins

**New table: `nation_game_quality_bin_wdl`** (332)
- Grain: `(nation_id, bin_scheme_id, bin_index, opponent_group)`
- Binning value: `(coach_glo_peak + opponent_glo_peak) / 2` — average rating of the two players
- Same bin scheme as opponent bins (same scale, same edges)
- Columns: same as opponent bins

### Presentation (342)

- Join bin labels from `gold_presentation.global_elo_bin_scheme` for both tables.
- Two dashboard charts: "performance by opponent strength" and "performance by game quality".

**Files**: 332, 342, Nation Dashboard JSON.
**Delivers**: R6.

---

## Phase 6 — National Team Selector + Nation Power Ranking (R7)

**Goal**: Multi-dimensional candidate scoring for team selection. Nation ranking by squad strength.

### Prerequisites: Phases 1, 4

**New table: `nation_team_candidate_scores`** (332)
- Grain: `(nation_id, coach_id)`
- Component scores (each 0–100, percentile within nation):
  - `rating_pctl` — GLO peak percentile
  - `form_pctl` — last-50 win_frac percentile
  - `opponent_strength_pctl` — avg_opponent_glo_peak percentile (higher = tougher opponents)
  - `versatility_pctl` — races_above_world_median percentile (more races above median = more versatile)
  - `international_pctl` — win_frac_vs_foreign percentile
- Composite: `selector_score = 0.30×rating + 0.25×form + 0.15×opponent_strength + 0.15×versatility + 0.15×international`
- `selector_rank` — DENSE_RANK within nation by selector_score DESC

**New config params** (310): `selector_w_rating`, `selector_w_form`, `selector_w_opponent`, `selector_w_versatility`, `selector_w_international`.

**New table: `nation_power_ranking`** (332)
- Grain: `(nation_id)`
- `top_8_avg_selector_score` — average selector_score of top 8 coaches
- `top_8_avg_glo_peak` — average GLO peak of top 8
- `power_rank` — DENSE_RANK by top_8_avg_selector_score DESC
- `coaches_eligible` — count of coaches with enough data for scoring

### Presentation (342)

- `nation_team_candidates_display` — join coach_dim, include race breakdown
- `nation_power_ranking_display` — join nation_dim

**Files**: 310, 332, 342, Nation Dashboard JSON, Analytical_Parameters.md.
**Delivers**: R7.

---

## Phase 7 — Global Status Report (R10)

**Goal**: New cross-national dashboard. Deferred until Phases 1–6 are functional.

**New notebook: `335_NAF_gold_summary_global.py`**

**New tables**:

1. **`global_coach_leaderboard`**
   - Grain: `(coach_id, leaderboard_type)` where type ∈ {ALL_TIME_PEAK, FORM_LAST_50}
   - Columns: `rating_value`, `global_rank`, `nation_id`, `games_played`, `avg_opponent_glo_peak`

2. **`global_biggest_movers`**
   - Grain: `(coach_id)`
   - `form_delta` (last_50 win_frac minus all-time win_frac), `rank_improvement`

3. **`global_nation_rankings`**
   - Grain: `(nation_id)`
   - Powered by `nation_power_ranking` from Phase 6

4. **`global_race_leaderboard`**
   - Grain: `(race_id, coach_id)`
   - Top coaches per race by elo_peak. Include world/national specialist badges.

5. **`global_major_tournament_winners`**
   - Grain: `(tournament_id)` or `(coach_id)` for win counts
   - Major tournament victories and podium finishes

**New notebook: `345_NAF_gold_presentation_global.py`**

**New dashboard: `Global Status Report.lvdash.json`**
- All-time leaderboard (top 50 by GLO peak)
- Form leaderboard (top 50 by last-50 performance)
- Biggest movers (top risers + fallers)
- Nation power rankings
- Race leaderboards
- Major tournament winners

**Files**: New 335, new 345, new dashboard JSON.
**Delivers**: R10.

---

## Phase 8 — Coach Dashboard Updates (R2, R9 coach-side)

**Goal**: Surface form, opponent strength, and specialist badges on the Coach Dashboard.

**Changes**:
- Couple rating displays with avg opponent rating
- Show specialist badges (world + national) on coach profile
- Show races_played_eligible and races_above_world_median
- Tournament win count on coach profile (requires Phase 7 data)

**Files**: 341, Coach Dashboard JSON.
**Delivers**: R2 + R9 (coach side).

---

## Phase 9 — Testing + Documentation

**Goal**: Full test coverage, updated documentation.

1. PK uniqueness tests for all new tables
2. FK coverage tests
3. Cross-check: nation aggregates = sum of coach-level data
4. Intra-nation exclusion validation (no self-play in nation WDL)
5. Update `Pipeline_Object_Index.md`
6. Update `Analytical_Parameters.md`
7. Update `PROJECT_STATUS.md`

---

## Execution Order (Updated 2026-02-20)

Phases 0–4 are implemented. Revised execution order below.

### ✅ Complete
- Phase 0 — Cleanup
- Phase 1 — Opponent strength + form score
- Phase 2 — Cumulative WDL series
- Phase 3 — Race strength comparisons
- Phase 4 — Home/abroad + specialist badges
- Phase 5 — Opponent-adjusted win rates (code done, not yet run)

### Next Steps (in order)

```
Step A  Fix critical issues from code review
        - 331: rating_system 'ELO' → 'NAF_ELO' (2 tables)
        - 341: remove duplicate global_elo_bin_scheme view
        - 341: rename flag_emoji mislabeled as *_flag_code (5 places)
        - 342: switch to flag_emoji via nation_identity_v
        - 350: add rating_system allowed-values test
        │
Step B  Run + validate Phase 4 in Databricks
        - Commit, push, pull in Databricks
        - Run: 331 → 332 → 342 → 350
        - Spot-check specialist badge counts
        │
Step C  ✅ Phase 5 — Opponent-adjusted win rates (R6)
        - nation_opponent_elo_bin_wdl (332) ✅
        - nation_game_quality_bin_wdl (332) ✅
        - Presentation views (342) ✅
        - Tests added to 350 ✅
        │
Step D  Phase 6 — Team selector + nation power ranking (R7)
        - nation_team_candidate_scores (332)
        - nation_power_ranking (332)
        - Config params in 310
        - Presentation views (342)
        │
Step E  Dashboard overhaul
        - Cell-by-cell review of Coach Dashboard
        - Cell-by-cell review of Nation Dashboard
        - Multi-page restructuring for both
        - Wire up all new Phase 1–6 data
        │
Step F  Phase 7 — Global status report (R10) [nice-to-have]
        - New notebooks 335 + 345
        - New dashboard
        │
Step G  Phase 8 — Coach dashboard updates (R2, R9) [nice-to-have]
        - Specialist badges on coach profile
        - Tournament wins (requires Phase 7)
        │
Step H  Phase 9 — Final testing + documentation [nice-to-have]
```

---

## New Objects Summary

| Phase | Object | Schema | Grain |
|-------|--------|--------|-------|
| 1 | `coach_performance_summary` (extended) | gold_summary | (coach_id) |
| 1 | `nation_coach_glo_metrics` (extended) | gold_summary | (nation_id, coach_id) |
| 2 | `nation_results_cumulative_series` | gold_summary | (nation_id, game_seq) |
| 3 | `world_race_elo_quantiles` | gold_summary | (race_id) |
| 3 | `nation_race_strength_comparison` | gold_presentation | (nation_id, race_id, scope) |
| 4 | `coach_domestic_summary` | gold_summary | (coach_id) |
| 4 | `coach_race_relative_strength` | gold_summary | (coach_id, race_id) |
| 4 | `coach_race_nation_rank` | gold_summary | (nation_id, coach_id, race_id) |
| 5 | `nation_opponent_elo_bin_wdl` | gold_summary | (nation_id, bin_scheme_id, bin_index, opponent_group) |
| 5 | `nation_game_quality_bin_wdl` | gold_summary | (nation_id, bin_scheme_id, bin_index, opponent_group) |
| 6 | `nation_team_candidate_scores` | gold_summary | (nation_id, coach_id) |
| 6 | `nation_power_ranking` | gold_summary | (nation_id) |
| 7 | `global_coach_leaderboard` | gold_summary | (coach_id, leaderboard_type) |
| 7 | `global_biggest_movers` | gold_summary | (coach_id) |
| 7 | `global_race_leaderboard` | gold_summary | (race_id, coach_id) |
| 7 | `global_major_tournament_winners` | gold_summary | (tournament_id) |

Plus presentation views for each in 341/342/345.

---

## New Config Parameters

| Parameter | Value | Column | Phase |
|-----------|-------|--------|-------|
| Selector weight: rating | 0.30 | `selector_w_rating` | 6 |
| Selector weight: form | 0.25 | `selector_w_form` | 6 |
| Selector weight: opponent strength | 0.15 | `selector_w_opponent` | 6 |
| Selector weight: versatility | 0.15 | `selector_w_versatility` | 6 |
| Selector weight: international | 0.15 | `selector_w_international` | 6 |
