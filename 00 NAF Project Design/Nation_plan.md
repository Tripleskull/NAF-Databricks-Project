> **Deprecated.** This document has been superseded by `Nation_Dashboard_Plan.md` and `NAF_Design_Specification.md`. Kept for historical reference only — do not update.

# Nation Dashboard vNext — single plan (content + strategy + architecture + metrics)

## 0) Goals (what this dashboard should answer)
A) **Top performers** (Top N, not fixed 10)
- Identify Top N coaches in a nation by:
  - **GLOBAL rating** (race-agnostic scope)
  - **RACE rating** for user-chosen race(s)
- Compare those Top N coaches via **W/D/L by binned opponent ratings**, where the “rating metric” is selectable:
  - MEAN / MEDIAN / PEAK

B) **Nation snapshot**
- Size, activity, quality (counts + rating summaries)

C) **Performance trends vs Rest of World**
- Baseline opponent group is **REST_OF_WORLD** (not specific nations)

D) **Rivalries**
- Top rivalries (specific opponent nations)
- Comparisons vs user-chosen opponent nations (detail section)

---

## 1) Architecture & modelling rules (fixed)
### 1.1 Medallion responsibilities (Unity Catalog)
- `naf_catalog.gold_dim`: minimal entities (nation, coach, race, date). No analytics.
- `naf_catalog.gold_fact`: event truth only (game-level coach perspective; no aggregates).
- `naf_catalog.gold_summary`: all analytics (ranks, windows, bins, trends, rivalry scores).
- `naf_catalog.gold_presentation`: thin dashboard contracts (joins for display, stable labels; no metric recomputation).

### 1.2 Key conventions (must follow)
- `snake_case`
- Keys: `<entity>_id` (e.g., `nation_id`, `coach_id`, `race_id`, `date_id`)
- `race_id = 0` reserved for GLOBAL scope (“none” race), never a playable race.
- Nation attributes (names/flags) live only in `nation_dim`; everything else joins by `nation_id`.

---

## 2) Canonical source tables/views we will reuse (no re-derivation)
### 2.1 GLOBAL rating metrics (coach-level)
Use `naf_catalog.gold_summary.coach_rating_global_elo_summary` as the single source of truth for:
- `global_elo_current`
- distribution stats over rating history: `global_elo_peak_*`, `global_elo_mean_*`, `global_elo_median_*`
- threshold/window parameters per row (e.g., `threshold_games`, `last_n_games_window`)

### 2.2 RACE rating + performance (coach,race-level)
Use `naf_catalog.gold_summary.coach_race_summary` (built from `coach_rating_race_summary` + games aggregates) as the single source of truth for:
- race performance: W/D/L, points per game, TD stats, activity dates
- race rating stats: `elo_peak_*`, `elo_mean_*`, `elo_median_*`, `elo_current`
- **validity flags** and threshold/window values:
  - `min_games_threshold_race_performance` (=10) and `is_valid_min_games_race_performance`
  - `min_games_threshold_race_elo` (burn-in threshold) + `is_valid_min_games_race_elo` / `is_valid_post_threshold_race_elo`
  - `last_n_games_window` + `is_valid_last_50_race_elo`

### 2.3 Game results base (for trends & nation-vs-row)
Use `naf_catalog.gold_fact.coach_games_fact` as the event truth base.
Key fields:
- `coach_id`, `opponent_coach_id`
- `race_id`, `opponent_race_id`
- `date_id`, `event_timestamp`
- `td_for`, `td_against`, `td_diff`
- `result_str` (W/D/L) and `result_numeric` (1/0.5/0)

### 2.4 Existing GLOBAL opponent bin scheme (reusable)
There is an established global Elo bin backbone:
- `naf_catalog.gold_summary.global_elo_bin_scheme` (scheme IDs + numeric edges)
- `naf_catalog.gold_presentation.global_elo_bin_scheme` (stable display labels)
- `naf_catalog.gold_summary.coach_opponent_global_elo_bin_summary` (coach x bin aggregates)
- `naf_catalog.gold_summary.coach_opponent_global_elo_enriched_v` where
  `opponent_global_elo_for_binning = COALESCE(opponent_global_elo_peak_all, opponent_global_elo_current)`

We will **reuse the scheme IDs + labels** for comparability, but we will generalize “value used for binning” to support MEAN/MEDIAN/PEAK selections (see Section 4).

---

## 3) Dashboard controls (single shared selector vocabulary)
### 3.1 Global controls
- `nation_name_display` (single select; resolved to `nation_id`)
- `top_n` (integer; applied to Top performers sections)
- `rating_scope` ∈ {GLOBAL, RACE}
- `race_selector` (multi-select; enabled when `rating_scope = RACE`)
- `metric_type` ∈ {PEAK, MEAN, MEDIAN}
- `metric_phase` ∈ {ALL, POST_THRESHOLD, LAST_50}
- `opponent_group` fixed default = REST_OF_WORLD for trend/binned sections
- `comparison_nations` (multi-select) used only in Rivalries section (optional)
- Optional gating controls (UI niceties, not new metrics):
  - `require_full_last_50_window` (boolean; see “LAST_50 semantics”)
  - `exclude_unknown_nation` for opponent grouping (optional)

---

## 4) Metrics plan (how to use thresholds/windows consistently)
### 4.1 What MEAN/MEDIAN/PEAK mean (do not reinterpret)
Metric stats are **distribution statistics over the rating history series** already computed in the coach context:
- PEAK = `MAX(rating_after)` within the phase window
- MEAN = `AVG(rating_after)` within the phase window
- MEDIAN = `PERCENTILE_APPROX(rating_after, 0.5)` within the phase window

### 4.2 Phase definitions & thresholds (must match coach context)
#### GLOBAL (coach-level)
- Burn-in threshold: `threshold_games = 50`
- Post-threshold phase is games strictly **after** the first N games:
  - rule: `game_number_asc > threshold_games`
- Last-50 phase uses **most recent min(50, global_elo_games)** games.
  - Additional invariant checked in coach notebook: when `global_elo_games < last_n_games_window`, `*_last_50 == *_all`

Implication for dashboard usage:
- `metric_phase = POST_THRESHOLD` will naturally exclude/NULL out coaches without enough games in that phase (no custom filter needed beyond “value is not null”).
- `metric_phase = LAST_50` is always populated, but it can equal ALL when games < 50. If we want “true last 50 only”, use UI toggle `require_full_last_50_window = TRUE` to require `global_elo_games >= last_n_games_window`.

#### RACE (coach,race-level)
- Burn-in threshold: `threshold_games = 25`
- Post-threshold phase: games strictly after first N games:
  - rule: `game_number_asc > threshold_games` (burn-in 25)
- Last-50 phase uses **most recent min(50, games_with_race)** games.
- Validity flags in `coach_race_summary` are canonical:
  - `is_valid_post_threshold_race_elo` uses `elo_games_with_race > min_games_threshold_race_elo`
  - `is_valid_last_50_race_elo` uses `elo_games_with_race >= last_n_games_window`
  - `is_valid_min_games_race_performance` uses `games_played >= 10`

Implication for dashboard usage:
- For race leaderboards: filter eligibility using the flags that match the selected `metric_phase`.
- For race LAST_50: the same “full window only” UX applies using `is_valid_last_50_race_elo`.

### 4.3 Single mapping function (prevents metric drift)
Every dashboard dataset that needs a rating value will use the same mapping:
- Inputs: `(rating_scope, metric_phase, metric_type, race_id)`
- Output: `rating_value` (DOUBLE), plus `games_count_for_phase` for gating UX

GLOBAL rating_value mapping (from `coach_rating_global_elo_summary`):
- ALL: `global_elo_{peak|mean|median}_all`
- POST_THRESHOLD: `global_elo_{peak|mean|median}_post_threshold`
- LAST_50: `global_elo_{peak|mean|median}_last_50`

RACE rating_value mapping (from `coach_race_summary`):
- ALL: `elo_{peak|mean|median}_all`
- POST_THRESHOLD: `elo_{peak|mean|median}_post_threshold`
- LAST_50: `elo_{peak|mean|median}_last_50`

### 4.4 Binning plan for “W/D/L by binned ratings”
Principle: keep bin labels stable, change only “value being bucketed”.

- GLOBAL binned charts:
  - reuse existing `global_elo_bin_scheme` IDs/labels
  - compute `opponent_rating_for_binning` as the opponent coach’s `rating_value` under the SAME `(metric_phase, metric_type)` as selected for the chart
  - then apply `WIDTH_BUCKET` using the chosen bin scheme edges

- RACE binned charts:
  - preferred: introduce a **race-elo bin scheme** mirroring the global pattern (so bins are interpretable within race scope)
  - fallback (if we want to avoid a new scheme now): use a quantile binning approach per race_id in summary (but that changes semantics vs global)
  - decision: default to “create race bin scheme” if race scope binning is required for launch

---

## 5) Definitions used across the dashboard
### 5.1 REST_OF_WORLD (ROW)
Given selected nation N:
- ROW = opponent coaches where `opponent_nation_id != N`
- Optional config: whether `opponent_nation_id = 0` (Unknown) is included in ROW.

### 5.2 Rivalries
Rivalry is explicitly:
- selected nation vs **specific opponent nations**
- “Top rivalries” is a ranked list with stable components (games, closeness, share, etc.)
- this is separate from ROW baseline sections

---

## 6) Dashboard sections (content + required datasets)
### 6.1 Nation snapshot (B)
Content:
- #coaches, #active coaches, #games, #tournaments, last activity date
- rating summaries for GLOBAL and for chosen race(s) (optional)
Strategy:
- build analytics in summary; presentation only joins nation labels

### 6.2 Top performers (A)
Content:
- Top N coaches for selected nation, for:
  - GLOBAL scope (race_id=0)
  - RACE scope (for selected race(s))
- Show rating value (per selected `metric_phase` + `metric_type`) and context:
  - games count, last active date, optional world rank
Eligibility:
- use post-threshold / last-50 validity rules described in Section 4 (no dashboard-specific thresholds)

### 6.3 Compare Top performers via W/D/L by binned opponent rating (A)
Content:
- For the Top N coaches, show W/D/L rates by opponent rating bins
- Baseline opponent group default = ROW
Notes:
- bins must be consistent with the selected metric definition (same phase+type)

### 6.4 Trends vs Rest of World (C)
Content:
- Time series (monthly via `date_dim.year_month`) of W/D/L vs ROW
- Optional split by race_id to answer “performance with each race vs ROW”
Inputs:
- `coach_games_fact` joined to coach/opponent coach for nation IDs, plus `date_dim`

### 6.5 Rivalries (D)
Content:
- Top rivalries table: games, W/D/L, win%, “rivalry score” components
- Detail comparisons vs chosen opponent nations (time series + summary KPIs)
Notes:
- Rivalries are the only place we compare vs specific nations (plus optional peer selection)

---

## 7) Proposed data products (new objects) — designs only (no build yet)
### 7.1 gold_summary (analytics layer)
1) `gold_summary.nation_snapshot_summary`
- Grain: 1 row per `nation_id` (optionally `nation_id, year_month` for time variants)
- PK: `nation_id` (or `nation_id, year_month`)
- FKs: `nation_id`, optional `date_id/year_month`
- Metrics: counts + last activity + rating summaries

2) `gold_summary.nation_top_performer_rank_summary`
- Grain: 1 row per `(nation_id, rating_scope, race_id, metric_phase, metric_type, coach_id)`
- PK: same
- FKs: nation_id, coach_id, race_id
- Metrics: `rating_value`, `nation_rank`, optional `world_rank`, `games_played_context`, `is_eligible`

3) `gold_summary.nation_top_performer_bin_wdl_summary`
- Grain: 1 row per `(nation_id, rating_scope, race_id, metric_phase, metric_type, coach_id, bin_scheme_id, bin_index)`
- PK: same
- FKs: nation_id, coach_id, race_id, (bin scheme)
- Metrics: games, wins, draws, losses, win_rate
- Filter: opponent group default ROW

4) `gold_summary.nation_wdl_over_time_vs_row_summary`
- Grain: `(nation_id, year_month)` and optional `(nation_id, year_month, race_id)`
- PK: as grain
- Metrics: games, W/D/L, win_rate, points per game (if needed)

5) `gold_summary.nation_rivalry_summary`
- Grain: `(nation_id, opponent_nation_id)`
- PK: same
- Metrics: games, W/D/L, win_rate + rivalry scoring components

6) `gold_summary.nation_rivalry_over_time_summary`
- Grain: `(nation_id, opponent_nation_id, year_month)`
- PK: same
- Metrics: games, W/D/L, win_rate, optional rating deltas/exchange

### 7.2 gold_presentation (dashboard contracts)
1) `gold_presentation.nation_snapshot`
- joins `nation_dim` for display fields

2) `gold_presentation.nation_top_performers`
- joins `nation_dim`, `coach_dim`, and `race_dim` for display
- includes `metric_phase`, `metric_type` columns (for dashboard filtering consistency)

3) `gold_presentation.nation_top_performer_bin_wdl`
- adds `bin_label_display` by joining `gold_presentation.global_elo_bin_scheme` (and future `race_elo_bin_scheme`)

4) `gold_presentation.nation_wdl_over_time_vs_row`
- adds `year_month` labels from `date_dim`

5) `gold_presentation.nation_rivalries`
- enriched top rivalries list with flags/labels

6) `gold_presentation.nation_rivalry_detail`
- opponent-nation time series for chosen opponent nations

---

## 8) Consistency & QA checklist (avoid silent drift)
- No dashboard dataset recomputes MEAN/MEDIAN/PEAK — it only selects from coach-context summaries.
- POST_THRESHOLD always uses strict “after burn-in” semantics (GLOBAL 50, RACE 25).
- LAST_50 respects “min(window, games)” semantics; optional UI gating for “full window only”.
- Nation attributes are never duplicated; always joined from `nation_dim`.
- GLOBAL scope always uses `race_id = 0`.
- Bins reuse stable scheme IDs + labels; only the bucketed rating value changes with selector.

---

## 9) Open decisions (small, but should be explicit)
1) For RACE scope binning: create `race_elo_bin_scheme` (recommended) vs quantile bins.
2) Include Unknown nation (`nation_id=0`) inside REST_OF_WORLD baseline or exclude by default.
3) Default selector values for:
   - `metric_type` (PEAK vs MEAN default)
   - `metric_phase` (ALL vs POST_THRESHOLD vs LAST_50)
   - `require_full_last_50_window` (TRUE/FALSE)

   
