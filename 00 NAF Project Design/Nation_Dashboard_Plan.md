# Nation Dashboard — Overhaul Plan

> Date: 2026-02-23
> Status: AGREED (widget-by-widget review complete)
> Base file: Coach Dashboard (12).lvdash.json

## Layout

Single page, 12-column grid (`GRID_V1`). Logical sections flow top-to-bottom:

1. Identity & Overview
2. Ratings & Distributions
3. Performance & Bins
4. Head-to-Head
5. Rankings

All widgets get titles. All widgets use full 12-column width or sensible splits (e.g. 6+6 side by side).

---

## Global Filters

| Filter | Type | Notes |
|--------|------|-------|
| Nation | single-select | Primary nation |
| Comparison Nations | multi-select | For box plots, density, race histogram |
| GLO Metric | single-select | Values: **Peak**, **Median** (drop Mean). Controls all GLO-based widgets. |
| Race | single-select | Controls ELO box plot |
| Selector Focus | single-select | Values: GLO, Race, Opponent, Balanced. Controls team candidate scoring. |

---

## Section 1 — Identity & Overview

### W1: Nation Profile Card (table, ~4×TBD)
Dataset: `nation_overview` (needs rework)

Categories and rows:

**Ranking convention**: Most numeric stats in the profile should include a global nation rank
(DENSE_RANK across all nations), displayed as e.g. "245.3 (#12)". Gives instant context.

**Identity**
- Nation name (with flag emoji)
- NAF member coaches (total registered) + rank
- Active coaches (played in last 1 + current years) + rank

**Overview**
- Total games played (all coaches combined) — `round(game_participations / 2)` + rank
- Total tournaments hosted + rank
- W/D/L (all time, absolute)
- Last 50 games W/D/L (absolute) — if applicable

**Ratings (GLO)**
- Median GLO (all coaches) + rank
- Peak GLO (best single coach) + rank
- P90 GLO (top-end strength) + rank
- Power rank (from power ranking table)

**Race Profile**
- Most played race (and % of games)
- Race diversity index (if available) + rank

**National Team Strength**
- Top-8 average selector score + rank
- Top-8 average GLO peak and median + rank
- Coach pool depth (coaches meeting selector threshold) + rank

**Head-to-Head**
- Most played opponent nation
- Best record vs (highest PPG)
- Worst record vs (lowest PPG)

### W2: NAF Members Over Time (line chart, ~8×7) — NEW
- Weekly cumulative count of NAF-registered coaches for the nation
- Cumulative (members stay in DB or are removed entirely)
- **Requires new pipeline view** in 342 or 332

---

## Section 2 — Ratings & Distributions

### W3: GLO Box Plot — Nation vs World (box plot, ~6×7)
- Title: "GLO distribution (Nation vs World)"
- Dataset: `Nations vs World BOX with metrics`
- Respects GLO Metric filter (peak/median)
- **Colour scheme**: 3 distinct colours — World / Selected nation / Comparison nations
- World always included
- Same colour scheme used for ELO box plot

### W4: ELO Box Plot — Nation vs World (box plot, ~6×7)
- Title: "Race ELO distribution (Nation vs World)"
- Dataset: `Nations vs World and nations ELO`
- Controlled by Race filter
- **Same colour scheme** as GLO box plot
- World always included

### W5: GLO Density / Histogram (bar chart, ~6×8) — BROKEN, NEEDS NEW VIEW
- Title: "GLO rating histogram"
- Bins individual coach GLO values into buckets, counts per nation + World
- Respects GLO Metric filter (peak/median)
- **Requires new pipeline view**: binned distribution of per-coach GLO values
- Colour: World / Nation / Comparison nations

### W6: GLO Cumulative Density (line chart, ~6×7) — BROKEN, NEEDS NEW VIEW
- Title: "GLO cumulative distribution"
- Same new view as W5, but shows cumulative density as a line
- Respects GLO Metric filter

---

## Section 3 — Performance & Bins

### W7: Race Popularity Histogram (bar chart, ~12×8)
- Title: "Race popularity (% of coaches)"
- Shows proportion of coaches playing each race
- Compares: Selected nation / Comparison nations / World
- **Fix**: align scale — nation uses % (0–100), World baseline uses fraction (0–1). Standardise to % everywhere.
- **Remove** "Rest of World" — keep only World + selected + comparison nations

### W8: Home / Away Performance (table, ~12×4) — REWORK
- Title: "Performance context"
- **Rethink metrics**:
  - Away ratio: games abroad / total games
  - PPG vs foreign opponents (non-domestic games only)
  - PPG at home vs PPG away (against foreign opponents only)
- Drop domestic PPG (always 0.5)

### W9: W/D/L by Opponent GLO Bin (table, ~6×7)
- Title: "Results by opponent rating"
- **Respects GLO Metric filter** (bins by peak or median)
- Show: bin label, games, W/D/L, PPG
- Drop win% and TD diff columns

### W10: W/D/L by Game Quality Bin (table, ~6×7)
- Title: "Results by game quality"
- Game quality = average of both players' GLO (peak or median per filter)
- **Respects GLO Metric filter**
- Show: bin label, games, W/D/L, PPG
- Drop win% and TD diff columns

---

## Section 4 — Head-to-Head

### W11: GLO Exchange Bar Chart (bar chart, ~12×8)
- Title: "Average GLO exchange vs other nations"
- Shows average rating points gained/lost per opponent nation
- Already works — just needs title and sizing
- Add axis labels for clarity (positive = gain, negative = loss)

### W12: Nation Rivalries Table (table, ~12×6) — MERGED from old W11 + W12
- Title: "Nation rivalries"
- Merge Nation vs Nation records + Rival Score into one table
- Columns: opponent nation, games played, W/D/L (absolute), PPG
- Add rivalry intensity metric (combination of games played + closeness to 0.5 PPG)
- **Drop**: win%, TD for, TD against

---

## Section 5 — Rankings

### W13: National Team Candidates (table, ~12×10) — REWORK
- Title: "National team candidates"
- **Simplified selector score** (3 components):
  - Median GLO (percentile within nation)
  - Median race ELO (percentile within nation)
  - Median opponent GLO (percentile within nation)
- **Weighting controlled by Selector Focus filter**:
  - Focus GLO: 50% GLO, 25% Race, 25% Opponent
  - Focus Race: 25% GLO, 50% Race, 25% Opponent
  - Focus Opponent: 25% GLO, 25% Race, 50% Opponent
  - Balanced: 33/33/33
- Supplementary columns (not in score): versatility, international experience, form
- Selector rank = DENSE_RANK within nation by selector_score DESC

### W14: Nation Power Ranking (table, ~12×10)
- Title: "Nation power ranking"
- Top-8 average of new simplified selector_score
- Columns: power_rank, flag, nation, top_8_avg_selector_score, top_8_avg_glo_peak, top_8_avg_glo_median, coaches_eligible
- Power rank = DENSE_RANK by top_8_avg_selector_score DESC

---

## Removed Widgets

| Old # | Widget | Reason |
|-------|--------|--------|
| 3 | Summary Card (pivot) | Confusing, redundant with bin tables |
| 13 | Top coaches bar chart | Redundant with team candidates table |

---

## Pipeline Changes Required

### New views/tables needed

1. **GLO binned distribution** (332 or 342)
   - Grain: (nation_id, metric_type, glo_bin)
   - Bin individual coach GLO values (peak and median) into fixed-width buckets
   - Compute count and density per bin, per nation + World aggregate
   - Feeds W5 and W6

2. **NAF members over time** (332 or 342)
   - Grain: (nation_id, week)
   - Weekly cumulative count of registered coaches per nation
   - Feeds W2

3. **Simplified selector score** (332)
   - Rework `nation_team_candidate_scores`
   - 3 components: median GLO pctl, median race ELO pctl, median opponent GLO pctl
   - Composite with configurable focus weighting
   - Rework `nation_power_ranking` to use new selector

### Existing views to modify

4. **Race histogram view** — fix scale mismatch (% everywhere), remove Rest of World
5. **Home/Away view** — rework to away ratio + PPG vs foreign + PPG home vs away
6. **Opponent bin view** — respect GLO Metric filter (peak/median), use PPG not win%
7. **Game quality bin view** — respect GLO Metric filter, use PPG not win%
8. **Rivalry table** — merge two views, add rivalry intensity metric
9. **Nation profile view** — add all new profile rows (active coaches, hosted tournaments, h2h summary, team strength)

### Config changes (310)

10. **Selector weights**: replace 5-weight config with `selector_focus` parameter
11. **GLO metric default**: add param for default metric type (PEAK vs MEDIAN)

---

## Pipeline Object Mapping

Maps each plan widget to existing pipeline objects and the specific work needed.

| Widget | Existing summary (332) | Existing presentation (342) | Work needed |
|--------|----------------------|---------------------------|-------------|
| W1 Profile | `nation_overview_summary`, `nation_power_ranking` | `nation_profile_long` | **Heavy rework** — add active coaches, hosted tournaments, rankings, h2h summary, team strength rows. Pull from multiple summary tables. |
| W2 Members over time | `nation_coach_activity_timeseries` (has coach-level activity) | `nation_coach_activity_timeseries` (view exists) | **Check if usable** — may already have cumulative coach counts. If not, new view needed. |
| W3 GLO box plot | `nation_glo_metric_quantiles`, `world_glo_metric_quantiles` | `nation_coach_glo_metrics_long` | **Modify** — add metric_type parameter support (peak/median). Currently hardcoded to PEAK. |
| W4 ELO box plot | `nation_race_elo_peak_summary`, `world_race_elo_quantiles` | (dataset query inline) | **Modify** — add World baseline to ELO comparison. Same colour scheme as W3. |
| W5 GLO histogram | — | — | **NEW** — bin per-coach GLO (peak+median) into buckets. Compute count/density per nation + World. |
| W6 GLO cumulative density | — | — | **NEW** — same source as W5, compute cumulative density. |
| W7 Race popularity | `nation_race_summary` | `nation_race_meta` | **Fix** — multiply World baseline by 100 to align scales. Remove Rest of World from query. |
| W8 Home/Away | `nation_domestic_summary` | `nation_domestic_performance_display` | **Rework** — change metrics to away ratio, PPG vs foreign, PPG home vs away. Drop domestic PPG. |
| W9 Opp GLO bins | `nation_opponent_elo_bin_wdl` | `nation_opponent_elo_bin_wdl_display` | **Modify** — respect GLO Metric filter, switch to PPG, drop win%/TD. |
| W10 Game quality bins | `nation_game_quality_bin_wdl` | `nation_game_quality_bin_wdl_display` | **Modify** — respect GLO Metric filter, switch to PPG, drop win%/TD. |
| W11 GLO exchange | (inline in dataset query) | — | **Minor** — title + sizing only. Already works. |
| W12 Rivalries | `nation_vs_nation_summary`, `nation_rivalry_summary` | `nation_rivalry_meta` | **Merge** — combine into one view with PPG + rivalry intensity. Drop win%/TD. |
| W13 Team candidates | `nation_team_candidate_scores` | `nation_team_candidates_display` | **Rework** — simplify to 3 components (median GLO, median race ELO, median opp GLO). Add focus weighting. |
| W14 Power ranking | `nation_power_ranking` | `nation_power_ranking_display` | **Rework** — recalculate using simplified selector. Add glo_median column. |

### Datasets to remove from dashboard JSON

| Dataset name | Display name | Reason |
|-------------|-------------|--------|
| `41a0b7be` | Summary Card | Widget removed |
| `7ad190ff` | Top performing coaches | Widget removed |
| `44813501` | Untitled dataset | Unused |
| `a861eb98` | nation_peak_glo_distribution | Superseded by new GLO histogram view |
| `b8f184d0` | nation_race_peak_elo_distribution | Unused in plan |
| `db6a177f` | nation_race_peak_elo_summary | Unused in plan |
| `ab8b0908` | Nations vs World and nations GLO | Superseded by box plot dataset |
| `b3fb708b` | Nation vs World boxplot | Redundant with metrics-based box plot |
| `d1bb6b80` | Nation vs Nation records | Merged into rivalry table |
| `0cc8c0f4` | Rival Score | Merged into rivalry table |

### Datasets to keep (possibly modify)

| Dataset name | Display name | Widget |
|-------------|-------------|--------|
| `5436dca7` | nation_overview | W1 |
| `efd8dfe9` | Nations vs World BOX with metrics | W3 |
| `0d4ef502` | Nations vs World and nations ELO | W4 |
| `6ac8952f` | Density GLO | W5/W6 (rewrite query) |
| `1b775d70` | race hist | W7 |
| `680c657c` | GLO exchange | W11 |
| `ph5_domestic_perf` | Domestic/International Performance | W8 |
| `ph5_opp_bin_wdl` | Nation vs Opponent ELO Bins | W9 |
| `ph5_gq_bin_wdl` | Nation vs Game Quality Bins | W10 |
| `ph6_team_candidates` | National Team Candidates | W13 |
| `ph6_power_ranking` | Nation Power Ranking | W14 |
| `d429dda5` | nation_vs_nation_meta | Global Filter |
| `f43468b1` | nation_race_meta | Possibly W7 |

### New datasets needed

| Widget | Dataset | Source view |
|--------|---------|-------------|
| W2 | nation_members_timeseries | New or existing `nation_coach_activity_timeseries` |
| W12 | nation_rivalries | New merged rivalry view |

---

## Implementation Order

### Phase A — Pipeline foundations (no dashboard changes yet)
1. **New view**: GLO binned distribution (332 + 342) — feeds W5/W6
2. **New/check view**: NAF members over time (332/342) — feeds W2
3. **Modify**: `nation_glo_metric_quantiles` and box plot views to support peak/median filter
4. **Modify**: opponent bin + game quality bin views to support GLO Metric filter and PPG
5. **Modify**: race histogram scale fix (% everywhere, drop Rest of World)
6. **Modify**: home/away view (new metrics: away ratio, PPG vs foreign)

### Phase B — Selector rework
7. **Config** (310): add `selector_focus` parameter
8. **Rework** `nation_team_candidate_scores` (332): 3 components, focus weighting
9. **Rework** `nation_power_ranking` (332): use new selector
10. **Update** presentation views (342): candidates + power ranking displays

### Phase C — Profile card
11. **Rework** `nation_profile_long` (342): add all new rows with ranks
12. **Merge** rivalry views into single view (342)

### Phase D — Dashboard JSON
13. Rebuild single page: all 14 widgets, 12-col grid, titles, colours
14. Wire up Global Filters (nation, comparison, GLO metric, race, selector focus)
15. Remove stale datasets

### Phase E — Test
16. Run full pipeline: 332 → 342
17. Import dashboard, verify all widgets render
18. Spot-check data for a known nation (e.g. Denmark)
