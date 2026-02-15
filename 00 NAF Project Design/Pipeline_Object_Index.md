# Pipeline Object Index

> **Purpose**: Logical ordering and dependency map for every summary and presentation object. Intended as a reading guide ("what builds on what") and a build-order reference ("what must exist before I can run X").
> **Convention**: Objects are listed in build order within each section. Indentation shows dependency depth. `[V]` = view, `[T]` = table.

---

## How to read this document

Each domain (Coach, Nation, Tournament) follows the same structural pattern:

```
Gold Facts (inputs — not listed here, see 320)
  └─ Summary: Spines [V] → Series [V] → Summaries [T]
       └─ Presentation Core (identity shims, shared lookups)
            └─ Presentation: Dashboard contracts [V]
```

Views (`[V]`) are lightweight and rebuild instantly. Tables (`[T]`) are materialised with `load_timestamp` and represent the stable analytical outputs.

---

## Shared Foundation

These objects are consumed across all domains.

### Gold Dimensions (310)

| # | Object | Type | Grain |
|---|---|---|---|
| 1 | `gold_dim.analytical_config` | [T] | Singleton — all tuneable parameters |
| 2 | `gold_dim.date_dim` | [T] | 1 row per calendar date |
| 3 | `gold_dim.nation_dim` | [T] | 1 row per nation_id |
| 4 | `gold_dim.race_dim` | [T] | 1 row per race_id |
| 5 | `gold_dim.coach_dim` | [T] | 1 row per coach_id |
| 6 | `gold_dim.tournament_dim` | [T] | 1 row per tournament_id |
| 7 | `gold_dim.tournament_stat_dim` | [T] | 1 row per stat_id |
| 8 | `gold_dim.tournament_parameters` | [T] | 1 row per tournament_id (n_eff, k_value) |

### Gold Facts (320)

| # | Object | Type | Grain |
|---|---|---|---|
| 1 | `gold_fact.games_fact` | [T] | 1 row per game_id |
| 2 | `gold_fact.coach_games_fact` | [T] | 1 row per (coach_id, game_id) |
| 3 | `gold_fact.game_feed_for_ratings_fact` | [V] | 1 row per game_id (ordered for Elo) |
| 4 | `gold_fact.rating_history_fact` | [T] | 1 row per (scope, rating_system, game_id, coach_id, race_id) |

### Presentation Core (340)

Identity shims consumed by all domain-specific presentation notebooks.

| # | Object | Type | Grain | Sources |
|---|---|---|---|---|
| 1 | `gold_presentation.nation_flag_emoji_v` | [V] | 1 per nation_id | nation_dim |
| 2 | `gold_presentation.nation_identity_v` | [V] | 1 per nation_id | nation_dim, nation_flag_emoji_v |
| 3 | `gold_presentation.coach_identity_v` | [V] | 1 per coach_id | coach_dim, nation_identity_v |
| 4 | `gold_presentation.tournament_identity_v` | [V] | 1 per tournament_id | tournament_dim, nation_identity_v |
| 5 | `gold_presentation.race_identity_v` | [V] | 1 per race_id | race_dim |
| 6 | `gold_presentation.global_elo_bin_scheme` | [V] | 1 per (scheme_id, bin_index) | gold_summary.global_elo_bin_scheme |

---

## Coach Domain (331 → 341)

### Summary Layer (331) — Spines

Spines define the grain of an analytical object without computing metrics.

| # | Object | Type | Grain | Sources |
|---|---|---|---|---|
| 1 | `coach_game_spine_v` | [V] | 1 per (coach_id, game_id) | coach_games_fact |
| 2 | `coach_rating_history_spine_v` | [V] | 1 per (scope, rating_system, coach_id, race_id, game_id) | rating_history_fact |

### Summary Layer (331) — Filtered History Views

Scope-specific slices of the rating history spine.

| # | Object | Type | Grain | Sources |
|---|---|---|---|---|
| 3 | `coach_race_elo_rating_history_v` | [V] | 1 per (rating_system, coach_id, race_id, game_id) — RACE scope | coach_rating_history_spine_v |
| 4 | `coach_global_elo_rating_history_v` | [V] | 1 per (rating_system, coach_id, game_id) — GLOBAL scope | coach_rating_history_spine_v |
| 5 | `coach_global_elo_game_spine_v` | [V] | 1 per (rating_system, coach_id, game_id) — GLOBAL with game context | coach_global_elo_rating_history_v |

### Summary Layer (331) — Series

Cumulative/sequential computation over spines.

| # | Object | Type | Grain | Sources |
|---|---|---|---|---|
| 6 | `coach_results_cumulative_series_v` | [V] | 1 per (scope, coach_id, scope_race_id, game_id) | coach_game_spine_v |

### Summary Layer (331) — Core Summaries

Materialised analytical products.

| # | Object | Type | Grain | Sources |
|---|---|---|---|---|
| 7 | `coach_rating_race_summary` | [T] | 1 per (rating_system, coach_id, race_id) | coach_race_elo_rating_history_v, analytical_config |
| 8 | `coach_rating_global_elo_summary` | [T] | 1 per (rating_system, coach_id) | coach_global_elo_rating_history_v, analytical_config |
| 9 | `coach_performance_summary` | [T] | 1 per coach_id | coach_game_spine_v, tournament_dim, coach_rating_global_elo_summary |
| 9a | `coach_form_summary` | [T] | 1 per coach_id | rating_history_fact, analytical_config |
| 10 | `coach_race_summary` | [T] | 1 per (rating_system, coach_id, race_id) | coach_game_spine_v, coach_rating_race_summary, analytical_config |
| 11 | `coach_tournament_performance_summary` | [T] | 1 per (rating_system, coach_id, tournament_id) | coach_game_spine_v, coach_global_elo_rating_history_v |

### Summary Layer (331) — Streaks

| # | Object | Type | Grain | Sources |
|---|---|---|---|---|
| 12 | `coach_streak_segments` | [T] | 1 per (scope, coach_id, scope_race_id, streak_type, group_id) | coach_game_spine_v |
| 13 | `coach_streak_summary` | [T] | 1 per (scope, coach_id, scope_race_id) | coach_streak_segments, coach_dim |

### Summary Layer (331) — Opponent Analytics

| # | Object | Type | Grain | Sources |
|---|---|---|---|---|
| 14 | `coach_opponent_summary` | [T] | 1 per (rating_system, coach_id, opponent_coach_id) | coach_global_elo_game_spine_v |
| 15 | `coach_opponent_global_elo_enriched_v` | [V] | 1 per (rating_system, coach_id, opponent_coach_id) | coach_opponent_summary, coach_rating_global_elo_summary |
| 16 | `coach_opponent_global_elo_all_opponents_summary_v` | [V] | 1 per (rating_system, coach_id) | coach_opponent_global_elo_enriched_v |
| 17 | `coach_opponent_global_elo_mean_summary_v` | [V] | 1 per (rating_system, coach_id) | coach_opponent_global_elo_enriched_v, coach_rating_global_elo_summary, analytical_config |

### Summary Layer (331) — Binning

| # | Object | Type | Grain | Sources |
|---|---|---|---|---|
| 18 | `global_elo_bin_scheme_config` | [T] | 1 per (scheme_type, target_num_bins) — seed data | (inline) |
| 19 | `global_elo_bin_scheme` | [T] | 1 per (bin_scheme_id, bin_index) | global_elo_bin_scheme_config, coach_rating_global_elo_summary |
| 20 | `coach_opponent_global_elo_bin_summary` | [T] | 1 per (rating_system, coach_id, bin_scheme_id, bin_index) | coach_opponent_global_elo_enriched_v, global_elo_bin_scheme |

### Presentation Layer (341)

All views. Listed in build order (some depend on earlier presentation views).

| # | Object | Type | Grain | Key sources |
|---|---|---|---|---|
| 1 | `coach_profile` | [V] | 1 per coach_id | coach_race_summary, coach_streak_summary, coach_performance_summary, coach_form_summary, coach_rating_global_elo_summary, coach_identity_v |
| 2 | `coach_race_performance` | [V] | 1 per (coach_id, race_id) | coach_race_summary, coach_identity_v, race_dim |
| 3 | `coach_opponent_rating` | [V] | 1 per (coach_id, opponent_coach_id) | coach_opponent_summary, coach_identity_v |
| 4 | `coach_opponent_highlights` | [V] | 1 per coach_id | coach_opponent_global_elo_enriched_v, coach_identity_v |
| 5 | `coach_opponent_insights` | [V] | 1 per (coach_id, category) | coach_opponent_rating, coach_rating_global_elo_summary |
| 6 | `coach_opponent_top5_by_games` | [V] | 1 per (coach_id, rank) | coach_opponent_rating, coach_profile |
| 7 | `coach_opponent_global_elo_bin_insights` | [V] | 1 per (coach_id, scheme_id, bin_index) | coach_opponent_global_elo_bin_summary, global_elo_bin_scheme, coach_identity_v |
| 8 | `coach_opponent_global_elo_bin_results_long` | [V] | 1 per (coach_id, scheme_id, bin_index, result) | coach_opponent_global_elo_bin_insights |
| 9 | `coach_cumulative_results_daily_series` | [V] | 1 per (scope, race_id, coach_id, day, result) | coach_results_cumulative_series_v, coach_dim |
| 10 | `coach_results_all_long` | [V] | 1 per (coach_id, level, result) | coach_performance_summary, coach_identity_v |
| 11 | `coach_streak_detail` | [V] | 1 per (scope, coach_id, race_id, streak_type, group_id) | coach_streak_segments, coach_identity_v, race_dim, date_dim |
| 12 | `coach_streak_overview` | [V] | 1 per (scope, coach_id, race_id) | coach_streak_summary, coach_streak_segments, coach_identity_v, race_dim, date_dim |
| 13 | `coach_tournament_performance` | [V] | 1 per (coach_id, tournament_id) | coach_tournament_performance_summary, tournament_dim, nation_dim, date_dim |
| 14 | `coach_global_elo_rating_history` | [V] | 1 per (rating_system, coach_id, game_id) | coach_global_elo_rating_history_v, coach_identity_v, date_dim |
| 15 | `coach_race_elo_rating_history` | [V] | 1 per (rating_system, coach_id, race_id, game_id) | coach_race_elo_rating_history_v, coach_identity_v, race_dim, date_dim |

---

## Nation Domain (332 → 342)

### Summary Layer (332)

| # | Object | Type | Grain | Sources |
|---|---|---|---|---|
| 1 | `nation_coach_glo_metrics` | [T] | 1 per (nation_id, coach_id) | rating_history_fact, coach_dim, analytical_config |
| 2 | `nation_overview_summary` | [T] | 1 per nation_id | coach_games_fact, games_fact, tournament_dim, nation_coach_glo_metrics |
| 3 | `nation_games_timeseries` | [T] | 1 per (nation_id, game_date) | games_fact, tournament_dim |
| 4 | `nation_coach_activity_timeseries` | [T] | 1 per (nation_id, game_date) | coach_games_fact, coach_dim |
| 5 | `nation_overview_comparison` | [T] | 1 per (focus_nation_id, comparison_group) | nation_overview_summary, nation_coach_glo_metrics |
| 6 | `nation_race_summary` | [T] | 1 per (nation_id, race_id) | coach_games_fact, coach_dim |
| 7 | `nation_glo_metric_quantiles` | [T] | 1 per (nation_id, metric_type) | nation_coach_glo_metrics |
| 8 | `nation_vs_nation_summary` | [T] | 1 per (nation_id, opponent_nation_id) | games_fact, coach_dim, rating_history_fact |
| 9 | `nation_rivalry_summary` | [V] | 1 per (nation_id, opponent_nation_id) | nation_vs_nation_summary, analytical_config |
| 10 | `nation_coach_race_elo_peak` | [V] | 1 per (coach_id, race_id) | coach_rating_race_summary (331), coach_dim |
| 11 | `nation_race_elo_peak_summary` | [T] | 1 per (nation_id, race_id) | nation_coach_race_elo_peak |
| 12 | `world_glo_metric_quantiles` | [V] | Singleton per metric_type | nation_coach_glo_metrics |
| 13 | `coach_opponent_glo_bin_summary` | [V] | 1 per (coach_id, bin_index) | coach_opponent_summary (331), coach_glo_peak |

### Presentation Layer (332 inline + 342)

Some presentation views are defined inline in 332 rather than 342. Listed together for logical ordering.

| # | Object | Type | Grain | Notebook | Key sources |
|---|---|---|---|---|---|
| 1 | `nation_overview` | [V] | 1 per nation_id | 342 | nation_overview_summary, nation_dim |
| 2 | `nation_overview_comparison` | [V] | 1 per (focus_nation_id, group) | 342 | gold_summary.nation_overview_comparison, nation_dim |
| 3 | `nation_profile_long` | [V] | 1 per (nation_id, sort_order) | 342 | nation_overview |
| 4 | `nation_race_meta` | [V] | 1 per (nation_id, race_id) | 342 | nation_race_summary, nation_dim, race_dim |
| 5 | `nation_vs_nation_meta` | [V] | 1 per (nation_id, opponent_nation_id) | 342 | nation_vs_nation_summary, nation_dim |
| 6 | `nation_rivalry_meta` | [V] | 1 per (nation_id, opponent_nation_id) | 342 | nation_rivalry_summary, nation_dim |
| 7 | `nation_coach_activity_timeseries` | [V] | 1 per (nation_id, game_date) | 342 | gold_summary.nation_coach_activity_timeseries, nation_dim |
| 8 | `nation_glo_peak_distribution` | [V] | 1 per (nation_id, coach_id) | 342 | nation_coach_glo_metrics, nation_dim, coach_dim |
| 9 | `nation_glo_metric_quantiles` | [V] | 1 per (nation_id, metric_type) + World | 332/342 | gold_summary.nation_glo_metric_quantiles, world_glo_metric_quantiles, nation_dim |
| 10 | `nation_glo_peak_boxplot` | [V] | 1 per (nation_id, metric_type) | 332 | nation_glo_metric_quantiles (presentation) |
| 11 | `nation_glo_peak_card_long` | [V] | 1 per (nation_id, metric) | 332 | nation_glo_metric_quantiles (presentation) |
| 12 | `nation_race_elo_peak_summary` | [V] | 1 per (nation_id, race_id) + World | 342 | gold_summary.nation_race_elo_peak_summary, nation_dim, race_dim |
| 13 | `nation_coach_glo_metrics_long` | [V] | 1 per (nation_id, coach_id, metric) | 342 | nation_coach_glo_metrics, nation_dim, coach_dim |
| 14 | `nation_top_coach_opponent_bin_perf` | [V] | 1 per (nation_id, coach_id, bin_index) | 342 | nation_coach_glo_metrics, coach_opponent_glo_bin_summary, nation_dim, coach_dim |

---

## Tournament Domain (333 → 343)

*Placeholder status — objects exist but are not finalised.*

### Summary Layer (333)

| # | Object | Type | Grain | Sources |
|---|---|---|---|---|
| 1 | `tournament_winners` | [T] | 1 per (tournament_id, winner) | tournament_stat_dim, tournament_statistics_fact, tournament_dim, coach_dim, race_dim |
| 2 | `tournament_summary` | [T] | 1 per tournament_id | coach_games_fact, coach_dim, coach_rating_global_summary, tournament_winners |
| 3 | `tournament_race_summary` | [T] | 1 per (tournament_id, race_id) | coach_games_fact |
| 4 | `global_race_summary` | [T] | All games aggregate per race | coach_games_fact |
| 5 | `tournament_nation_summary` | [T] | 1 per (tournament_id, nation) | coach_games_fact, coach_dim |

### Presentation Layer (343)

| # | Object | Type | Grain | Key sources |
|---|---|---|---|---|
| 1 | `tournament_overview` | [V] | 1 per tournament_id | tournament_dim, tournament_summary, tournament_parameters, coach_dim |
| 2 | `tournament_race_meta` | [V] | 1 per (tournament_id, race_id) | tournament_race_summary, race_dim |
| 3 | `tournament_nation_meta` | [V] | 1 per (tournament_id, nation) | tournament_nation_summary, nation_dim |

---

## Race Domain (334 → 344)

*Not started — notebooks are empty.*

---

## Cross-domain Dependencies

Some objects in one domain depend on objects in another. Key cross-references:

| Consumer (domain) | Object | Depends on (domain) | Object |
|---|---|---|---|
| Nation (332) | `nation_coach_race_elo_peak` | Coach (331) | `coach_rating_race_summary` |
| Nation (332) | `coach_opponent_glo_bin_summary` | Coach (331) | `coach_opponent_summary` |
| Nation (342) | `nation_top_coach_opponent_bin_perf` | Coach (331) | `coach_opponent_glo_bin_summary` (via 332) |
| Tournament (333) | `tournament_summary` | Coach (331) | `coach_rating_global_summary` |

This means **331 (coach summary) must run before 332 (nation summary) and 333 (tournament summary)**.

---

## Build Execution Order

The correct notebook execution order, respecting all dependencies:

```
310  Gold Dim           (dims + analytical_config + tournament_parameters)
320  Gold Fact          (games_fact, coach_games_fact, rating_history_fact)
331  Gold Summary Coach (spines → series → summaries → streaks → opponent → bins)
332  Gold Summary Nation
333  Gold Summary Tournament
340  Gold Presentation Core (identity shims)
341  Gold Presentation Coach
342  Gold Presentation Nation
343  Gold Presentation Tournament
```

Note: 340 (presentation core) can technically run after 310 (it only needs dims), but logically it groups with the presentation phase. 332 and 333 can run in parallel after 331.
