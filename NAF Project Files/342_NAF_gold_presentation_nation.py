# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Presentation — Nation
# MAGIC
# MAGIC This notebook builds **dashboard-friendly views** in `naf_catalog.gold_presentation` for nation analytics.
# MAGIC
# MAGIC These views are **thin shapes** over the curated Gold model:
# MAGIC
# MAGIC - **Dimensions** (`naf_catalog.gold_dim`) provide stable IDs + canonical attributes (names, codes, flags).
# MAGIC - **Summaries/Facts** (`naf_catalog.gold_summary`, `naf_catalog.gold_fact`) provide all KPIs/aggregations and event truth.
# MAGIC - **Presentation** (`naf_catalog.gold_presentation`) only:
# MAGIC   - joins dims + summaries/facts,
# MAGIC   - adds light UI helpers (labels/buckets),
# MAGIC   - **does not** re-implement heavy business logic or re-aggregate base facts.
# MAGIC
# MAGIC ## Views created here
# MAGIC This notebook creates nation-facing views such as:
# MAGIC - `gold_presentation.nation_overview`
# MAGIC - `gold_presentation.nation_race_meta`
# MAGIC - `gold_presentation.nation_vs_nation_meta`
# MAGIC - `gold_presentation.nation_coach_activity_timeseries`
# MAGIC - `gold_presentation.nation_glo_peak_distribution`
# MAGIC - `gold_presentation.nation_race_elo_peak_distribution`
# MAGIC - `gold_presentation.nation_race_elo_peak_summary`
# MAGIC
# MAGIC ## Notebook conventions
# MAGIC - **One object per SQL cell** (one `CREATE OR REPLACE VIEW` per cell).
# MAGIC - Put the target object on the **first line** for collapsible navigation, e.g.  
# MAGIC   `%sql -- VIEW: naf_catalog.gold_presentation.nation_overview`
# MAGIC - Keep ID columns explicit (e.g. `opponent_nation_id`, `race_id`), and treat presentation as formatting/orchestration only.
# MAGIC

# COMMAND ----------

# MAGIC %sql -- SCHEMA: naf_catalog.gold_presentation
# MAGIC CREATE SCHEMA IF NOT EXISTS naf_catalog.gold_presentation;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- VIEW: naf_catalog.gold_presentation.nation_overview_comparison
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE:
# MAGIC --   Adds focus nation display fields + comparison labels for dashboards
# MAGIC -- GRAIN:
# MAGIC --   One row per (focus_nation_id, comparison_group)
# MAGIC -- SOURCES:
# MAGIC --   naf_catalog.gold_summary.nation_overview_comparison
# MAGIC --   naf_catalog.gold_dim.nation_dim
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.nation_overview_comparison AS
# MAGIC SELECT
# MAGIC   c.focus_nation_id,
# MAGIC   nd.nation_name,
# MAGIC   nd.nation_name_display,
# MAGIC   nd.fifa_code,
# MAGIC   nd.flag_code,
# MAGIC
# MAGIC   c.comparison_group,
# MAGIC   CASE
# MAGIC     WHEN c.comparison_group = 'NATION'        THEN nd.nation_name_display
# MAGIC     WHEN c.comparison_group = 'REST_OF_WORLD' THEN 'Rest of World'
# MAGIC     WHEN c.comparison_group = 'WORLD'         THEN 'World'
# MAGIC     ELSE c.comparison_group
# MAGIC   END AS comparison_group_display,
# MAGIC
# MAGIC   CASE
# MAGIC     WHEN c.comparison_group = 'NATION'        THEN 1
# MAGIC     WHEN c.comparison_group = 'REST_OF_WORLD' THEN 2
# MAGIC     WHEN c.comparison_group = 'WORLD'         THEN 3
# MAGIC     ELSE 99
# MAGIC   END AS comparison_group_sort,
# MAGIC
# MAGIC   c.coaches_count,
# MAGIC   c.coach_participations_count,
# MAGIC   c.avg_points_per_game,
# MAGIC
# MAGIC   c.coaches_global_pct,
# MAGIC   c.coach_participations_global_pct,
# MAGIC
# MAGIC   c.games_hosted_count,
# MAGIC   c.tournaments_hosted_count,
# MAGIC   c.games_hosted_global_pct,
# MAGIC   c.tournaments_hosted_global_pct,
# MAGIC
# MAGIC   c.glo_coaches_count,
# MAGIC   c.glo_peak_max,
# MAGIC   c.glo_peak_mean,
# MAGIC   c.glo_peak_median,
# MAGIC
# MAGIC   c.load_timestamp
# MAGIC FROM naf_catalog.gold_summary.nation_overview_comparison c
# MAGIC LEFT JOIN naf_catalog.gold_dim.nation_dim nd
# MAGIC   ON c.focus_nation_id = nd.nation_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql -- VIEW: naf_catalog.gold_presentation.nation_overview
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE:
# MAGIC --   - One row per nation_id
# MAGIC --   - Dashboard-friendly nation overview:
# MAGIC --       * display attributes from naf_catalog.gold_dim.nation_dim
# MAGIC --       * rollups from naf_catalog.gold_summary.nation_overview_summary
# MAGIC --       * light bucketing for UI (size / strength)
# MAGIC -- GRAIN:
# MAGIC --   - One row per nation_id
# MAGIC -- SOURCES:
# MAGIC --   - naf_catalog.gold_dim.nation_dim
# MAGIC --   - naf_catalog.gold_summary.nation_overview_summary
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.nation_overview AS
# MAGIC SELECT
# MAGIC   -- Identity + display
# MAGIC   n.nation_id,
# MAGIC   n.nation_name,
# MAGIC   n.nation_name_display,
# MAGIC   n.fifa_code,
# MAGIC   n.flag_code,
# MAGIC
# MAGIC   -- Participation volume (from summary)
# MAGIC   COALESCE(s.coaches_count, 0)              AS coaches_count,
# MAGIC   COALESCE(s.game_representations_count, 0) AS games_count,
# MAGIC   COALESCE(s.tournaments_attended_count, 0) AS tournaments_count,
# MAGIC   COALESCE(s.coach_participations_count, 0) AS coach_participations_count,
# MAGIC
# MAGIC   -- Performance / ratings
# MAGIC   s.avg_points_per_game AS points_per_game,
# MAGIC   s.glo_peak_max,
# MAGIC   s.glo_peak_mean,
# MAGIC   s.glo_peak_median,
# MAGIC   s.avg_opponent_glo_peak,
# MAGIC
# MAGIC   -- Global share (from summary)
# MAGIC   s.coaches_global_pct,
# MAGIC   s.game_representations_global_pct AS games_global_pct,
# MAGIC   s.tournaments_attended_global_pct AS tournaments_global_pct,
# MAGIC   s.coach_participations_global_pct,
# MAGIC
# MAGIC   -- Hosting (from summary)
# MAGIC   COALESCE(s.games_hosted_count, 0)       AS games_hosted_count,
# MAGIC   COALESCE(s.tournaments_hosted_count, 0) AS tournaments_hosted_count,
# MAGIC   s.games_hosted_global_pct,
# MAGIC   s.tournaments_hosted_global_pct,
# MAGIC
# MAGIC   -- Dashboard helper
# MAGIC   (COALESCE(s.coaches_count, 0) > 0
# MAGIC     OR COALESCE(s.games_hosted_count, 0) > 0
# MAGIC     OR COALESCE(s.tournaments_hosted_count, 0) > 0) AS has_activity,
# MAGIC
# MAGIC   -- Presentation-only bucketing
# MAGIC   CASE
# MAGIC     WHEN COALESCE(s.coaches_count, 0) >= 200 THEN 'Large'
# MAGIC     WHEN COALESCE(s.coaches_count, 0) >= 50  THEN 'Medium'
# MAGIC     WHEN COALESCE(s.coaches_count, 0) >= 10  THEN 'Small'
# MAGIC     WHEN COALESCE(s.coaches_count, 0) > 0    THEN 'Tiny'
# MAGIC     ELSE 'No data'
# MAGIC   END AS nation_size_bucket,
# MAGIC
# MAGIC   CASE
# MAGIC     WHEN s.glo_peak_mean IS NULL THEN 'No data'
# MAGIC     WHEN s.glo_peak_mean >= 200 THEN 'Top tier'
# MAGIC     WHEN s.glo_peak_mean >= 175 THEN 'Strong'
# MAGIC     WHEN s.glo_peak_mean >= 150 THEN 'Developing'
# MAGIC     ELSE 'Emerging'
# MAGIC   END AS nation_strength_bucket,
# MAGIC
# MAGIC   s.load_timestamp
# MAGIC FROM naf_catalog.gold_dim.nation_dim AS n
# MAGIC LEFT JOIN naf_catalog.gold_summary.nation_overview_summary AS s
# MAGIC   ON n.nation_id = s.nation_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql -- VIEW: naf_catalog.gold_presentation.nation_race_meta
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE:
# MAGIC --   - One row per (nation_id, race_id)
# MAGIC --   - UI-friendly nation x race composition + performance (coach nationality-based)
# MAGIC -- SOURCES:
# MAGIC --   - naf_catalog.gold_summary.nation_race_summary
# MAGIC --   - naf_catalog.gold_dim.nation_dim
# MAGIC --   - naf_catalog.gold_dim.race_dim
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.nation_race_meta AS
# MAGIC SELECT
# MAGIC   -- Nation identity + display
# MAGIC   n.nation_id,
# MAGIC   n.nation_name,
# MAGIC   n.nation_name_display,
# MAGIC   n.fifa_code,
# MAGIC   n.flag_code,
# MAGIC
# MAGIC   -- Race identity + display
# MAGIC   s.race_id,
# MAGIC   r.race_name,
# MAGIC
# MAGIC   -- Participation within the nation (from summary)
# MAGIC   COALESCE(s.coaches_count, 0)               AS coaches_count,
# MAGIC   COALESCE(s.coach_participations_count, 0)  AS coach_participations_count,
# MAGIC   COALESCE(s.tournaments_attended_count, 0)  AS tournaments_attended_count,
# MAGIC
# MAGIC   -- Share within nation (from summary)
# MAGIC   s.coaches_pct_nation,
# MAGIC   s.coach_participations_pct_nation,
# MAGIC   s.tournaments_attended_pct_nation,
# MAGIC
# MAGIC   -- Performance (from summary)
# MAGIC   s.avg_points_per_game AS points_per_game,
# MAGIC
# MAGIC   s.load_timestamp
# MAGIC FROM naf_catalog.gold_summary.nation_race_summary AS s
# MAGIC LEFT JOIN naf_catalog.gold_dim.nation_dim AS n
# MAGIC   ON s.nation_id = n.nation_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.race_dim AS r
# MAGIC   ON s.race_id = r.race_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql -- VIEW: naf_catalog.gold_presentation.nation_vs_nation_meta
# MAGIC -- =============================================================================
# MAGIC -- PURPOSE:
# MAGIC --   - One row per (nation_id, opponent_nation_id)
# MAGIC --   - Presentation-friendly head-to-head nation matchup metrics
# MAGIC --   - Joins summary measures to nation_dim for UI fields (no string-based joins)
# MAGIC -- GRAIN:
# MAGIC --   - (nation_id, opponent_nation_id)
# MAGIC -- SOURCES:
# MAGIC --   - naf_catalog.gold_summary.nation_vs_nation_summary
# MAGIC --   - naf_catalog.gold_dim.nation_dim
# MAGIC -- =============================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.nation_vs_nation_meta AS
# MAGIC SELECT
# MAGIC   -- Focus nation (keys from summary, UI fields from dim)
# MAGIC   s.nation_id,
# MAGIC   n1.nation_name,
# MAGIC   n1.nation_name_display,
# MAGIC   n1.fifa_code,
# MAGIC   n1.flag_code,
# MAGIC
# MAGIC   -- Opponent nation
# MAGIC   s.opponent_nation_id,
# MAGIC   n2.nation_name         AS opponent_nation_name,
# MAGIC   n2.nation_name_display AS opponent_nation_name_display,
# MAGIC   n2.fifa_code           AS opponent_fifa_code,
# MAGIC   n2.flag_code           AS opponent_flag_code,
# MAGIC
# MAGIC   -- Head-to-head stats (from summary)
# MAGIC   COALESCE(s.games_played, 0) AS games_played,
# MAGIC   COALESCE(s.wins, 0)         AS wins,
# MAGIC   COALESCE(s.draws, 0)        AS draws,
# MAGIC   COALESCE(s.losses, 0)       AS losses,
# MAGIC   s.win_pct,
# MAGIC   s.avg_score_for,
# MAGIC   s.avg_score_against,
# MAGIC
# MAGIC   s.glo_exchange_total,
# MAGIC   s.glo_exchange_mean,
# MAGIC
# MAGIC   s.load_timestamp
# MAGIC FROM naf_catalog.gold_summary.nation_vs_nation_summary AS s
# MAGIC LEFT JOIN naf_catalog.gold_dim.nation_dim AS n1
# MAGIC   ON s.nation_id = n1.nation_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.nation_dim AS n2
# MAGIC   ON s.opponent_nation_id = n2.nation_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql -- VIEW: naf_catalog.gold_presentation.nation_coach_activity_timeseries
# MAGIC -- =============================================================================
# MAGIC -- PURPOSE:
# MAGIC --   Nation participation activity over time (via coach nationality)
# MAGIC -- GRAIN:
# MAGIC --   One row per (nation_id, game_date)
# MAGIC -- SOURCES:
# MAGIC --   naf_catalog.gold_summary.nation_coach_activity_timeseries
# MAGIC --   naf_catalog.gold_dim.nation_dim
# MAGIC -- =============================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.nation_coach_activity_timeseries AS
# MAGIC SELECT
# MAGIC   s.nation_id,
# MAGIC   n.nation_name,
# MAGIC   n.nation_name_display,
# MAGIC   n.fifa_code,
# MAGIC   n.flag_code,
# MAGIC
# MAGIC   s.game_date,
# MAGIC   CAST(date_format(s.game_date, 'yyyyMMdd') AS INT) AS date_id,
# MAGIC
# MAGIC   -- legacy-friendly names for dashboards
# MAGIC   COALESCE(s.coach_participations_count, 0) AS games_participated_count,
# MAGIC   COALESCE(s.game_representations_count, 0) AS games_represented_count,
# MAGIC
# MAGIC   s.load_timestamp
# MAGIC FROM naf_catalog.gold_summary.nation_coach_activity_timeseries AS s
# MAGIC LEFT JOIN naf_catalog.gold_dim.nation_dim AS n
# MAGIC   ON s.nation_id = n.nation_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql -- VIEW: naf_catalog.gold_presentation.nation_glo_peak_distribution
# MAGIC -- =============================================================================
# MAGIC -- PURPOSE:
# MAGIC --   Coach-level GLOBAL (glo) peak distribution, attributed to coach nation
# MAGIC -- GRAIN:
# MAGIC --   One row per (nation_id, coach_id)
# MAGIC -- SOURCES:
# MAGIC --   naf_catalog.gold_summary.nation_coach_glo_metrics
# MAGIC --   naf_catalog.gold_dim.nation_dim
# MAGIC --   naf_catalog.gold_dim.coach_dim
# MAGIC -- =============================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.nation_glo_peak_distribution AS
# MAGIC SELECT
# MAGIC   -- Keys (from summary, to avoid null keys if dim missing)
# MAGIC   m.nation_id,
# MAGIC
# MAGIC   -- Nation display
# MAGIC   n.nation_name,
# MAGIC   n.nation_name_display,
# MAGIC   n.fifa_code,
# MAGIC   n.flag_code,
# MAGIC
# MAGIC   -- Coach identity (for drill / tooltips)
# MAGIC   m.coach_id,
# MAGIC   c.coach_name,
# MAGIC
# MAGIC   -- Distribution value(s)
# MAGIC   m.glo_peak,
# MAGIC   m.glo_mean,
# MAGIC   m.glo_median,
# MAGIC
# MAGIC   -- Sample context
# MAGIC   COALESCE(m.games_played, 0) AS games_played,
# MAGIC   m.is_valid_glo,
# MAGIC
# MAGIC   m.load_timestamp
# MAGIC FROM naf_catalog.gold_summary.nation_coach_glo_metrics AS m
# MAGIC LEFT JOIN naf_catalog.gold_dim.nation_dim AS n
# MAGIC   ON m.nation_id = n.nation_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.coach_dim AS c
# MAGIC   ON m.coach_id = c.coach_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql -- VIEW: naf_catalog.gold_presentation.nation_race_elo_peak_summary
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE:
# MAGIC --   One row per (nation_id, race_id) for dashboard display.
# MAGIC -- SOURCES:
# MAGIC --   naf_catalog.gold_summary.nation_race_elo_peak_summary
# MAGIC --   naf_catalog.gold_dim.nation_dim
# MAGIC --   naf_catalog.gold_dim.race_dim
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.nation_race_elo_peak_summary AS
# MAGIC SELECT
# MAGIC   s.nation_id,
# MAGIC   n.nation_name,
# MAGIC   n.nation_name_display,
# MAGIC   n.fifa_code,
# MAGIC   n.flag_code,
# MAGIC
# MAGIC   s.race_id,
# MAGIC   r.race_name,
# MAGIC
# MAGIC   COALESCE(s.coaches_with_race_count, 0) AS coaches_with_race_count,
# MAGIC   s.elo_peak_mean,
# MAGIC   s.elo_peak_median,
# MAGIC   s.elo_peak_p90,
# MAGIC   s.elo_peak_p10,
# MAGIC
# MAGIC   s.load_timestamp
# MAGIC FROM naf_catalog.gold_summary.nation_race_elo_peak_summary AS s
# MAGIC LEFT JOIN naf_catalog.gold_dim.nation_dim AS n
# MAGIC   ON s.nation_id = n.nation_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.race_dim AS r
# MAGIC   ON s.race_id = r.race_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- VIEW: naf_catalog.gold_presentation.nation_race_strength_comparison
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE:
# MAGIC --   Grouped boxplot data: nation race Elo PEAK vs world race Elo PEAK.
# MAGIC --   Two rows per (nation_id, race_id): one NATION, one WORLD.
# MAGIC --   Dashboard uses this for side-by-side boxplot comparisons per race.
# MAGIC -- GRAIN:
# MAGIC --   One row per (nation_id, race_id, scope)
# MAGIC -- SOURCES:
# MAGIC --   - naf_catalog.gold_summary.nation_race_elo_peak_summary (nation quantiles)
# MAGIC --   - naf_catalog.gold_summary.world_race_elo_quantiles (world quantiles)
# MAGIC --   - naf_catalog.gold_dim.nation_dim, naf_catalog.gold_dim.race_dim
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.nation_race_strength_comparison AS
# MAGIC
# MAGIC -- NATION rows
# MAGIC SELECT
# MAGIC   s.nation_id,
# MAGIC   n.nation_name,
# MAGIC   n.nation_name_display,
# MAGIC   n.flag_code,
# MAGIC   s.race_id,
# MAGIC   r.race_name,
# MAGIC   'NATION'                              AS scope,
# MAGIC   1                                     AS scope_sort,
# MAGIC   s.coaches_with_race_count             AS coaches_count,
# MAGIC   s.elo_peak_min,
# MAGIC   s.elo_peak_p10,
# MAGIC   CAST(NULL AS DOUBLE)                  AS elo_peak_p25,
# MAGIC   s.elo_peak_median                     AS elo_peak_p50,
# MAGIC   CAST(NULL AS DOUBLE)                  AS elo_peak_p75,
# MAGIC   s.elo_peak_p90,
# MAGIC   s.elo_peak_max,
# MAGIC   s.elo_peak_mean,
# MAGIC   s.load_timestamp
# MAGIC FROM naf_catalog.gold_summary.nation_race_elo_peak_summary AS s
# MAGIC LEFT JOIN naf_catalog.gold_dim.nation_dim AS n
# MAGIC   ON s.nation_id = n.nation_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.race_dim AS r
# MAGIC   ON s.race_id = r.race_id
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- WORLD rows (one per race, replicated per nation for easy dashboard joins)
# MAGIC SELECT
# MAGIC   nrs.nation_id,
# MAGIC   nd.nation_name,
# MAGIC   nd.nation_name_display,
# MAGIC   nd.flag_code,
# MAGIC   w.race_id,
# MAGIC   rd.race_name,
# MAGIC   'WORLD'                               AS scope,
# MAGIC   2                                     AS scope_sort,
# MAGIC   w.coaches_count,
# MAGIC   w.elo_peak_min,
# MAGIC   w.elo_peak_p10,
# MAGIC   w.elo_peak_p25,
# MAGIC   w.elo_peak_p50,
# MAGIC   w.elo_peak_p75,
# MAGIC   w.elo_peak_p90,
# MAGIC   w.elo_peak_max,
# MAGIC   CAST(NULL AS DOUBLE)                  AS elo_peak_mean,
# MAGIC   w.load_timestamp
# MAGIC FROM naf_catalog.gold_summary.nation_race_elo_peak_summary AS nrs
# MAGIC INNER JOIN naf_catalog.gold_summary.world_race_elo_quantiles AS w
# MAGIC   ON nrs.race_id = w.race_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.nation_dim AS nd
# MAGIC   ON nrs.nation_id = nd.nation_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.race_dim AS rd
# MAGIC   ON w.race_id = rd.race_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql -- VIEW: naf_catalog.gold_presentation.nation_glo_metric_quantiles
# MAGIC -- =============================================================================
# MAGIC -- PURPOSE: Dashboard contract for nation + world GLO quantiles (boxplot stats)
# MAGIC -- GRAIN  : one row per (nation_id, metric_type) plus World row (nation_id IS NULL)
# MAGIC -- SOURCES:
# MAGIC --   naf_catalog.gold_summary.nation_glo_metric_quantiles
# MAGIC --   naf_catalog.gold_summary.world_glo_metric_quantiles
# MAGIC --   naf_catalog.gold_dim.nation_dim
# MAGIC -- =============================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.nation_glo_metric_quantiles AS
# MAGIC SELECT
# MAGIC   q.nation_id,
# MAGIC   n.nation_name,
# MAGIC   n.nation_name_display,
# MAGIC   q.metric_type,
# MAGIC   q.coaches_count,
# MAGIC   q.value_min,
# MAGIC   q.value_p10,
# MAGIC   q.value_p25,
# MAGIC   q.value_p50,
# MAGIC   q.value_p75,
# MAGIC   q.value_p90,
# MAGIC   q.value_max,
# MAGIC   q.load_timestamp
# MAGIC FROM naf_catalog.gold_summary.nation_glo_metric_quantiles q
# MAGIC LEFT JOIN naf_catalog.gold_dim.nation_dim n
# MAGIC   ON q.nation_id = n.nation_id
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   CAST(NULL AS INT) AS nation_id,
# MAGIC   'World'           AS nation_name,
# MAGIC   'World'           AS nation_name_display,
# MAGIC   w.metric_type,
# MAGIC   w.coaches_count,
# MAGIC   w.value_min,
# MAGIC   w.value_p10,
# MAGIC   w.value_p25,
# MAGIC   w.value_p50,
# MAGIC   w.value_p75,
# MAGIC   w.value_p90,
# MAGIC   w.value_max,
# MAGIC   w.load_timestamp
# MAGIC FROM naf_catalog.gold_summary.world_glo_metric_quantiles w;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.nation_glo_peak_card_long AS
# MAGIC SELECT
# MAGIC   nation_id,
# MAGIC   nation_name,
# MAGIC   nation_name_display AS nation_label,
# MAGIC   metric,
# MAGIC   metric_sort,
# MAGIC   value
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     nation_name,
# MAGIC     nation_name_display,
# MAGIC     'Median Peak GLO' AS metric,
# MAGIC     1 AS metric_sort,
# MAGIC     value_p50 AS value
# MAGIC   FROM naf_catalog.gold_presentation.nation_glo_metric_quantiles
# MAGIC   WHERE metric_type = 'PEAK'
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT nation_id, nation_name, nation_name_display, '25th Percentile GLO', 2, value_p25
# MAGIC   FROM naf_catalog.gold_presentation.nation_glo_metric_quantiles
# MAGIC   WHERE metric_type = 'PEAK'
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT nation_id, nation_name, nation_name_display, '75th Percentile GLO', 3, value_p75
# MAGIC   FROM naf_catalog.gold_presentation.nation_glo_metric_quantiles
# MAGIC   WHERE metric_type = 'PEAK'
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT nation_id, nation_name, nation_name_display, 'IQR (P75-P25)', 4, (value_p75 - value_p25)
# MAGIC   FROM naf_catalog.gold_presentation.nation_glo_metric_quantiles
# MAGIC   WHERE metric_type = 'PEAK'
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT nation_id, nation_name, nation_name_display, 'Number of Coaches', 5, CAST(coaches_count AS DOUBLE)
# MAGIC   FROM naf_catalog.gold_presentation.nation_glo_metric_quantiles
# MAGIC   WHERE metric_type = 'PEAK'
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT nation_id, nation_name, nation_name_display, 'Highest Peak GLO', 6, value_max
# MAGIC   FROM naf_catalog.gold_presentation.nation_glo_metric_quantiles
# MAGIC   WHERE metric_type = 'PEAK'
# MAGIC ) x;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.nation_coach_glo_metrics_long AS
# MAGIC WITH base AS (
# MAGIC   SELECT
# MAGIC     m.nation_id,
# MAGIC     n.nation_name,
# MAGIC     n.nation_name_display,
# MAGIC     m.coach_id,
# MAGIC     c.coach_name,
# MAGIC     m.games_played,
# MAGIC     m.is_valid_glo,
# MAGIC     m.load_timestamp,
# MAGIC     stack(
# MAGIC       3,
# MAGIC       'PEAK',   m.glo_peak,
# MAGIC       'MEAN',   m.glo_mean,
# MAGIC       'MEDIAN', m.glo_median
# MAGIC     ) AS (metric_type, metric_value)
# MAGIC   FROM naf_catalog.gold_summary.nation_coach_glo_metrics m
# MAGIC   LEFT JOIN naf_catalog.gold_dim.nation_dim n
# MAGIC     ON m.nation_id = n.nation_id
# MAGIC   LEFT JOIN naf_catalog.gold_dim.coach_dim c
# MAGIC     ON m.coach_id = c.coach_id
# MAGIC   WHERE m.is_valid_glo = TRUE
# MAGIC ),
# MAGIC long_base AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     nation_name,
# MAGIC     nation_name_display,
# MAGIC     coach_id,
# MAGIC     coach_name,
# MAGIC     metric_type,
# MAGIC     metric_value,
# MAGIC     games_played,
# MAGIC     is_valid_glo,
# MAGIC     load_timestamp
# MAGIC   FROM base
# MAGIC   WHERE metric_value IS NOT NULL
# MAGIC )
# MAGIC SELECT
# MAGIC   nation_id,
# MAGIC   nation_name,
# MAGIC   nation_name_display,
# MAGIC   coach_id,
# MAGIC   coach_name,
# MAGIC   metric_type,
# MAGIC   metric_value,
# MAGIC   games_played,
# MAGIC   is_valid_glo,
# MAGIC   load_timestamp
# MAGIC FROM long_base
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   CAST(NULL AS INT) AS nation_id,
# MAGIC   'World'           AS nation_name,
# MAGIC   'World'           AS nation_name_display,
# MAGIC   coach_id,
# MAGIC   coach_name,
# MAGIC   metric_type,
# MAGIC   metric_value,
# MAGIC   games_played,
# MAGIC   is_valid_glo,
# MAGIC   load_timestamp
# MAGIC FROM long_base;
# MAGIC

# COMMAND ----------

# MAGIC %sql -- VIEW: naf_catalog.gold_presentation.nation_rivalry_meta
# MAGIC -- =============================================================================
# MAGIC -- PURPOSE:
# MAGIC --   Presentation shape for nation rivalries with display attributes for both sides
# MAGIC -- GRAIN:
# MAGIC --   One row per (nation_id, opponent_nation_id)
# MAGIC -- SOURCES:
# MAGIC --   naf_catalog.gold_summary.nation_rivalry_summary
# MAGIC --   naf_catalog.gold_dim.nation_dim (twice)
# MAGIC -- NOTES:
# MAGIC --   - Uses CURRENT_TIMESTAMP() for load_timestamp to avoid dependency on a load column
# MAGIC --     that may not exist on the summary object.
# MAGIC -- =============================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.nation_rivalry_meta AS
# MAGIC SELECT
# MAGIC   s.nation_id,
# MAGIC   n1.nation_name,
# MAGIC   n1.nation_name_display,
# MAGIC   n1.fifa_code,
# MAGIC   n1.flag_code,
# MAGIC
# MAGIC   s.opponent_nation_id,
# MAGIC   n2.nation_name         AS opponent_nation_name,
# MAGIC   n2.nation_name_display AS opponent_nation_name_display,
# MAGIC   n2.fifa_code           AS opponent_fifa_code,
# MAGIC   n2.flag_code           AS opponent_flag_code,
# MAGIC
# MAGIC   s.games_played,
# MAGIC   s.win_pct,
# MAGIC   s.avg_score_for,
# MAGIC   s.avg_score_against,
# MAGIC   s.avg_score_diff,
# MAGIC   s.glo_exchange_total,
# MAGIC
# MAGIC   s.games_score,
# MAGIC   s.closeness_score,
# MAGIC   s.games_share,
# MAGIC   s.rivalry_score,
# MAGIC
# MAGIC   DENSE_RANK() OVER (
# MAGIC     PARTITION BY s.nation_id
# MAGIC     ORDER BY s.rivalry_score DESC, s.games_played DESC, s.opponent_nation_id
# MAGIC   ) AS rivalry_rank,
# MAGIC
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM naf_catalog.gold_summary.nation_rivalry_summary AS s
# MAGIC LEFT JOIN naf_catalog.gold_dim.nation_dim AS n1
# MAGIC   ON s.nation_id = n1.nation_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.nation_dim AS n2
# MAGIC   ON s.opponent_nation_id = n2.nation_id
# MAGIC WHERE s.opponent_nation_id IS NOT NULL
# MAGIC   AND s.nation_id <> s.opponent_nation_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- VIEW: naf_catalog.gold_presentation.nation_profile_long
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE:
# MAGIC --   Profile-card rows for nations (label/value pairs) for dashboards.
# MAGIC -- GRAIN:
# MAGIC --   One row per (nation_id, sort_order)
# MAGIC -- SOURCES:
# MAGIC --   naf_catalog.gold_presentation.nation_overview
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.nation_profile_long AS
# MAGIC WITH src AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     nation_name_display,
# MAGIC     fifa_code,
# MAGIC     flag_code,
# MAGIC     coaches_count,
# MAGIC     games_count,
# MAGIC     tournaments_count,
# MAGIC     glo_peak_mean,
# MAGIC     glo_peak_max,
# MAGIC     glo_peak_median,
# MAGIC     coaches_global_pct,
# MAGIC     games_global_pct,
# MAGIC     tournaments_global_pct,
# MAGIC     load_timestamp,
# MAGIC
# MAGIC     -- Flag emoji from ISO 2-letter flag_code (fallback 🌍)
# MAGIC     CASE
# MAGIC       WHEN flag_code IS NULL OR flag_code = '' OR length(flag_code) <> 2 THEN '🌍'
# MAGIC       ELSE CONCAT(
# MAGIC         CAST(unhex(CONCAT('F09F87', lpad(hex(166 + (ascii(upper(substr(flag_code, 1, 1))) - ascii('A'))), 2, '0'))) AS STRING),
# MAGIC         CAST(unhex(CONCAT('F09F87', lpad(hex(166 + (ascii(upper(substr(flag_code, 2, 1))) - ascii('A'))), 2, '0'))) AS STRING)
# MAGIC       )
# MAGIC     END AS flag_emoji,
# MAGIC
# MAGIC     -- Defensive: handle pct as either 0–1 or 0–100
# MAGIC     CASE
# MAGIC       WHEN coaches_global_pct IS NULL THEN NULL
# MAGIC       WHEN coaches_global_pct > 1.5 THEN coaches_global_pct
# MAGIC       ELSE coaches_global_pct * 100
# MAGIC     END AS coaches_global_pct_0_100,
# MAGIC
# MAGIC     CASE
# MAGIC       WHEN games_global_pct IS NULL THEN NULL
# MAGIC       WHEN games_global_pct > 1.5 THEN games_global_pct
# MAGIC       ELSE games_global_pct * 100
# MAGIC     END AS games_global_pct_0_100,
# MAGIC
# MAGIC     CASE
# MAGIC       WHEN tournaments_global_pct IS NULL THEN NULL
# MAGIC       WHEN tournaments_global_pct > 1.5 THEN tournaments_global_pct
# MAGIC       ELSE tournaments_global_pct * 100
# MAGIC     END AS tournaments_global_pct_0_100
# MAGIC
# MAGIC   FROM naf_catalog.gold_presentation.nation_overview
# MAGIC )
# MAGIC SELECT
# MAGIC   nation_id,
# MAGIC   nation_name_display,
# MAGIC   sort_order,
# MAGIC   label,
# MAGIC   profile,
# MAGIC   load_timestamp
# MAGIC FROM (
# MAGIC   SELECT nation_id, nation_name_display,  10 AS sort_order, '🌐 Nation'         AS label, nation_name_display                      AS profile, load_timestamp FROM src
# MAGIC   UNION ALL SELECT nation_id, nation_name_display,  20, '🏁 FIFA code',          COALESCE(fifa_code, ''),                            load_timestamp FROM src
# MAGIC   UNION ALL SELECT nation_id, nation_name_display,  30, '🏳️ Flag',              flag_emoji,                                         load_timestamp FROM src
# MAGIC
# MAGIC   UNION ALL SELECT nation_id, nation_name_display,  40, '📊 Overview',           '' ,                                                load_timestamp FROM src
# MAGIC   UNION ALL SELECT nation_id, nation_name_display,  41, ' • Coaches in NAF DB',  CAST(coaches_count AS STRING),                      load_timestamp FROM src
# MAGIC   UNION ALL SELECT nation_id, nation_name_display,  42, ' • Game participations',CAST(games_count AS STRING),                        load_timestamp FROM src
# MAGIC   UNION ALL SELECT nation_id, nation_name_display,  43, ' • Tournaments hosted', CAST(tournaments_count AS STRING),                  load_timestamp FROM src
# MAGIC
# MAGIC   UNION ALL SELECT nation_id, nation_name_display,  50, '📈 Peak GLO (coach level)', '',                                             load_timestamp FROM src
# MAGIC   UNION ALL SELECT nation_id, nation_name_display,  51, ' • Best peak GLO (max)', CAST(ROUND(glo_peak_max,    1) AS STRING),        load_timestamp FROM src
# MAGIC   UNION ALL SELECT nation_id, nation_name_display,  52, ' • Avg peak GLO (mean)', CAST(ROUND(glo_peak_mean,   1) AS STRING),        load_timestamp FROM src
# MAGIC   UNION ALL SELECT nation_id, nation_name_display,  53, ' • Median peak GLO',     CAST(ROUND(glo_peak_median, 1) AS STRING),        load_timestamp FROM src
# MAGIC
# MAGIC   UNION ALL SELECT nation_id, nation_name_display,  60, '🌍 Global share',        '',                                                load_timestamp FROM src
# MAGIC   UNION ALL SELECT nation_id, nation_name_display,  61, ' • % of global coaches',    CONCAT(CAST(ROUND(coaches_global_pct_0_100,    2) AS STRING), ' %'), load_timestamp FROM src
# MAGIC   UNION ALL SELECT nation_id, nation_name_display,  62, ' • % of global games',      CONCAT(CAST(ROUND(games_global_pct_0_100,      2) AS STRING), ' %'), load_timestamp FROM src
# MAGIC   UNION ALL SELECT nation_id, nation_name_display,  63, ' • % of global tournaments',CONCAT(CAST(ROUND(tournaments_global_pct_0_100,2) AS STRING), ' %'), load_timestamp FROM src
# MAGIC ) x;
# MAGIC

# COMMAND ----------

# MAGIC %sql -- VIEW: naf_catalog.gold_presentation.nation_top_coach_opponent_bin_perf
# MAGIC -- =============================================================================
# MAGIC -- PURPOSE:
# MAGIC --   Dashboard shape: top coaches (by global GLO peak) with opponent-bin performance
# MAGIC -- GRAIN:
# MAGIC --   One row per (nation_id, coach_id, bin_index)   [bin_index null if no bin rows]
# MAGIC -- SOURCES:
# MAGIC --   naf_catalog.gold_summary.nation_coach_glo_metrics
# MAGIC --   naf_catalog.gold_summary.coach_opponent_glo_bin_summary
# MAGIC --   naf_catalog.gold_dim.nation_dim
# MAGIC --   naf_catalog.gold_dim.coach_dim
# MAGIC -- =============================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.nation_top_coach_opponent_bin_perf AS
# MAGIC WITH params AS (
# MAGIC   SELECT
# MAGIC     CAST(10 AS INT) AS top_n_per_nation
# MAGIC ),
# MAGIC coach_peaks AS (
# MAGIC   SELECT
# MAGIC     m.nation_id,
# MAGIC     m.coach_id,
# MAGIC     CAST(m.glo_peak AS DOUBLE) AS glo_peak
# MAGIC   FROM naf_catalog.gold_summary.nation_coach_glo_metrics AS m
# MAGIC   WHERE m.is_valid_glo = TRUE
# MAGIC     AND m.glo_peak IS NOT NULL
# MAGIC ),
# MAGIC ranked AS (
# MAGIC   SELECT
# MAGIC     p.nation_id,
# MAGIC     p.coach_id,
# MAGIC     p.glo_peak,
# MAGIC     DENSE_RANK() OVER (ORDER BY p.glo_peak DESC, p.coach_id) AS world_rank,
# MAGIC     DENSE_RANK() OVER (PARTITION BY p.nation_id ORDER BY p.glo_peak DESC, p.coach_id) AS nation_rank
# MAGIC   FROM coach_peaks AS p
# MAGIC ),
# MAGIC top_ranked AS (
# MAGIC   SELECT r.*
# MAGIC   FROM ranked r
# MAGIC   CROSS JOIN params p
# MAGIC   WHERE r.nation_rank <= p.top_n_per_nation
# MAGIC )
# MAGIC SELECT
# MAGIC   n.nation_name_display,
# MAGIC   tr.nation_id,
# MAGIC   tr.coach_id,
# MAGIC   c.coach_name,
# MAGIC   tr.glo_peak,
# MAGIC   tr.world_rank,
# MAGIC   tr.nation_rank,
# MAGIC
# MAGIC   b.bin_index,
# MAGIC   b.bin_lower,
# MAGIC   b.bin_upper,
# MAGIC   CASE
# MAGIC     WHEN b.bin_lower IS NOT NULL AND b.bin_upper IS NOT NULL
# MAGIC       THEN CONCAT(CAST(CAST(b.bin_lower AS INT) AS STRING), '–', CAST(CAST(b.bin_upper AS INT) AS STRING))
# MAGIC     ELSE NULL
# MAGIC   END AS bin_label,
# MAGIC
# MAGIC   b.opponents_count,
# MAGIC
# MAGIC   -- NOTE: these columns assume your coach_opponent_glo_bin_summary exposes volume + points.
# MAGIC   -- If your bin table uses wins/draws/losses instead, swap these 3 fields accordingly.
# MAGIC   b.games_played,
# MAGIC   b.points_total,
# MAGIC   b.win_points_per_game,
# MAGIC
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM top_ranked AS tr
# MAGIC LEFT JOIN naf_catalog.gold_summary.coach_opponent_glo_bin_summary AS b
# MAGIC   ON tr.coach_id = b.coach_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.nation_dim AS n
# MAGIC   ON tr.nation_id = n.nation_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.coach_dim AS c
# MAGIC   ON tr.coach_id = c.coach_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- VIEW: naf_catalog.gold_presentation.nation_results_cumulative_display
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE:
# MAGIC --   Dashboard-friendly cumulative W/D/L series for nations.
# MAGIC --   Enriched with nation + opponent nation display attributes.
# MAGIC --   Dashboard can filter by opponent_nation_id for specific matchups
# MAGIC --   or show all rows for REST_OF_WORLD view.
# MAGIC -- GRAIN:
# MAGIC --   One row per (nation_id, game_sequence_number)
# MAGIC -- SOURCES:
# MAGIC --   - naf_catalog.gold_summary.nation_results_cumulative_series
# MAGIC --   - naf_catalog.gold_dim.nation_dim (x2: nation + opponent)
# MAGIC --   - naf_catalog.gold_dim.date_dim
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.nation_results_cumulative_display AS
# MAGIC SELECT
# MAGIC   -- Nation identity
# MAGIC   s.nation_id,
# MAGIC   n.nation_name,
# MAGIC   n.nation_name_display,
# MAGIC   n.flag_code AS nation_flag_code,
# MAGIC
# MAGIC   -- Game-level context
# MAGIC   s.game_sequence_number,
# MAGIC   s.game_id,
# MAGIC   s.game_date,
# MAGIC   s.date_id,
# MAGIC   d.year_month,
# MAGIC   s.event_timestamp,
# MAGIC   s.tournament_id,
# MAGIC   s.result_numeric,
# MAGIC
# MAGIC   -- Opponent nation
# MAGIC   s.opponent_nation_id,
# MAGIC   opp.nation_name           AS opponent_nation_name,
# MAGIC   opp.nation_name_display   AS opponent_nation_name_display,
# MAGIC   opp.flag_code             AS opponent_flag_code,
# MAGIC
# MAGIC   -- Cumulative aggregates
# MAGIC   s.cum_wins,
# MAGIC   s.cum_draws,
# MAGIC   s.cum_losses,
# MAGIC   s.cum_games,
# MAGIC   s.cum_win_frac,
# MAGIC   s.cum_points_frac,
# MAGIC
# MAGIC   s.load_timestamp
# MAGIC FROM naf_catalog.gold_summary.nation_results_cumulative_series AS s
# MAGIC LEFT JOIN naf_catalog.gold_dim.nation_dim AS n
# MAGIC   ON s.nation_id = n.nation_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.nation_dim AS opp
# MAGIC   ON s.opponent_nation_id = opp.nation_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.date_dim AS d
# MAGIC   ON s.date_id = d.date_id;
# MAGIC
