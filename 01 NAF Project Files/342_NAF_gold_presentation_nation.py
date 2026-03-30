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
# MAGIC
# MAGIC ### Overview / comparison
# MAGIC - `nation_overview_comparison` — focus nation vs NATION / REST_OF_WORLD / WORLD
# MAGIC - `nation_overview` — one row per nation; headline KPIs + bucketing
# MAGIC
# MAGIC ### Race / matchup
# MAGIC - `nation_race_meta` — nation × race composition (+ World aggregate)
# MAGIC - `nation_vs_nation_meta` — pairwise nation H2H with display fields
# MAGIC
# MAGIC ### Activity series
# MAGIC - `nation_coach_activity_timeseries` — participation over time
# MAGIC - `nation_members_cumulative_weekly_display` — cumulative member count by week
# MAGIC - `nation_results_cumulative_display` — cumulative W/D/L series
# MAGIC
# MAGIC ### GLO distributions
# MAGIC - `nation_glo_peak_distribution` — coach-level GLO peak scatter
# MAGIC - `nation_glo_binned_distribution_display` — 25-pt GLO histogram
# MAGIC - `nation_glo_smooth_cdf_display` — smooth CDF per nation + World
# MAGIC - `nation_glo_metric_quantiles` — GLO quantile boxplot data (nation + World)
# MAGIC - `nation_glo_peak_card_long` — pivoted GLO card metrics
# MAGIC - `nation_coach_glo_metrics_long` — coach GLO metrics unpivoted (PEAK/MEAN/MEDIAN)
# MAGIC
# MAGIC ### Race Elo
# MAGIC - `nation_race_elo_peak_summary` — nation race Elo peak stats
# MAGIC - `nation_race_strength_comparison` — nation vs world race Elo boxplot
# MAGIC
# MAGIC ### Rivalry / exchange
# MAGIC - `nation_rivalry_meta` — rivalry pairs with display fields
# MAGIC - `nation_rivalry_display` — top 10 rivals merged with H2H
# MAGIC - `nation_glo_exchange_display` — GLO exchange bar chart data
# MAGIC
# MAGIC ### Nation profile
# MAGIC - `nation_profile_long` — rich profile card (label/value pairs)
# MAGIC
# MAGIC ### Opponent binning
# MAGIC - `nation_top_coach_opponent_bin_perf` — top coaches with 4-bin opponent W/D/L
# MAGIC
# MAGIC ### Domestic / international
# MAGIC - `nation_domestic_performance_display` — home/away performance split
# MAGIC - `nation_opponent_elo_bin_wdl_display` — nation W/D/L by opponent rating bin
# MAGIC
# MAGIC ### Team selector / power ranking
# MAGIC - `nation_team_candidates_display` — coach selector scores
# MAGIC - `nation_power_ranking_display` — nation power ranking
# MAGIC
# MAGIC ## Notebook conventions
# MAGIC - **One object per SQL cell** (one `CREATE OR REPLACE VIEW` per cell).
# MAGIC - Put the target object on the **first line** for collapsible navigation, e.g.  
# MAGIC   `%sql -- VIEW: naf_catalog.gold_presentation.nation_overview`
# MAGIC - Keep ID columns explicit (e.g. `opponent_nation_id`, `race_id`), and treat presentation as formatting/orchestration only.
# MAGIC


# COMMAND ----------

# DBTITLE 1,Create schema gold_presentation
# MAGIC %sql -- SCHEMA: naf_catalog.gold_presentation
# MAGIC CREATE SCHEMA IF NOT EXISTS naf_catalog.gold_presentation;

# COMMAND ----------

# DBTITLE 1,Create view gold_presentation.nation_overview_comparison
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
# MAGIC   ROUND(c.coaches_global_frac * 100, 2)              AS coaches_global_pct,
# MAGIC   ROUND(c.coach_participations_global_frac * 100, 2) AS coach_participations_global_pct,
# MAGIC
# MAGIC   c.games_hosted_count,
# MAGIC   c.tournaments_hosted_count,
# MAGIC   ROUND(c.games_hosted_global_frac * 100, 2)       AS games_hosted_global_pct,
# MAGIC   ROUND(c.tournaments_hosted_global_frac * 100, 2) AS tournaments_hosted_global_pct,
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

# DBTITLE 1,Create view gold_presentation.nation_overview
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
# MAGIC   -- Global share (summary stores 0-1 fractions; presentation converts to 0-100 pct)
# MAGIC   ROUND(s.coaches_global_frac * 100, 2)              AS coaches_global_pct,
# MAGIC   ROUND(s.game_representations_global_frac * 100, 2) AS games_global_pct,
# MAGIC   ROUND(s.tournaments_attended_global_frac * 100, 2) AS tournaments_global_pct,
# MAGIC   ROUND(s.coach_participations_global_frac * 100, 2) AS coach_participations_global_pct,
# MAGIC
# MAGIC   -- Hosting (from summary)
# MAGIC   COALESCE(s.games_hosted_count, 0)       AS games_hosted_count,
# MAGIC   COALESCE(s.tournaments_hosted_count, 0) AS tournaments_hosted_count,
# MAGIC   ROUND(s.games_hosted_global_frac * 100, 2)       AS games_hosted_global_pct,
# MAGIC   ROUND(s.tournaments_hosted_global_frac * 100, 2) AS tournaments_hosted_global_pct,
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

# DBTITLE 1,Create view gold_presentation.nation_race_meta
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
# MAGIC WITH nation_rows AS (
# MAGIC   SELECT
# MAGIC     n.nation_id,
# MAGIC     n.nation_name,
# MAGIC     n.nation_name_display,
# MAGIC     n.fifa_code,
# MAGIC     n.flag_code,
# MAGIC     s.race_id,
# MAGIC     r.race_name,
# MAGIC     COALESCE(s.coaches_count, 0)               AS coaches_count,
# MAGIC     COALESCE(s.coach_participations_count, 0)  AS coach_participations_count,
# MAGIC     COALESCE(s.tournaments_attended_count, 0)  AS tournaments_attended_count,
# MAGIC     s.coaches_pct_nation,
# MAGIC     s.coach_participations_pct_nation,
# MAGIC     s.tournaments_attended_pct_nation,
# MAGIC     s.avg_points_per_game AS points_per_game,
# MAGIC     s.load_timestamp
# MAGIC   FROM naf_catalog.gold_summary.nation_race_summary AS s
# MAGIC   LEFT JOIN naf_catalog.gold_dim.nation_dim AS n ON s.nation_id = n.nation_id
# MAGIC   LEFT JOIN naf_catalog.gold_dim.race_dim AS r   ON s.race_id = r.race_id
# MAGIC ),
# MAGIC -- World aggregate: all nations combined, computed in-line
# MAGIC -- total_coaches from nation_overview (unique per nation, not per race)
# MAGIC -- total_participations/tournaments from nation_rows (non-overlapping across races)
# MAGIC world_totals AS (
# MAGIC   SELECT
# MAGIC     (SELECT SUM(coaches_count) FROM naf_catalog.gold_presentation.nation_overview
# MAGIC      WHERE nation_id <> 0) AS total_coaches,
# MAGIC     SUM(coach_participations_count)  AS total_participations,
# MAGIC     SUM(tournaments_attended_count)  AS total_tournaments
# MAGIC   FROM nation_rows
# MAGIC ),
# MAGIC world_rows AS (
# MAGIC   SELECT
# MAGIC     -1                         AS nation_id,
# MAGIC     'World'                    AS nation_name,
# MAGIC     'World'                    AS nation_name_display,
# MAGIC     CAST(NULL AS STRING)       AS fifa_code,
# MAGIC     CAST(NULL AS STRING)       AS flag_code,
# MAGIC     nr.race_id,
# MAGIC     nr.race_name,
# MAGIC     CAST(SUM(nr.coaches_count) AS INT)               AS coaches_count,
# MAGIC     CAST(SUM(nr.coach_participations_count) AS INT)  AS coach_participations_count,
# MAGIC     CAST(SUM(nr.tournaments_attended_count) AS INT)  AS tournaments_attended_count,
# MAGIC     ROUND(100.0 * SUM(nr.coaches_count)               / NULLIF(wt.total_coaches, 0), 2)        AS coaches_pct_nation,
# MAGIC     ROUND(100.0 * SUM(nr.coach_participations_count)  / NULLIF(wt.total_participations, 0), 2) AS coach_participations_pct_nation,
# MAGIC     ROUND(100.0 * SUM(nr.tournaments_attended_count)  / NULLIF(wt.total_tournaments, 0), 2)    AS tournaments_attended_pct_nation,
# MAGIC     AVG(nr.points_per_game)    AS points_per_game,
# MAGIC     MAX(nr.load_timestamp)     AS load_timestamp
# MAGIC   FROM nation_rows AS nr
# MAGIC   CROSS JOIN world_totals AS wt
# MAGIC   GROUP BY nr.race_id, nr.race_name, wt.total_coaches, wt.total_participations, wt.total_tournaments
# MAGIC )
# MAGIC SELECT nation_id, nation_name, nation_name_display, fifa_code, flag_code,
# MAGIC        race_id, race_name, coaches_count, coach_participations_count,
# MAGIC        tournaments_attended_count, coaches_pct_nation,
# MAGIC        coach_participations_pct_nation, tournaments_attended_pct_nation,
# MAGIC        points_per_game, load_timestamp
# MAGIC FROM nation_rows
# MAGIC UNION ALL
# MAGIC SELECT nation_id, nation_name, nation_name_display, fifa_code, flag_code,
# MAGIC        race_id, race_name, coaches_count, coach_participations_count,
# MAGIC        tournaments_attended_count, coaches_pct_nation,
# MAGIC        coach_participations_pct_nation, tournaments_attended_pct_nation,
# MAGIC        points_per_game, load_timestamp
# MAGIC FROM world_rows;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create view gold_presentation.nation_vs_nation_meta
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
# MAGIC   ROUND((COALESCE(s.wins, 0) + 0.5 * COALESCE(s.draws, 0)) / NULLIF(s.games_played, 0), 3) AS ppg,
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

# DBTITLE 1,Create view gold_presentation.nation_coach_activity_timeseries
# MAGIC %sql -- VIEW: naf_catalog.gold_presentation.nation_coach_activity_timeseries
# MAGIC -- =============================================================================
# MAGIC -- PURPOSE:
# MAGIC --   Nation participation activity over time (via coach nationality)
# MAGIC -- GRAIN:
# MAGIC --   One row per (nation_id, date_id)
# MAGIC -- SOURCES:
# MAGIC --   naf_catalog.gold_summary.nation_coach_activity_timeseries
# MAGIC --   naf_catalog.gold_dim.nation_dim
# MAGIC --   naf_catalog.gold_dim.date_dim
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
# MAGIC   s.date_id,
# MAGIC   d.date AS game_date,
# MAGIC
# MAGIC   -- legacy-friendly names for dashboards
# MAGIC   COALESCE(s.coach_participations_count, 0) AS games_participated_count,
# MAGIC   COALESCE(s.game_representations_count, 0) AS games_represented_count,
# MAGIC
# MAGIC   s.load_timestamp
# MAGIC FROM naf_catalog.gold_summary.nation_coach_activity_timeseries AS s
# MAGIC LEFT JOIN naf_catalog.gold_dim.nation_dim AS n
# MAGIC   ON s.nation_id = n.nation_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.date_dim AS d
# MAGIC   ON s.date_id = d.date_id;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create view gold_presentation.nation_members_cumulative_weekly_display
# MAGIC %sql -- VIEW: naf_catalog.gold_presentation.nation_members_cumulative_weekly_display
# MAGIC -- =============================================================================
# MAGIC -- PURPOSE:
# MAGIC --   Dashboard-ready weekly cumulative NAF member count per nation + World.
# MAGIC --   Joins nation_dim for display names. World row uses nation_id = -1.
# MAGIC -- GRAIN:
# MAGIC --   One row per (nation_id, iso_week)
# MAGIC -- SOURCES:
# MAGIC --   naf_catalog.gold_summary.nation_members_cumulative_weekly
# MAGIC --   naf_catalog.gold_dim.nation_dim
# MAGIC -- FEEDS: W2 — NAF Members Over Time (Nation Dashboard)
# MAGIC -- =============================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.nation_members_cumulative_weekly_display AS
# MAGIC SELECT
# MAGIC   s.nation_id,
# MAGIC   CASE WHEN s.nation_id = -1 THEN 'World'
# MAGIC        ELSE n.nation_name_display
# MAGIC   END                                     AS nation_name_display,
# MAGIC   s.iso_week,
# MAGIC   s.new_coaches,
# MAGIC   s.cumulative_coaches,
# MAGIC   s.load_timestamp
# MAGIC FROM naf_catalog.gold_summary.nation_members_cumulative_weekly AS s
# MAGIC LEFT JOIN naf_catalog.gold_dim.nation_dim AS n
# MAGIC   ON s.nation_id = n.nation_id;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create view gold_presentation.nation_glo_peak_distribution
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

# DBTITLE 1,Create view gold_presentation.nation_glo_binned_distribution_display
# MAGIC %sql -- VIEW: naf_catalog.gold_presentation.nation_glo_binned_distribution_display
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Dashboard-facing GLO histogram data with nation display names.
# MAGIC --                Supports PEAK and MEDIAN metric types. Includes World (nation_id=-1).
# MAGIC -- GRAIN        : 1 row per (nation_name_display, metric_type, glo_bin)
# MAGIC -- SOURCES      : naf_catalog.gold_summary.nation_glo_binned_distribution
# MAGIC --                naf_catalog.gold_dim.nation_dim
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.nation_glo_binned_distribution_display AS
# MAGIC SELECT
# MAGIC   merged.nation_id,
# MAGIC   CASE WHEN merged.nation_id = -1 THEN 'World'
# MAGIC        ELSE n.nation_name_display
# MAGIC   END AS nation_name_display,
# MAGIC   merged.metric_type,
# MAGIC   merged.glo_bin,
# MAGIC   merged.glo_bin_label,
# MAGIC   merged.coach_count,
# MAGIC   merged.total_coaches,
# MAGIC   ROUND(merged.density * 100, 2) AS density_pct,
# MAGIC   ROUND(merged.cumulative_density * 100, 2) AS cumulative_density_pct,
# MAGIC   merged.load_timestamp
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     d.nation_id,
# MAGIC     d.metric_type,
# MAGIC     CASE WHEN d.glo_bin >= 300 THEN 300 ELSE d.glo_bin END AS glo_bin,
# MAGIC     CASE WHEN d.glo_bin >= 300 THEN '300+'
# MAGIC          ELSE CONCAT(CAST(d.glo_bin AS STRING), '–', CAST(d.glo_bin + 25 AS STRING))
# MAGIC     END AS glo_bin_label,
# MAGIC     SUM(d.coach_count) AS coach_count,
# MAGIC     MAX(d.total_coaches) AS total_coaches,
# MAGIC     SUM(d.density) AS density,
# MAGIC     MAX(d.cumulative_density) AS cumulative_density,
# MAGIC     MAX(d.load_timestamp) AS load_timestamp
# MAGIC   FROM naf_catalog.gold_summary.nation_glo_binned_distribution AS d
# MAGIC   GROUP BY d.nation_id, d.metric_type,
# MAGIC     CASE WHEN d.glo_bin >= 300 THEN 300 ELSE d.glo_bin END,
# MAGIC     CASE WHEN d.glo_bin >= 300 THEN '300+'
# MAGIC          ELSE CONCAT(CAST(d.glo_bin AS STRING), '–', CAST(d.glo_bin + 25 AS STRING))
# MAGIC     END
# MAGIC ) merged
# MAGIC LEFT JOIN naf_catalog.gold_dim.nation_dim AS n
# MAGIC   ON merged.nation_id = n.nation_id;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create view gold_presentation.nation_glo_smooth_cdf_display
# MAGIC %sql
# MAGIC -- VIEW: naf_catalog.gold_presentation.nation_glo_smooth_cdf_display
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Smooth cumulative distribution function for GLO values.
# MAGIC --                Computes CUME_DIST at each individual coach GLO value
# MAGIC --                instead of using pre-binned histogram data.
# MAGIC --                Supports PEAK and MEDIAN metric types. Includes World (nation_id=-1).
# MAGIC -- GRAIN        : 1 row per (nation_id, metric_type, glo_value) — one point per coach
# MAGIC -- SOURCES      : naf_catalog.gold_summary.nation_coach_glo_metrics
# MAGIC --                naf_catalog.gold_dim.nation_dim
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.nation_glo_smooth_cdf_display AS
# MAGIC WITH raw AS (
# MAGIC   -- Per-nation CDF
# MAGIC   SELECT
# MAGIC     g.nation_id,
# MAGIC     n.nation_name_display,
# MAGIC     'PEAK' AS metric_type,
# MAGIC     ROUND(g.glo_peak, 0)   AS glo_value,
# MAGIC     CUME_DIST() OVER (PARTITION BY g.nation_id ORDER BY g.glo_peak) AS cume_pct
# MAGIC   FROM naf_catalog.gold_summary.nation_coach_glo_metrics AS g
# MAGIC   LEFT JOIN naf_catalog.gold_dim.nation_dim AS n ON g.nation_id = n.nation_id
# MAGIC   WHERE g.is_valid_glo = TRUE
# MAGIC     AND g.nation_id <> 0  -- exclude Unknown from per-nation lines
# MAGIC
# MAGIC   UNION ALL
# MAGIC
# MAGIC   SELECT
# MAGIC     g.nation_id,
# MAGIC     n.nation_name_display,
# MAGIC     'MEDIAN' AS metric_type,
# MAGIC     ROUND(g.glo_median, 0) AS glo_value,
# MAGIC     CUME_DIST() OVER (PARTITION BY g.nation_id ORDER BY g.glo_median) AS cume_pct
# MAGIC   FROM naf_catalog.gold_summary.nation_coach_glo_metrics AS g
# MAGIC   LEFT JOIN naf_catalog.gold_dim.nation_dim AS n ON g.nation_id = n.nation_id
# MAGIC   WHERE g.is_valid_glo = TRUE
# MAGIC     AND g.nation_id <> 0  -- exclude Unknown from per-nation lines
# MAGIC
# MAGIC   UNION ALL
# MAGIC
# MAGIC   -- World CDF (all valid coaches regardless of nation)
# MAGIC   SELECT
# MAGIC     -1 AS nation_id,
# MAGIC     'World' AS nation_name_display,
# MAGIC     'PEAK' AS metric_type,
# MAGIC     ROUND(g.glo_peak, 0)   AS glo_value,
# MAGIC     CUME_DIST() OVER (ORDER BY g.glo_peak) AS cume_pct
# MAGIC   FROM naf_catalog.gold_summary.nation_coach_glo_metrics AS g
# MAGIC   WHERE g.is_valid_glo = TRUE
# MAGIC
# MAGIC   UNION ALL
# MAGIC
# MAGIC   SELECT
# MAGIC     -1 AS nation_id,
# MAGIC     'World' AS nation_name_display,
# MAGIC     'MEDIAN' AS metric_type,
# MAGIC     ROUND(g.glo_median, 0) AS glo_value,
# MAGIC     CUME_DIST() OVER (ORDER BY g.glo_median) AS cume_pct
# MAGIC   FROM naf_catalog.gold_summary.nation_coach_glo_metrics AS g
# MAGIC   WHERE g.is_valid_glo = TRUE
# MAGIC ),
# MAGIC -- Aggregate by rounded glo_value to guarantee monotonicity and smooth the curve
# MAGIC deduped AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     nation_name_display,
# MAGIC     metric_type,
# MAGIC     glo_value,
# MAGIC     MAX(cume_pct) AS cume_pct
# MAGIC   FROM raw
# MAGIC   GROUP BY nation_id, nation_name_display, metric_type, glo_value
# MAGIC )
# MAGIC SELECT
# MAGIC   nation_id,
# MAGIC   nation_name_display,
# MAGIC   metric_type,
# MAGIC   glo_value,
# MAGIC   ROUND(cume_pct * 100, 2) AS cumulative_pct
# MAGIC FROM deduped;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create view gold_presentation.nation_race_elo_peak_summary
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

# DBTITLE 1,Create view gold_presentation.nation_race_strength_comparison
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

# DBTITLE 1,Create view gold_presentation.nation_glo_metric_quantiles
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
# MAGIC   CAST(-1 AS INT) AS nation_id,
# MAGIC   'World'          AS nation_name,
# MAGIC   'World'          AS nation_name_display,
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

# DBTITLE 1,Create view gold_presentation.nation_glo_peak_card_long
# MAGIC %sql -- VIEW: naf_catalog.gold_presentation.nation_glo_peak_card_long
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.nation_glo_peak_card_long AS
# MAGIC SELECT
# MAGIC   nation_id,
# MAGIC   nation_name,
# MAGIC   nation_name_display,
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

# DBTITLE 1,Create view gold_presentation.nation_coach_glo_metrics_long
# MAGIC %sql -- VIEW: naf_catalog.gold_presentation.nation_coach_glo_metrics_long
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
# MAGIC   CAST(-1 AS INT) AS nation_id,
# MAGIC   'World'          AS nation_name,
# MAGIC   'World'          AS nation_name_display,
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

# DBTITLE 1,Create view gold_presentation.nation_rivalry_meta
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
# MAGIC   s.avg_score_for,
# MAGIC   s.ppg_closeness,
# MAGIC   s.games_rank,
# MAGIC   s.closeness_rank,
# MAGIC   s.rivalry_score,
# MAGIC
# MAGIC   DENSE_RANK() OVER (
# MAGIC     PARTITION BY s.nation_id
# MAGIC     ORDER BY s.rivalry_score ASC, s.games_played DESC, s.opponent_nation_id
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

# DBTITLE 1,Create view gold_presentation.nation_rivalry_display
# MAGIC %sql
# MAGIC -- VIEW: naf_catalog.gold_presentation.nation_rivalry_display
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE:
# MAGIC --   Merged rivalry view for W12 (Nation Dashboard).
# MAGIC --   Combines nation_vs_nation records + rivalry scores into one table.
# MAGIC --   Columns: opponent nation, games, W/D/L (absolute), PPG, rivalry score.
# MAGIC -- GRAIN:
# MAGIC --   One row per (nation_id, opponent_nation_id)
# MAGIC -- SOURCES:
# MAGIC --   naf_catalog.gold_summary.nation_vs_nation_summary
# MAGIC --   naf_catalog.gold_summary.nation_rivalry_summary
# MAGIC --   naf_catalog.gold_presentation.nation_identity_v
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.nation_rivalry_display AS
# MAGIC SELECT
# MAGIC   s.nation_id,
# MAGIC   ni.nation_name_display,
# MAGIC   ni.flag_emoji,
# MAGIC   s.opponent_nation_id,
# MAGIC   oni.nation_name_display AS opponent_nation_name_display,
# MAGIC   oni.flag_emoji AS opponent_flag_emoji,
# MAGIC   CAST(s.games_played AS INT) AS games,
# MAGIC   CAST(s.wins AS INT) AS wins,
# MAGIC   CAST(s.draws AS INT) AS draws,
# MAGIC   CAST(s.losses AS INT) AS losses,
# MAGIC   ROUND(s.avg_score_for, 3) AS ppg,
# MAGIC   ROUND(s.glo_exchange_mean, 2) AS avg_glo_exchange,
# MAGIC   ROUND(s.glo_exchange_total, 1) AS total_glo_exchange,
# MAGIC   r.games_rank,
# MAGIC   r.closeness_rank,
# MAGIC   r.rivalry_score,
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM naf_catalog.gold_summary.nation_vs_nation_summary AS s
# MAGIC INNER JOIN naf_catalog.gold_summary.nation_rivalry_summary AS r
# MAGIC   ON s.nation_id = r.nation_id AND s.opponent_nation_id = r.opponent_nation_id
# MAGIC LEFT JOIN naf_catalog.gold_presentation.nation_identity_v AS ni
# MAGIC   ON s.nation_id = ni.nation_id
# MAGIC LEFT JOIN naf_catalog.gold_presentation.nation_identity_v AS oni
# MAGIC   ON s.opponent_nation_id = oni.nation_id
# MAGIC QUALIFY DENSE_RANK() OVER (PARTITION BY s.nation_id ORDER BY r.rivalry_score ASC) <= 10;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create view gold_presentation.nation_glo_exchange_display
# MAGIC %sql
# MAGIC -- VIEW: naf_catalog.gold_presentation.nation_glo_exchange_display
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE:
# MAGIC --   GLO exchange data for ALL opponent nations (not limited to top 10 rivals).
# MAGIC --   Feeds W11 (GLO Exchange bar chart).
# MAGIC -- GRAIN:
# MAGIC --   One row per (nation_id, opponent_nation_id)
# MAGIC -- SOURCES:
# MAGIC --   naf_catalog.gold_summary.nation_vs_nation_summary
# MAGIC --   naf_catalog.gold_presentation.nation_identity_v
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.nation_glo_exchange_display AS
# MAGIC SELECT
# MAGIC   s.nation_id,
# MAGIC   ni.nation_name_display,
# MAGIC   oni.nation_name_display AS opponent_nation_name_display,
# MAGIC   ROUND(s.glo_exchange_mean, 2)  AS avg_glo_exchange,
# MAGIC   ROUND(s.glo_exchange_total, 1) AS total_glo_exchange,
# MAGIC   CAST(s.games_played AS INT)    AS games
# MAGIC FROM naf_catalog.gold_summary.nation_vs_nation_summary AS s
# MAGIC LEFT JOIN naf_catalog.gold_presentation.nation_identity_v AS ni
# MAGIC   ON s.nation_id = ni.nation_id
# MAGIC LEFT JOIN naf_catalog.gold_presentation.nation_identity_v AS oni
# MAGIC   ON s.opponent_nation_id = oni.nation_id
# MAGIC WHERE s.nation_id <> 0
# MAGIC   AND s.opponent_nation_id <> 0
# MAGIC   AND s.nation_id <> s.opponent_nation_id;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create view gold_presentation.nation_profile_long
# MAGIC %sql
# MAGIC -- VIEW: naf_catalog.gold_presentation.nation_profile_long
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE:
# MAGIC --   Rich profile-card rows for nations (label/value pairs) for dashboards.
# MAGIC --   Six categories: Identity, Overview, Ratings, Race Profile, Team Strength, Head-to-Head.
# MAGIC --   Most numeric stats include a global nation rank as "(#N)".
# MAGIC -- GRAIN:
# MAGIC --   One row per (nation_id, sort_order)
# MAGIC -- SOURCES:
# MAGIC --   nation_overview, nation_identity_v, nation_glo_metric_quantiles,
# MAGIC --   nation_power_ranking, nation_race_summary, nation_vs_nation_summary,
# MAGIC --   nation_members_cumulative_weekly, nation_team_candidate_scores,
# MAGIC --   nation_active_coaches_summary, nation_rivalry_summary, nation_domestic_summary
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.nation_profile_long AS
# MAGIC
# MAGIC WITH -- 1. Base overview data with rankings
# MAGIC overview AS (
# MAGIC   SELECT
# MAGIC     o.nation_id,
# MAGIC     o.nation_name_display,
# MAGIC     COALESCE(ni.flag_emoji, '') AS flag_emoji,
# MAGIC     o.coaches_count,
# MAGIC     o.games_count,
# MAGIC     o.tournaments_hosted_count,
# MAGIC     o.points_per_game,
# MAGIC     o.glo_peak_max,
# MAGIC     o.glo_peak_median,
# MAGIC     o.load_timestamp,
# MAGIC     DENSE_RANK() OVER (ORDER BY o.coaches_count DESC)           AS rank_coaches,
# MAGIC     DENSE_RANK() OVER (ORDER BY o.games_count DESC)             AS rank_games,
# MAGIC     DENSE_RANK() OVER (ORDER BY o.tournaments_hosted_count DESC) AS rank_hosted
# MAGIC   FROM naf_catalog.gold_presentation.nation_overview AS o
# MAGIC   LEFT JOIN naf_catalog.gold_presentation.nation_identity_v AS ni
# MAGIC     ON o.nation_id = ni.nation_id
# MAGIC   WHERE o.coaches_count > 0
# MAGIC ),
# MAGIC
# MAGIC -- 2. Active coaches (from pre-computed summary; rolling 2-year window)
# MAGIC active_ranked AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     active_coaches,
# MAGIC     DENSE_RANK() OVER (ORDER BY active_coaches DESC) AS rank_active
# MAGIC   FROM naf_catalog.gold_summary.nation_active_coaches_summary
# MAGIC ),
# MAGIC
# MAGIC -- 3. Total W/D/L from nation_vs_nation_summary (international games)
# MAGIC wdl AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     CAST(SUM(wins)   AS INT) AS total_wins,
# MAGIC     CAST(SUM(draws)  AS INT) AS total_draws,
# MAGIC     CAST(SUM(losses) AS INT) AS total_losses
# MAGIC   FROM naf_catalog.gold_summary.nation_vs_nation_summary
# MAGIC   WHERE nation_id <> 0
# MAGIC   GROUP BY nation_id
# MAGIC ),
# MAGIC
# MAGIC -- 4a. GLO ratings (PEAK metric) with P90 and rankings
# MAGIC glo_peak AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     value_p50 AS glo_median_peak,
# MAGIC     value_p90 AS glo_p90,
# MAGIC     value_max AS glo_peak_best,
# MAGIC     DENSE_RANK() OVER (ORDER BY value_p50 DESC) AS rank_median_peak,
# MAGIC     DENSE_RANK() OVER (ORDER BY value_p90 DESC) AS rank_p90,
# MAGIC     DENSE_RANK() OVER (ORDER BY value_max DESC) AS rank_peak
# MAGIC   FROM naf_catalog.gold_summary.nation_glo_metric_quantiles
# MAGIC   WHERE metric_type = 'PEAK'
# MAGIC     AND nation_id <> 0
# MAGIC ),
# MAGIC
# MAGIC -- 4b. GLO ratings (MEDIAN metric = career) with P90 and rankings
# MAGIC glo_career AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     value_p50 AS glo_median_career,
# MAGIC     value_p90 AS glo_p90_career,
# MAGIC     DENSE_RANK() OVER (ORDER BY value_p50 DESC) AS rank_median_career,
# MAGIC     DENSE_RANK() OVER (ORDER BY value_p90 DESC) AS rank_p90_career
# MAGIC   FROM naf_catalog.gold_summary.nation_glo_metric_quantiles
# MAGIC   WHERE metric_type = 'MEDIAN'
# MAGIC     AND nation_id <> 0
# MAGIC ),
# MAGIC
# MAGIC -- 5. Power ranking with coaches_eligible ranking
# MAGIC power AS (
# MAGIC   SELECT nation_id, power_rank, top_8_avg_selector_score, coaches_eligible,
# MAGIC     DENSE_RANK() OVER (ORDER BY coaches_eligible DESC) AS rank_eligible
# MAGIC   FROM naf_catalog.gold_summary.nation_power_ranking
# MAGIC ),
# MAGIC
# MAGIC -- 6. Top-8 averages for team strength (global selector score)
# MAGIC team_str AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     ROUND(AVG(glo_peak), 1)   AS top8_avg_glo_peak,
# MAGIC     ROUND(AVG(glo_median), 1) AS top8_avg_glo_median,
# MAGIC     ROUND(AVG(selector_score), 1) AS top8_avg_selector_global
# MAGIC   FROM naf_catalog.gold_summary.nation_team_candidate_scores
# MAGIC   WHERE selector_rank_national <= 8
# MAGIC   GROUP BY nation_id
# MAGIC ),
# MAGIC team_str_ranked AS (
# MAGIC   SELECT *,
# MAGIC     DENSE_RANK() OVER (ORDER BY top8_avg_selector_global ASC) AS rank_team_score,
# MAGIC     DENSE_RANK() OVER (ORDER BY top8_avg_glo_peak DESC) AS rank_team_peak,
# MAGIC     DENSE_RANK() OVER (ORDER BY top8_avg_glo_median DESC) AS rank_team_median
# MAGIC   FROM team_str
# MAGIC ),
# MAGIC
# MAGIC -- 7a. Race diversity index (normalised Shannon entropy, 0–1)
# MAGIC race_diversity AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     -- Shannon entropy: -SUM(p * ln(p)), normalised by ln(N) where N = number of races played
# MAGIC     -- Uses coach_participations_pct_nation (sums to 100%) not coaches_pct_nation (overlapping)
# MAGIC     CASE WHEN COUNT(*) <= 1 THEN 0.0
# MAGIC     ELSE ROUND(
# MAGIC       -SUM( (coach_participations_pct_nation / 100.0) * LN(coach_participations_pct_nation / 100.0) )
# MAGIC       / LN(COUNT(*)),
# MAGIC     3) END AS diversity_index
# MAGIC   FROM naf_catalog.gold_summary.nation_race_summary
# MAGIC   WHERE coach_participations_pct_nation > 0
# MAGIC     AND nation_id <> 0
# MAGIC   GROUP BY nation_id
# MAGIC ),
# MAGIC race_diversity_ranked AS (
# MAGIC   SELECT *, DENSE_RANK() OVER (ORDER BY diversity_index DESC) AS rank_diversity
# MAGIC   FROM race_diversity
# MAGIC ),
# MAGIC
# MAGIC -- 7b. International play stats
# MAGIC intl_play AS (
# MAGIC   SELECT
# MAGIC     s.nation_id,
# MAGIC     ROUND(100.0 * s.games_away / NULLIF(s.games_home + s.games_away, 0), 1) AS pct_abroad,
# MAGIC     ROUND(100.0 * s.games_vs_foreign / NULLIF(s.games_home + s.games_away, 0), 1) AS pct_vs_foreign,
# MAGIC     ROUND(s.win_frac_vs_foreign, 3) AS ppg_vs_foreign
# MAGIC   FROM naf_catalog.gold_summary.nation_domestic_summary AS s
# MAGIC ),
# MAGIC
# MAGIC -- 7c. Top 5 races (by participation share)
# MAGIC top_races AS (
# MAGIC   SELECT
# MAGIC     s.nation_id,
# MAGIC     r.race_name,
# MAGIC     ROUND(s.coach_participations_pct_nation, 1) AS participations_pct,
# MAGIC     ROUND(s.coaches_pct_nation, 1)              AS coaches_pct,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY s.nation_id ORDER BY s.coach_participations_pct_nation DESC) AS race_rank
# MAGIC   FROM naf_catalog.gold_summary.nation_race_summary AS s
# MAGIC   INNER JOIN naf_catalog.gold_dim.race_dim AS r ON s.race_id = r.race_id
# MAGIC   WHERE s.nation_id <> 0
# MAGIC   QUALIFY race_rank <= 5
# MAGIC ),
# MAGIC
# MAGIC -- 7d. Top 3 rivals (from nation_rivalry_summary, ranked by rivalry_score ASC = strongest rival)
# MAGIC top_rivals AS (
# MAGIC   SELECT
# MAGIC     r.nation_id,
# MAGIC     n.nation_name_display AS rival_name,
# MAGIC     r.games_played AS rival_games,
# MAGIC     DENSE_RANK() OVER (PARTITION BY r.nation_id ORDER BY r.rivalry_score ASC) AS rival_rank
# MAGIC   FROM naf_catalog.gold_summary.nation_rivalry_summary AS r
# MAGIC   INNER JOIN naf_catalog.gold_dim.nation_dim AS n ON r.opponent_nation_id = n.nation_id
# MAGIC   QUALIFY rival_rank <= 3
# MAGIC ),
# MAGIC
# MAGIC -- 8. Head-to-head: most played, best PPG, worst PPG opponent
# MAGIC h2h_most AS (
# MAGIC   SELECT nation_id, opponent_nation_id, games_played
# MAGIC   FROM naf_catalog.gold_summary.nation_vs_nation_summary
# MAGIC   WHERE nation_id <> 0 AND opponent_nation_id <> 0
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY nation_id ORDER BY games_played DESC) = 1
# MAGIC ),
# MAGIC h2h_best AS (
# MAGIC   SELECT nation_id, opponent_nation_id, avg_score_for AS ppg
# MAGIC   FROM naf_catalog.gold_summary.nation_vs_nation_summary
# MAGIC   WHERE nation_id <> 0 AND opponent_nation_id <> 0 AND games_played >= 10
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY nation_id ORDER BY avg_score_for DESC) = 1
# MAGIC ),
# MAGIC h2h_worst AS (
# MAGIC   SELECT nation_id, opponent_nation_id, avg_score_for AS ppg
# MAGIC   FROM naf_catalog.gold_summary.nation_vs_nation_summary
# MAGIC   WHERE nation_id <> 0 AND opponent_nation_id <> 0 AND games_played >= 10
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY nation_id ORDER BY avg_score_for ASC) = 1
# MAGIC ),
# MAGIC
# MAGIC -- 9. Resolve opponent nation names
# MAGIC h2h_joined AS (
# MAGIC   SELECT
# MAGIC     o.nation_id,
# MAGIC     CONCAT(n1.nation_name_display, ' (', CAST(hm.games_played AS STRING), ' games)') AS most_played_opp,
# MAGIC     CONCAT(n2.nation_name_display, ' (', CAST(ROUND(hb.ppg, 3) AS STRING), ' PPG)') AS best_record_opp,
# MAGIC     CONCAT(n3.nation_name_display, ' (', CAST(ROUND(hw.ppg, 3) AS STRING), ' PPG)') AS worst_record_opp
# MAGIC   FROM overview o
# MAGIC   LEFT JOIN h2h_most  hm ON o.nation_id = hm.nation_id
# MAGIC   LEFT JOIN h2h_best  hb ON o.nation_id = hb.nation_id
# MAGIC   LEFT JOIN h2h_worst hw ON o.nation_id = hw.nation_id
# MAGIC   LEFT JOIN naf_catalog.gold_dim.nation_dim n1 ON hm.opponent_nation_id = n1.nation_id
# MAGIC   LEFT JOIN naf_catalog.gold_dim.nation_dim n2 ON hb.opponent_nation_id = n2.nation_id
# MAGIC   LEFT JOIN naf_catalog.gold_dim.nation_dim n3 ON hw.opponent_nation_id = n3.nation_id
# MAGIC ),
# MAGIC
# MAGIC -- 10. Combine into a single source per nation
# MAGIC combined AS (
# MAGIC   SELECT
# MAGIC     o.nation_id,
# MAGIC     o.nation_name_display,
# MAGIC     o.flag_emoji,
# MAGIC     o.coaches_count, o.rank_coaches,
# MAGIC     COALESCE(ar.active_coaches, 0) AS active_coaches, COALESCE(ar.rank_active, 999) AS rank_active,
# MAGIC     o.games_count, o.rank_games,
# MAGIC     o.tournaments_hosted_count, o.rank_hosted,
# MAGIC     COALESCE(w.total_wins, 0) AS wins, COALESCE(w.total_draws, 0) AS draws, COALESCE(w.total_losses, 0) AS losses,
# MAGIC     gp.glo_median_peak, gp.glo_peak_best, gp.glo_p90,
# MAGIC     COALESCE(gp.rank_median_peak, 999) AS rank_glo_median_peak,
# MAGIC     COALESCE(gp.rank_peak, 999)   AS rank_glo_peak,
# MAGIC     COALESCE(gp.rank_p90, 999)    AS rank_glo_p90,
# MAGIC     gc.glo_median_career,
# MAGIC     gc.glo_p90_career,
# MAGIC     COALESCE(gc.rank_median_career, 999) AS rank_glo_median_career,
# MAGIC     COALESCE(gc.rank_p90_career, 999)    AS rank_glo_p90_career,
# MAGIC     COALESCE(p.power_rank, 999)  AS power_rank,
# MAGIC     COALESCE(p.coaches_eligible, 0) AS coaches_eligible,
# MAGIC     COALESCE(p.rank_eligible, 999) AS rank_eligible,
# MAGIC     -- Top 5 races (by participation share)
# MAGIC     MAX(CASE WHEN tr.race_rank = 1 THEN tr.race_name END) AS race_1_name,
# MAGIC     MAX(CASE WHEN tr.race_rank = 1 THEN tr.participations_pct END) AS race_1_part_pct,
# MAGIC     MAX(CASE WHEN tr.race_rank = 1 THEN tr.coaches_pct END) AS race_1_coach_pct,
# MAGIC     MAX(CASE WHEN tr.race_rank = 2 THEN tr.race_name END) AS race_2_name,
# MAGIC     MAX(CASE WHEN tr.race_rank = 2 THEN tr.participations_pct END) AS race_2_part_pct,
# MAGIC     MAX(CASE WHEN tr.race_rank = 2 THEN tr.coaches_pct END) AS race_2_coach_pct,
# MAGIC     MAX(CASE WHEN tr.race_rank = 3 THEN tr.race_name END) AS race_3_name,
# MAGIC     MAX(CASE WHEN tr.race_rank = 3 THEN tr.participations_pct END) AS race_3_part_pct,
# MAGIC     MAX(CASE WHEN tr.race_rank = 3 THEN tr.coaches_pct END) AS race_3_coach_pct,
# MAGIC     MAX(CASE WHEN tr.race_rank = 4 THEN tr.race_name END) AS race_4_name,
# MAGIC     MAX(CASE WHEN tr.race_rank = 4 THEN tr.participations_pct END) AS race_4_part_pct,
# MAGIC     MAX(CASE WHEN tr.race_rank = 4 THEN tr.coaches_pct END) AS race_4_coach_pct,
# MAGIC     MAX(CASE WHEN tr.race_rank = 5 THEN tr.race_name END) AS race_5_name,
# MAGIC     MAX(CASE WHEN tr.race_rank = 5 THEN tr.participations_pct END) AS race_5_part_pct,
# MAGIC     MAX(CASE WHEN tr.race_rank = 5 THEN tr.coaches_pct END) AS race_5_coach_pct,
# MAGIC     -- Top 3 rivals
# MAGIC     MAX(CASE WHEN rv.rival_rank = 1 THEN rv.rival_name END) AS rival_1_name,
# MAGIC     MAX(CASE WHEN rv.rival_rank = 1 THEN rv.rival_games END) AS rival_1_games,
# MAGIC     MAX(CASE WHEN rv.rival_rank = 2 THEN rv.rival_name END) AS rival_2_name,
# MAGIC     MAX(CASE WHEN rv.rival_rank = 2 THEN rv.rival_games END) AS rival_2_games,
# MAGIC     MAX(CASE WHEN rv.rival_rank = 3 THEN rv.rival_name END) AS rival_3_name,
# MAGIC     MAX(CASE WHEN rv.rival_rank = 3 THEN rv.rival_games END) AS rival_3_games,
# MAGIC     COALESCE(rdr.diversity_index, 0) AS diversity_index,
# MAGIC     COALESCE(rdr.rank_diversity, 999) AS rank_diversity,
# MAGIC     COALESCE(ts.top8_avg_selector_global, 0) AS top8_avg_selector_global,
# MAGIC     COALESCE(tsr.rank_team_score, 999) AS rank_team_score,
# MAGIC     COALESCE(ts.top8_avg_glo_peak, 0)  AS top8_avg_glo_peak,
# MAGIC     COALESCE(tsr.rank_team_peak, 999)   AS rank_team_peak,
# MAGIC     COALESCE(ts.top8_avg_glo_median, 0) AS top8_avg_glo_median,
# MAGIC     COALESCE(tsr.rank_team_median, 999) AS rank_team_median,
# MAGIC     h.most_played_opp, h.best_record_opp, h.worst_record_opp,
# MAGIC     COALESCE(ip.pct_abroad, 0)      AS pct_abroad,
# MAGIC     COALESCE(ip.pct_vs_foreign, 0)  AS pct_vs_foreign,
# MAGIC     ip.ppg_vs_foreign,
# MAGIC     o.load_timestamp
# MAGIC   FROM overview o
# MAGIC   LEFT JOIN active_ranked ar ON o.nation_id = ar.nation_id
# MAGIC   LEFT JOIN wdl w            ON o.nation_id = w.nation_id
# MAGIC   LEFT JOIN glo_peak gp      ON o.nation_id = gp.nation_id
# MAGIC   LEFT JOIN glo_career gc    ON o.nation_id = gc.nation_id
# MAGIC   LEFT JOIN power p          ON o.nation_id = p.nation_id
# MAGIC   LEFT JOIN top_races tr      ON o.nation_id = tr.nation_id
# MAGIC   LEFT JOIN top_rivals rv      ON o.nation_id = rv.nation_id
# MAGIC   LEFT JOIN race_diversity_ranked rdr ON o.nation_id = rdr.nation_id
# MAGIC   LEFT JOIN team_str_ranked tsr ON o.nation_id = tsr.nation_id
# MAGIC   LEFT JOIN team_str ts      ON o.nation_id = ts.nation_id
# MAGIC   LEFT JOIN h2h_joined h     ON o.nation_id = h.nation_id
# MAGIC   LEFT JOIN intl_play ip     ON o.nation_id = ip.nation_id
# MAGIC   GROUP BY
# MAGIC     o.nation_id, o.nation_name_display, o.flag_emoji,
# MAGIC     o.coaches_count, o.rank_coaches,
# MAGIC     ar.active_coaches, ar.rank_active,
# MAGIC     o.games_count, o.rank_games,
# MAGIC     o.tournaments_hosted_count, o.rank_hosted,
# MAGIC     w.total_wins, w.total_draws, w.total_losses,
# MAGIC     gp.glo_median_peak, gp.glo_peak_best, gp.glo_p90,
# MAGIC     gp.rank_median_peak, gp.rank_peak, gp.rank_p90,
# MAGIC     gc.glo_median_career, gc.glo_p90_career, gc.rank_median_career, gc.rank_p90_career,
# MAGIC     p.power_rank, p.coaches_eligible, p.rank_eligible,
# MAGIC     rdr.diversity_index, rdr.rank_diversity,
# MAGIC     ts.top8_avg_selector_global, tsr.rank_team_score,
# MAGIC     ts.top8_avg_glo_peak, tsr.rank_team_peak,
# MAGIC     ts.top8_avg_glo_median, tsr.rank_team_median,
# MAGIC     h.most_played_opp, h.best_record_opp, h.worst_record_opp,
# MAGIC     ip.pct_abroad, ip.pct_vs_foreign, ip.ppg_vs_foreign,
# MAGIC     o.load_timestamp
# MAGIC )
# MAGIC
# MAGIC -- 11. Unpivot into long format
# MAGIC SELECT nation_id, nation_name_display, sort_order, label, profile, load_timestamp FROM (
# MAGIC   -- IDENTITY
# MAGIC   SELECT nation_id, nation_name_display, 10 AS sort_order,
# MAGIC     '🏳 Identity' AS label, CONCAT(flag_emoji, ' ', nation_name_display) AS profile, load_timestamp FROM combined
# MAGIC   UNION ALL SELECT nation_id, nation_name_display, 11,
# MAGIC     ' • NAF member coaches', CONCAT(CAST(coaches_count AS STRING), ' (#', CAST(rank_coaches AS STRING), ')'), load_timestamp FROM combined
# MAGIC   UNION ALL SELECT nation_id, nation_name_display, 12,
# MAGIC     ' • Active coaches', CONCAT(CAST(active_coaches AS STRING), ' (#', CAST(rank_active AS STRING), ')'), load_timestamp FROM combined
# MAGIC
# MAGIC   -- OVERVIEW
# MAGIC   UNION ALL SELECT nation_id, nation_name_display, 20,
# MAGIC     '📊 Overview', '', load_timestamp FROM combined
# MAGIC   UNION ALL SELECT nation_id, nation_name_display, 21,
# MAGIC     ' • Total games', CONCAT(CAST(games_count AS STRING), ' (#', CAST(rank_games AS STRING), ')'), load_timestamp FROM combined
# MAGIC   UNION ALL SELECT nation_id, nation_name_display, 22,
# MAGIC     ' • Tournaments hosted', CONCAT(CAST(tournaments_hosted_count AS STRING), ' (#', CAST(rank_hosted AS STRING), ')'), load_timestamp FROM combined
# MAGIC   UNION ALL SELECT nation_id, nation_name_display, 23,
# MAGIC     ' • W/D/L (international)', CONCAT(CAST(wins AS STRING), ' / ', CAST(draws AS STRING), ' / ', CAST(losses AS STRING)), load_timestamp FROM combined
# MAGIC
# MAGIC   -- RATINGS (GLO)
# MAGIC   UNION ALL SELECT nation_id, nation_name_display, 30,
# MAGIC     '📈 Ratings (GLO)', '', load_timestamp FROM combined
# MAGIC   UNION ALL SELECT nation_id, nation_name_display, 31,
# MAGIC     ' • Median GLO peak', CONCAT(COALESCE(CAST(ROUND(glo_median_peak, 1) AS STRING), 'N/A'), ' (#', CAST(rank_glo_median_peak AS STRING), ')'), load_timestamp FROM combined
# MAGIC   UNION ALL SELECT nation_id, nation_name_display, 32,
# MAGIC     ' • Median GLO career', CONCAT(COALESCE(CAST(ROUND(glo_median_career, 1) AS STRING), 'N/A'), ' (#', CAST(rank_glo_median_career AS STRING), ')'), load_timestamp FROM combined
# MAGIC   UNION ALL SELECT nation_id, nation_name_display, 33,
# MAGIC     ' • Peak GLO (best coach)', CONCAT(COALESCE(CAST(ROUND(glo_peak_best, 1) AS STRING), 'N/A'), ' (#', CAST(rank_glo_peak AS STRING), ')'), load_timestamp FROM combined
# MAGIC   UNION ALL SELECT nation_id, nation_name_display, 34,
# MAGIC     ' • P90 GLO peak', CONCAT(COALESCE(CAST(ROUND(glo_p90, 1) AS STRING), 'N/A'), ' (#', CAST(rank_glo_p90 AS STRING), ')'), load_timestamp FROM combined
# MAGIC   UNION ALL SELECT nation_id, nation_name_display, 35,
# MAGIC     ' • P90 GLO career', CONCAT(COALESCE(CAST(ROUND(glo_p90_career, 1) AS STRING), 'N/A'), ' (#', CAST(rank_glo_p90_career AS STRING), ')'), load_timestamp FROM combined
# MAGIC   UNION ALL SELECT nation_id, nation_name_display, 36,
# MAGIC     ' • Global Power Ranking', CONCAT('#', CAST(power_rank AS STRING)), load_timestamp FROM combined
# MAGIC
# MAGIC   -- RACE PROFILE (top 5 by participation share, showing both % metrics)
# MAGIC   UNION ALL SELECT nation_id, nation_name_display, 40,
# MAGIC     '🎲 Top Five Races', '', load_timestamp FROM combined
# MAGIC   UNION ALL SELECT nation_id, nation_name_display, 41,
# MAGIC     CONCAT(' 1. ', COALESCE(race_1_name, 'N/A')),
# MAGIC     CONCAT(CAST(race_1_part_pct AS STRING), '% games / ', CAST(race_1_coach_pct AS STRING), '% coaches'),
# MAGIC     load_timestamp FROM combined
# MAGIC   UNION ALL SELECT nation_id, nation_name_display, 42,
# MAGIC     CONCAT(' 2. ', COALESCE(race_2_name, 'N/A')),
# MAGIC     CONCAT(CAST(race_2_part_pct AS STRING), '% games / ', CAST(race_2_coach_pct AS STRING), '% coaches'),
# MAGIC     load_timestamp FROM combined
# MAGIC   UNION ALL SELECT nation_id, nation_name_display, 43,
# MAGIC     CONCAT(' 3. ', COALESCE(race_3_name, 'N/A')),
# MAGIC     CONCAT(CAST(race_3_part_pct AS STRING), '% games / ', CAST(race_3_coach_pct AS STRING), '% coaches'),
# MAGIC     load_timestamp FROM combined
# MAGIC   UNION ALL SELECT nation_id, nation_name_display, 44,
# MAGIC     CONCAT(' 4. ', COALESCE(race_4_name, 'N/A')),
# MAGIC     CONCAT(CAST(race_4_part_pct AS STRING), '% games / ', CAST(race_4_coach_pct AS STRING), '% coaches'),
# MAGIC     load_timestamp FROM combined
# MAGIC   UNION ALL SELECT nation_id, nation_name_display, 45,
# MAGIC     CONCAT(' 5. ', COALESCE(race_5_name, 'N/A')),
# MAGIC     CONCAT(CAST(race_5_part_pct AS STRING), '% games / ', CAST(race_5_coach_pct AS STRING), '% coaches'),
# MAGIC     load_timestamp FROM combined
# MAGIC   UNION ALL SELECT nation_id, nation_name_display, 46,
# MAGIC     ' • Race diversity index', CONCAT(CAST(diversity_index AS STRING), ' (#', CAST(rank_diversity AS STRING), ')'), load_timestamp FROM combined
# MAGIC
# MAGIC   -- INTERNATIONAL PLAY
# MAGIC   UNION ALL SELECT nation_id, nation_name_display, 50,
# MAGIC     '🌍 International Play', '', load_timestamp FROM combined
# MAGIC   UNION ALL SELECT nation_id, nation_name_display, 51,
# MAGIC     ' • Games abroad', CONCAT(CAST(pct_abroad AS STRING), '%'), load_timestamp FROM combined
# MAGIC   UNION ALL SELECT nation_id, nation_name_display, 52,
# MAGIC     ' • Games vs foreign', CONCAT(CAST(pct_vs_foreign AS STRING), '%'), load_timestamp FROM combined
# MAGIC   UNION ALL SELECT nation_id, nation_name_display, 53,
# MAGIC     ' • PPG vs foreign', COALESCE(CAST(ppg_vs_foreign AS STRING), 'N/A'), load_timestamp FROM combined
# MAGIC
# MAGIC   -- NATIONAL TEAM STRENGTH
# MAGIC   UNION ALL SELECT nation_id, nation_name_display, 60,
# MAGIC     '🏆 National Team Strength', '', load_timestamp FROM combined
# MAGIC   UNION ALL SELECT nation_id, nation_name_display, 61,
# MAGIC     ' • Top-8 avg selector score (global)', CONCAT(CAST(ROUND(top8_avg_selector_global, 1) AS STRING), ' (#', CAST(rank_team_score AS STRING), ')'), load_timestamp FROM combined
# MAGIC   UNION ALL SELECT nation_id, nation_name_display, 62,
# MAGIC     ' • Top-8 avg GLO peak', CONCAT(CAST(top8_avg_glo_peak AS STRING), ' (#', CAST(rank_team_peak AS STRING), ')'), load_timestamp FROM combined
# MAGIC   UNION ALL SELECT nation_id, nation_name_display, 63,
# MAGIC     ' • Top-8 avg GLO median', CONCAT(CAST(top8_avg_glo_median AS STRING), ' (#', CAST(rank_team_median AS STRING), ')'), load_timestamp FROM combined
# MAGIC   UNION ALL SELECT nation_id, nation_name_display, 64,
# MAGIC     ' • Coach pool (eligible)', CONCAT(CAST(coaches_eligible AS STRING), ' (#', CAST(rank_eligible AS STRING), ')'), load_timestamp FROM combined
# MAGIC
# MAGIC   -- HEAD-TO-HEAD
# MAGIC   UNION ALL SELECT nation_id, nation_name_display, 70,
# MAGIC     '⚡ Head-to-Head', '', load_timestamp FROM combined
# MAGIC   UNION ALL SELECT nation_id, nation_name_display, 71,
# MAGIC     ' • Most played opponent', COALESCE(most_played_opp, 'N/A'), load_timestamp FROM combined
# MAGIC   UNION ALL SELECT nation_id, nation_name_display, 72,
# MAGIC     ' • Best record vs', COALESCE(best_record_opp, 'N/A'), load_timestamp FROM combined
# MAGIC   UNION ALL SELECT nation_id, nation_name_display, 73,
# MAGIC     ' • Worst record vs', COALESCE(worst_record_opp, 'N/A'), load_timestamp FROM combined
# MAGIC
# MAGIC   -- TOP 3 RIVALS
# MAGIC   UNION ALL SELECT nation_id, nation_name_display, 80,
# MAGIC     '⚔ Top 3 Rivals', '', load_timestamp FROM combined
# MAGIC   UNION ALL SELECT nation_id, nation_name_display, 81,
# MAGIC     CONCAT(' 1. ', COALESCE(rival_1_name, 'N/A')),
# MAGIC     CONCAT(CAST(rival_1_games AS STRING), ' games'),
# MAGIC     load_timestamp FROM combined
# MAGIC   UNION ALL SELECT nation_id, nation_name_display, 82,
# MAGIC     CONCAT(' 2. ', COALESCE(rival_2_name, 'N/A')),
# MAGIC     CONCAT(CAST(rival_2_games AS STRING), ' games'),
# MAGIC     load_timestamp FROM combined
# MAGIC   UNION ALL SELECT nation_id, nation_name_display, 83,
# MAGIC     CONCAT(' 3. ', COALESCE(rival_3_name, 'N/A')),
# MAGIC     CONCAT(CAST(rival_3_games AS STRING), ' games'),
# MAGIC     load_timestamp FROM combined
# MAGIC ) x;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create view gold_presentation.nation_top_coach_opponent_bin_perf
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

# DBTITLE 1,Create view gold_presentation.nation_results_cumulative_display
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
# MAGIC --   - naf_catalog.gold_presentation.nation_identity_v (x2: nation + opponent)
# MAGIC --   - naf_catalog.gold_dim.date_dim
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.nation_results_cumulative_display AS
# MAGIC SELECT
# MAGIC   -- Nation identity
# MAGIC   s.nation_id,
# MAGIC   ni.nation_name_display,
# MAGIC   ni.flag_emoji,
# MAGIC
# MAGIC   -- Game-level context
# MAGIC   s.game_sequence_number,
# MAGIC   s.game_id,
# MAGIC   s.date_id,
# MAGIC   d.date AS game_date,
# MAGIC   d.year_month,
# MAGIC   s.event_timestamp,
# MAGIC   s.tournament_id,
# MAGIC   s.result_numeric,
# MAGIC
# MAGIC   -- Opponent nation
# MAGIC   s.opponent_nation_id,
# MAGIC   opp_ni.nation_name_display AS opponent_nation_name_display,
# MAGIC   opp_ni.flag_emoji          AS opponent_flag_emoji,
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
# MAGIC LEFT JOIN naf_catalog.gold_presentation.nation_identity_v AS ni
# MAGIC   ON s.nation_id = ni.nation_id
# MAGIC LEFT JOIN naf_catalog.gold_presentation.nation_identity_v AS opp_ni
# MAGIC   ON s.opponent_nation_id = opp_ni.nation_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.date_dim AS d
# MAGIC   ON s.date_id = d.date_id;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create view gold_presentation.nation_domestic_performance_display
# MAGIC %sql
# MAGIC -- VIEW: naf_catalog.gold_presentation.nation_domestic_performance_display
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE:
# MAGIC --   Dashboard contract for home/abroad performance metrics.
# MAGIC --   Shows performance split by tournament location and opponent origin.
# MAGIC -- LAYER:
# MAGIC --   GOLD_PRESENTATION
# MAGIC -- GRAIN:
# MAGIC --   1 row per nation_id
# MAGIC -- SOURCES:
# MAGIC --   - naf_catalog.gold_summary.nation_domestic_summary
# MAGIC --   - naf_catalog.gold_presentation.nation_identity_v
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.nation_domestic_performance_display AS
# MAGIC SELECT
# MAGIC   s.nation_id,
# MAGIC   ni.nation_name_display,
# MAGIC   ni.flag_emoji,
# MAGIC
# MAGIC   -- Away ratio (proportion of games played abroad)
# MAGIC   ROUND(100.0 * s.games_away / NULLIF(s.games_home + s.games_away, 0), 1)
# MAGIC     AS away_ratio_pct,
# MAGIC
# MAGIC   -- Tournament location: PPG home vs away
# MAGIC   s.games_home,
# MAGIC   s.games_away,
# MAGIC   ROUND(s.win_frac_home, 3)         AS ppg_home,
# MAGIC   ROUND(s.win_frac_away, 3)         AS ppg_away,
# MAGIC
# MAGIC   -- Opponent origin: PPG vs foreign (drop domestic PPG — always ~0.5)
# MAGIC   s.games_vs_foreign,
# MAGIC   ROUND(s.win_frac_vs_foreign, 3)   AS ppg_vs_foreign,
# MAGIC
# MAGIC   s.load_timestamp
# MAGIC FROM naf_catalog.gold_summary.nation_domestic_summary AS s
# MAGIC LEFT JOIN naf_catalog.gold_presentation.nation_identity_v AS ni
# MAGIC   ON s.nation_id = ni.nation_id;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create view gold_presentation.nation_opponent_elo_bin_wdl_display
# MAGIC %sql
# MAGIC -- VIEW: naf_catalog.gold_presentation.nation_opponent_elo_bin_wdl_display
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE:
# MAGIC --   Dashboard contract for nation W/D/L by opponent rating bin.
# MAGIC --   Supports GLO Metric filter via metric_type (PEAK/MEDIAN).
# MAGIC --   Fixed 4-bin scheme: 0–150, 150–200, 200–250, 250+.
# MAGIC -- LAYER:
# MAGIC --   GOLD_PRESENTATION
# MAGIC -- GRAIN:
# MAGIC --   1 row per (nation_id, metric_type, bin_index)
# MAGIC -- SOURCES:
# MAGIC --   - naf_catalog.gold_summary.nation_opponent_elo_bin_wdl
# MAGIC --   - naf_catalog.gold_presentation.nation_identity_v
# MAGIC -- PHASE: 5
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.nation_opponent_elo_bin_wdl_display AS
# MAGIC SELECT
# MAGIC   s.nation_id,
# MAGIC   ni.nation_name_display,
# MAGIC   ni.flag_emoji,
# MAGIC   s.metric_type,
# MAGIC   s.bin_index,
# MAGIC   s.bin_label,
# MAGIC   s.bin_min,
# MAGIC   s.bin_max,
# MAGIC   s.games,
# MAGIC   s.wins,
# MAGIC   s.draws,
# MAGIC   s.losses,
# MAGIC   s.ppg,
# MAGIC   s.load_timestamp
# MAGIC FROM naf_catalog.gold_summary.nation_opponent_elo_bin_wdl AS s
# MAGIC LEFT JOIN naf_catalog.gold_presentation.nation_identity_v AS ni
# MAGIC   ON s.nation_id = ni.nation_id;
# MAGIC

# COMMAND ----------

# DBTITLE 1,REMOVED: nation_game_quality_bin_wdl_display
# MAGIC %sql
# MAGIC -- REMOVED: nation_game_quality_bin_wdl_display
# MAGIC -- Legacy view — depended on configurable bin framework. Not used by any dashboard.

# COMMAND ----------

# DBTITLE 1,Create view gold_presentation.nation_team_candidates_display
# MAGIC %sql
# MAGIC -- VIEW: naf_catalog.gold_presentation.nation_team_candidates_display
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE:
# MAGIC --   Dashboard contract for the national team selector.
# MAGIC --   3-component scoring: GLO 50%, Race 25%, Opponent 25% (all global ranks).
# MAGIC --   Shows eligible coaches with scores, component ranks, raw metrics,
# MAGIC --   and supplementary badge counts.
# MAGIC -- LAYER:
# MAGIC --   GOLD_PRESENTATION
# MAGIC -- GRAIN:
# MAGIC --   1 row per (nation_id, coach_id)
# MAGIC -- SOURCES:
# MAGIC --   - naf_catalog.gold_summary.nation_team_candidate_scores
# MAGIC --   - naf_catalog.gold_presentation.nation_identity_v
# MAGIC --   - naf_catalog.gold_dim.coach_dim
# MAGIC --   - naf_catalog.gold_summary.coach_race_relative_strength  (specialist counts)
# MAGIC --   - naf_catalog.gold_summary.coach_race_nation_rank        (national specialist)
# MAGIC -- PHASE: 6
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.nation_team_candidates_display AS
# MAGIC SELECT
# MAGIC   tc.nation_id,
# MAGIC   ni.nation_name_display,
# MAGIC   ni.flag_emoji,
# MAGIC   tc.coach_id,
# MAGIC   cd.coach_name,
# MAGIC
# MAGIC   -- Ranking
# MAGIC   tc.selector_rank_national,
# MAGIC   tc.selector_rank_global,
# MAGIC   tc.selector_score,
# MAGIC
# MAGIC   -- Core component ranks (global, 1 = best)
# MAGIC   tc.glo_rank,
# MAGIC   tc.race_rank,
# MAGIC   tc.opponent_rank,
# MAGIC
# MAGIC   -- Raw metric values
# MAGIC   tc.glo_peak,
# MAGIC   tc.glo_median,
# MAGIC   tc.race_elo_peak_mean,
# MAGIC   tc.avg_opponent_glo,
# MAGIC
# MAGIC   -- Supplementary (display only, not in selector score)
# MAGIC   tc.form_score,
# MAGIC   tc.races_above_world_median,
# MAGIC   tc.races_played_eligible,
# MAGIC   COALESCE(ws.world_specialist_count,    0) AS world_specialist_count,
# MAGIC   COALESCE(ns.national_specialist_count, 0) AS national_specialist_count,
# MAGIC
# MAGIC   tc.load_timestamp
# MAGIC FROM naf_catalog.gold_summary.nation_team_candidate_scores tc
# MAGIC LEFT JOIN naf_catalog.gold_presentation.nation_identity_v ni
# MAGIC   ON tc.nation_id = ni.nation_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.coach_dim cd
# MAGIC   ON tc.coach_id = cd.coach_id
# MAGIC LEFT JOIN (
# MAGIC   SELECT
# MAGIC     coach_id,
# MAGIC     CAST(SUM(CASE WHEN is_world_specialist THEN 1 ELSE 0 END) AS INT) AS world_specialist_count
# MAGIC   FROM naf_catalog.gold_summary.coach_race_relative_strength
# MAGIC   GROUP BY coach_id
# MAGIC ) ws ON tc.coach_id = ws.coach_id
# MAGIC LEFT JOIN (
# MAGIC   SELECT
# MAGIC     coach_id,
# MAGIC     nation_id,
# MAGIC     CAST(SUM(CASE WHEN is_national_specialist THEN 1 ELSE 0 END) AS INT) AS national_specialist_count
# MAGIC   FROM naf_catalog.gold_summary.coach_race_nation_rank
# MAGIC   GROUP BY coach_id, nation_id
# MAGIC ) ns ON tc.coach_id = ns.coach_id AND tc.nation_id = ns.nation_id;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create view gold_presentation.nation_power_ranking_display
# MAGIC %sql
# MAGIC -- VIEW: naf_catalog.gold_presentation.nation_power_ranking_display
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE:
# MAGIC --   Dashboard contract for the nation power ranking table.
# MAGIC --   Ranks nations by the average selector_score of their top 8 candidates.
# MAGIC --   Weights: GLO 50%, Race 25%, Opponent 25% (all global ranks).
# MAGIC -- LAYER:
# MAGIC --   GOLD_PRESENTATION
# MAGIC -- GRAIN:
# MAGIC --   1 row per nation_id
# MAGIC -- SOURCES:
# MAGIC --   - naf_catalog.gold_summary.nation_power_ranking
# MAGIC --   - naf_catalog.gold_presentation.nation_identity_v
# MAGIC -- PHASE: 6
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.nation_power_ranking_display AS
# MAGIC SELECT
# MAGIC   pr.power_rank,
# MAGIC   pr.nation_id,
# MAGIC   ni.nation_name_display,
# MAGIC   ni.flag_emoji,
# MAGIC   pr.top_8_avg_selector_score,
# MAGIC   pr.top_8_avg_glo_peak,
# MAGIC   pr.top_8_avg_glo_median,
# MAGIC   pr.coaches_in_top_8,
# MAGIC   pr.coaches_eligible,
# MAGIC   pr.load_timestamp
# MAGIC FROM naf_catalog.gold_summary.nation_power_ranking pr
# MAGIC LEFT JOIN naf_catalog.gold_presentation.nation_identity_v ni
# MAGIC   ON pr.nation_id = ni.nation_id;
# MAGIC
