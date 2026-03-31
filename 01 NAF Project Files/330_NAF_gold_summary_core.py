# Databricks notebook source
# MAGIC %md
# MAGIC # 330 — Gold Summary Core
# MAGIC
# MAGIC **Layer:** GOLD_SUMMARY  |  **Status:** Production
# MAGIC **Pipeline position:** After 320 (fact layer); before 331–334
# MAGIC
# MAGIC Builds cross-cutting summary objects shared by all downstream summary notebooks (331–334).
# MAGIC Depends only on gold_dim and gold_fact — never on other summary notebooks.
# MAGIC
# MAGIC ## Dependencies
# MAGIC - `gold_dim.analytical_config` — tuneable parameters
# MAGIC - `gold_dim.coach_dim` — coach → nation_id
# MAGIC - `gold_fact.rating_history_fact` — Elo event history
# MAGIC
# MAGIC ## Outputs
# MAGIC - `gold_summary.nation_coach_glo_metrics` — 1 row per (nation_id, coach_id); GLOBAL Elo metrics
# MAGIC - `gold_summary.world_glo_metric_quantiles` (VIEW) — world GLO quantile benchmarks (PEAK/MEAN/MEDIAN)
# MAGIC
# MAGIC **Design authority:** `NAF_Design_Specification.md`, `style_guides.md`
# MAGIC

# COMMAND ----------

# DBTITLE 1,gold_summary.nation_coach_glo_metrics
# MAGIC %sql
# MAGIC -- TABLE: naf_catalog.gold_summary.nation_coach_glo_metrics
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE:
# MAGIC --   - One row per (nation_id, coach_id)
# MAGIC --   - GLOBAL (race-agnostic) NAF_ELO metrics from rating_history_fact
# MAGIC -- NOTES:
# MAGIC --   - is_valid_glo indicates stable sample (games_played >= min_games_nation_glo from config)
# MAGIC --   - glo_* metrics are computed from the coach's >= min_games_nation_glo-th GLOBAL game onward
# MAGIC --   - glo_*_all metrics are computed from all GLOBAL games (optional but useful)
# MAGIC --   - No opponent strength columns here — join coach_performance_summary (331)
# MAGIC --     where needed downstream.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_summary.nation_coach_glo_metrics
# MAGIC USING DELTA AS
# MAGIC
# MAGIC WITH params AS (
# MAGIC   SELECT min_games_nation_glo AS min_games
# MAGIC   FROM naf_catalog.gold_dim.analytical_config
# MAGIC ),
# MAGIC
# MAGIC rh_dedup AS (
# MAGIC   SELECT
# MAGIC     coach_id,
# MAGIC     game_id,
# MAGIC     game_index,
# MAGIC     event_timestamp,
# MAGIC     rating_after
# MAGIC   FROM naf_catalog.gold_fact.rating_history_fact
# MAGIC   WHERE scope = 'GLOBAL'
# MAGIC     AND rating_system = 'NAF_ELO'
# MAGIC     AND COALESCE(race_id, 0) = 0
# MAGIC     AND coach_id IS NOT NULL
# MAGIC   QUALIFY ROW_NUMBER() OVER (
# MAGIC     PARTITION BY coach_id, game_id
# MAGIC     ORDER BY event_timestamp DESC, game_index DESC, game_id DESC
# MAGIC   ) = 1
# MAGIC ),
# MAGIC
# MAGIC hist AS (
# MAGIC   SELECT
# MAGIC     cd.nation_id,
# MAGIC     r.coach_id,
# MAGIC     r.game_id,
# MAGIC     r.game_index,
# MAGIC     r.event_timestamp,
# MAGIC     r.rating_after
# MAGIC   FROM rh_dedup r
# MAGIC   INNER JOIN naf_catalog.gold_dim.coach_dim cd
# MAGIC     ON r.coach_id = cd.coach_id
# MAGIC   WHERE cd.nation_id IS NOT NULL
# MAGIC ),
# MAGIC
# MAGIC numbered AS (
# MAGIC   SELECT
# MAGIC     h.*,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY coach_id
# MAGIC       ORDER BY game_index ASC, game_id ASC
# MAGIC     ) AS coach_game_number,
# MAGIC     COUNT(*) OVER (PARTITION BY coach_id) AS games_played
# MAGIC   FROM hist h
# MAGIC ),
# MAGIC
# MAGIC all_agg AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     coach_id,
# MAGIC     MAX(rating_after)                    AS glo_peak_all,
# MAGIC     AVG(rating_after)                    AS glo_mean_all,
# MAGIC     PERCENTILE_APPROX(rating_after, 0.5) AS glo_median_all,
# MAGIC     MAX(games_played)                    AS games_played
# MAGIC   FROM numbered
# MAGIC   GROUP BY nation_id, coach_id
# MAGIC ),
# MAGIC
# MAGIC eligible AS (
# MAGIC   SELECT
# MAGIC     n.nation_id,
# MAGIC     n.coach_id,
# MAGIC     n.rating_after
# MAGIC   FROM numbered n
# MAGIC   CROSS JOIN params p
# MAGIC   WHERE n.games_played >= p.min_games
# MAGIC     AND n.coach_game_number >= p.min_games
# MAGIC ),
# MAGIC
# MAGIC eligible_agg AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     coach_id,
# MAGIC     MAX(rating_after)                    AS glo_peak,
# MAGIC     AVG(rating_after)                    AS glo_mean,
# MAGIC     PERCENTILE_APPROX(rating_after, 0.5) AS glo_median
# MAGIC   FROM eligible
# MAGIC   GROUP BY nation_id, coach_id
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   a.nation_id,
# MAGIC   a.coach_id,
# MAGIC
# MAGIC   -- Stable-sample metrics (NULL unless valid)
# MAGIC   e.glo_peak,
# MAGIC   e.glo_mean,
# MAGIC   e.glo_median,
# MAGIC
# MAGIC   -- Optional: full-history metrics
# MAGIC   a.glo_peak_all,
# MAGIC   a.glo_mean_all,
# MAGIC   a.glo_median_all,
# MAGIC
# MAGIC   a.games_played,
# MAGIC   (a.games_played >= p.min_games) AS is_valid_glo,
# MAGIC
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM all_agg a
# MAGIC CROSS JOIN params p
# MAGIC LEFT JOIN eligible_agg e
# MAGIC   ON a.nation_id = e.nation_id
# MAGIC  AND a.coach_id  = e.coach_id;

# COMMAND ----------

# DBTITLE 1,gold_summary.world_glo_metric_quantiles
# MAGIC %sql -- VIEW: naf_catalog.gold_summary.world_glo_metric_quantiles
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : World-level GLO metric quantiles (PEAK/MEAN/MEDIAN).
# MAGIC --                Provides the boxplot baseline so dashboards don't need
# MAGIC --                synthetic "World" rows.
# MAGIC -- LAYER        : GOLD_SUMMARY
# MAGIC -- GRAIN        : 1 row per metric_type
# MAGIC -- PRIMARY KEY  : (metric_type)
# MAGIC -- SOURCES      : naf_catalog.gold_summary.nation_coach_glo_metrics
# MAGIC -- =====================================================================
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_summary.world_glo_metric_quantiles AS
# MAGIC WITH metric_long AS (
# MAGIC   SELECT
# MAGIC     m.coach_id,
# MAGIC     stack(
# MAGIC       3,
# MAGIC       'PEAK',   m.glo_peak,
# MAGIC       'MEAN',   m.glo_mean,
# MAGIC       'MEDIAN', m.glo_median
# MAGIC     ) AS (metric_type, metric_value)
# MAGIC   FROM naf_catalog.gold_summary.nation_coach_glo_metrics m
# MAGIC   WHERE m.is_valid_glo = TRUE
# MAGIC )
# MAGIC SELECT
# MAGIC   metric_type,
# MAGIC   CAST(COUNT(DISTINCT coach_id) AS INT) AS coaches_count,
# MAGIC   MIN(metric_value)                     AS value_min,
# MAGIC   PERCENTILE_APPROX(metric_value, 0.10) AS value_p10,
# MAGIC   PERCENTILE_APPROX(metric_value, 0.25) AS value_p25,
# MAGIC   PERCENTILE_APPROX(metric_value, 0.50) AS value_p50,
# MAGIC   PERCENTILE_APPROX(metric_value, 0.75) AS value_p75,
# MAGIC   PERCENTILE_APPROX(metric_value, 0.90) AS value_p90,
# MAGIC   MAX(metric_value)                     AS value_max,
# MAGIC   CURRENT_TIMESTAMP()                   AS load_timestamp
# MAGIC FROM metric_long
# MAGIC WHERE metric_value IS NOT NULL
# MAGIC GROUP BY metric_type;
# MAGIC
# MAGIC
# MAGIC -- NOTE: Presentation views (nation_glo_metric_quantiles, nation_glo_peak_card_long)
# MAGIC -- are defined in 342_NAF_gold_presentation_nation.py (correct notebook).
