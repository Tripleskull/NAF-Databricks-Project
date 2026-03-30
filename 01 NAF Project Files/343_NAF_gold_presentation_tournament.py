# Databricks notebook source
# MAGIC %md
# MAGIC # 343 — Tournament Presentation Views
# MAGIC
# MAGIC **Layer:** GOLD_PRESENTATION &nbsp;|&nbsp; **Status:** Production
# MAGIC **Pipeline position:** Runs after 340 (core presentation) and 333 (tournament summaries)
# MAGIC
# MAGIC Dashboard-contract views for tournament analytics.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS naf_catalog.gold_presentation;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.tournament_overview AS
# MAGIC SELECT
# MAGIC     -- Core tournament info
# MAGIC     t.tournament_id,
# MAGIC     t.tournament_name,
# MAGIC     t.date_start,
# MAGIC     t.date_end,
# MAGIC     t.tournament_nation,
# MAGIC     t.tournament_nation_display,
# MAGIC     t.iso2_code,
# MAGIC     t.flag_code,
# MAGIC     t.fifa_code,
# MAGIC     t.fifa_nation_name,
# MAGIC     t.major_boolean,
# MAGIC     t.nation_id,
# MAGIC     t.organizer_id,
# MAGIC
# MAGIC     -- Summary metrics (aliased to num_* for presentation)
# MAGIC     ts.games_count,
# MAGIC     ts.participants_count,
# MAGIC     ts.nations_count,
# MAGIC     ts.races_count,
# MAGIC     ts.glo_peak_mean,
# MAGIC     ts.race_entropy,
# MAGIC     ts.race_gini,
# MAGIC     ts.first_game_timestamp,
# MAGIC     ts.last_game_timestamp,
# MAGIC     ts.winner_coach_id,
# MAGIC     ts.winner_race_id,
# MAGIC
# MAGIC     -- Winner coach name
# MAGIC     c.coach_name AS winner_coach_name,
# MAGIC
# MAGIC     -- Tournament parameter settings (ELO-related)
# MAGIC     tp.coaches_count,
# MAGIC     tp.k_value,
# MAGIC     tp.n_eff,
# MAGIC
# MAGIC     -- Derived metrics
# MAGIC     CASE
# MAGIC         WHEN ts.participants_count > 0 THEN
# MAGIC             ts.games_count * 1.0 / ts.participants_count
# MAGIC         ELSE NULL
# MAGIC     END AS games_per_participant,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN ts.participants_count > 0 THEN
# MAGIC             CEIL(ts.games_count * 1.0 / ts.participants_count)
# MAGIC         ELSE NULL
# MAGIC     END AS games_per_participant_ceil,
# MAGIC
# MAGIC     DATEDIFF(t.date_end, t.date_start) + 1 AS duration_days
# MAGIC
# MAGIC FROM naf_catalog.gold_dim.tournament t
# MAGIC LEFT JOIN naf_catalog.gold_summary.tournament_summary ts
# MAGIC        ON t.tournament_id = ts.tournament_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.tournament_parameters tp
# MAGIC        ON t.tournament_id = tp.tournament_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.coach c
# MAGIC        ON ts.winner_coach_id = c.coach_id;
# MAGIC


# COMMAND ----------

# DBTITLE 1,Create schema gold_presentation
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS naf_catalog.gold_presentation;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.tournament_race_meta AS
# MAGIC SELECT
# MAGIC     -- Keys
# MAGIC     trs.tournament_id,
# MAGIC     trs.coach_race_id AS race_id,
# MAGIC
# MAGIC     -- Race lookup
# MAGIC     rc.race_name,
# MAGIC     rc.sort_order,
# MAGIC     rc.race_is_counted,
# MAGIC
# MAGIC     -- Summary metrics (aliased for readability)
# MAGIC     trs.participants_count_race,
# MAGIC     trs.games_count_race,
# MAGIC     trs.games_pct_race,
# MAGIC     trs.participants_pct_race,
# MAGIC     trs.points_per_game_race
# MAGIC
# MAGIC FROM naf_catalog.gold_summary.tournament_race_summary trs
# MAGIC LEFT JOIN naf_catalog.gold_dim.race_dim rc
# MAGIC        ON trs.coach_race_id = rc.race_id;
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create schema gold_presentation
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS naf_catalog.gold_presentation;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.tournament_nation_meta AS
# MAGIC SELECT
# MAGIC     -- Keys
# MAGIC     tns.tournament_id,
# MAGIC     tns.nation_name,
# MAGIC
# MAGIC     -- Nation lookup from gold_dim.nation (no new calc)
# MAGIC     n.nation_id,
# MAGIC     n.fifa_nation_name,
# MAGIC     n.fifa_code,
# MAGIC     n.flag_code,
# MAGIC
# MAGIC     -- Summary metrics (aliased to your intended names)
# MAGIC     tns.participants_count_nation AS num_coaches_from_nation,
# MAGIC     tns.games_count_nation        AS num_games_by_nation,
# MAGIC     tns.games_pct                 AS pct_of_games,
# MAGIC     tns.participants_pct          AS pct_of_participants,
# MAGIC     tns.points_per_game           AS avg_result_numeric
# MAGIC
# MAGIC FROM naf_catalog.gold_summary.tournament_nation_summary tns
# MAGIC LEFT JOIN naf_catalog.gold_dim.nation n
# MAGIC     ON UPPER(TRIM(tns.nation_name)) = UPPER(TRIM(n.nation_name));
