# Databricks notebook source
# MAGIC %md
# MAGIC # 343 — Gold Presentation Tournament
# MAGIC
# MAGIC **Layer:** GOLD_PRESENTATION  |  **Status:** Placeholder
# MAGIC **Pipeline position:** After 340 (core presentation) and 333 (tournament summaries)
# MAGIC
# MAGIC Dashboard-facing views for tournament analytics. Joins dims and summaries — no re-aggregation of facts.
# MAGIC
# MAGIC **Status note (2026-04-10):** Original views referenced columns/tables that do not exist in
# MAGIC the current schema (e.g. `gold_dim.tournament`, `gold_dim.coach`, `gold_dim.nation`,
# MAGIC `tournament_nation_display`, `flag_code`/`fifa_code` on tournament, `coaches_count`,
# MAGIC `race_is_counted`, `sort_order`, `major_boolean`, `organizer_id`). They have been replaced
# MAGIC with minimal placeholder views that join to real columns only. Expand with dashboard-driven
# MAGIC additions during the tournament dashboard phase.
# MAGIC
# MAGIC ## Dependencies
# MAGIC - `gold_dim.tournament_dim`, `gold_dim.tournament_parameters`, `gold_dim.coach_dim`, `gold_dim.nation_dim`, `gold_dim.race_dim`
# MAGIC - `gold_summary.tournament_summary`, `gold_summary.tournament_race_summary`
# MAGIC - `gold_summary.tournament_nation_summary`, `gold_summary.tournament_winners`
# MAGIC
# MAGIC ## Outputs
# MAGIC - `gold_presentation.tournament_overview` (VIEW) — 1 row per tournament_id; hero metrics + winner
# MAGIC - `gold_presentation.tournament_race_meta` (VIEW) — 1 row per (tournament_id, race_id)
# MAGIC - `gold_presentation.tournament_nation_meta` (VIEW) — 1 row per (tournament_id, nation_name)
# MAGIC
# MAGIC **Design authority:** `NAF_Design_Specification.md`, `style_guides.md`

# COMMAND ----------

# DBTITLE 1,Create schema gold_presentation
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS naf_catalog.gold_presentation;

# COMMAND ----------

# DBTITLE 1,gold_presentation.tournament_overview
# MAGIC %sql
# MAGIC -- VIEW: naf_catalog.gold_presentation.tournament_overview
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : 1 row per tournament_id; hero metrics + winner
# MAGIC -- LAYER        : GOLD_PRESENTATION
# MAGIC -- GRAIN        : 1 row per tournament_id
# MAGIC -- SOURCES      : gold_dim.tournament_dim, gold_dim.tournament_parameters,
# MAGIC --                gold_dim.coach_dim, gold_summary.tournament_summary
# MAGIC -- NOTES        : Placeholder — minimal column set referencing real schema only.
# MAGIC --                Nation-level enrichment (flag, ISO codes) is intentionally omitted
# MAGIC --                here; join to nation_dim downstream if needed.
# MAGIC -- =====================================================================
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.tournament_overview AS
# MAGIC SELECT
# MAGIC     -- Core tournament info
# MAGIC     t.tournament_id,
# MAGIC     t.tournament_name,
# MAGIC     t.date_start,
# MAGIC     t.date_end,
# MAGIC     t.is_major_tournament,
# MAGIC     t.nation_id,
# MAGIC     t.organizer_coach_id,
# MAGIC
# MAGIC     -- Summary metrics
# MAGIC     ts.games_count,
# MAGIC     ts.participants_count,
# MAGIC     ts.nations_count,
# MAGIC     ts.races_count,
# MAGIC     ts.race_entropy,
# MAGIC     ts.race_gini,
# MAGIC     ts.glo_peak_mean,
# MAGIC     ts.first_game_timestamp,
# MAGIC     ts.last_game_timestamp,
# MAGIC     ts.winner_coach_id,
# MAGIC     ts.winner_race_id,
# MAGIC
# MAGIC     -- Winner coach name
# MAGIC     c.coach_name AS winner_coach_name,
# MAGIC
# MAGIC     -- Tournament parameter settings (Elo-related)
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
# MAGIC FROM naf_catalog.gold_dim.tournament_dim t
# MAGIC LEFT JOIN naf_catalog.gold_summary.tournament_summary ts
# MAGIC        ON t.tournament_id = ts.tournament_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.tournament_parameters tp
# MAGIC        ON t.tournament_id = tp.tournament_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.coach_dim c
# MAGIC        ON ts.winner_coach_id = c.coach_id;

# COMMAND ----------

# DBTITLE 1,gold_presentation.tournament_race_meta
# MAGIC %sql
# MAGIC -- VIEW: naf_catalog.gold_presentation.tournament_race_meta
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : 1 row per (tournament_id, race_id) — race participation per tournament
# MAGIC -- LAYER        : GOLD_PRESENTATION
# MAGIC -- GRAIN        : 1 row per (tournament_id, coach_race_id)
# MAGIC -- SOURCES      : gold_summary.tournament_race_summary, gold_dim.race_dim
# MAGIC -- NOTES        : Placeholder — race_dim only exposes race_id and race_name today,
# MAGIC --                so the previously referenced sort_order / race_is_counted columns
# MAGIC --                have been removed.
# MAGIC -- =====================================================================
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.tournament_race_meta AS
# MAGIC SELECT
# MAGIC     trs.tournament_id,
# MAGIC     trs.coach_race_id AS race_id,
# MAGIC
# MAGIC     rc.race_name,
# MAGIC
# MAGIC     trs.participants_count_race,
# MAGIC     trs.games_count_race,
# MAGIC     trs.games_pct_race,
# MAGIC     trs.participants_pct_race,
# MAGIC     trs.points_per_game_race
# MAGIC
# MAGIC FROM naf_catalog.gold_summary.tournament_race_summary trs
# MAGIC LEFT JOIN naf_catalog.gold_dim.race_dim rc
# MAGIC        ON trs.coach_race_id = rc.race_id;

# COMMAND ----------

# DBTITLE 1,gold_presentation.tournament_nation_meta
# MAGIC %sql
# MAGIC -- VIEW: naf_catalog.gold_presentation.tournament_nation_meta
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : 1 row per (tournament_id, nation_name) — nation participation per tournament
# MAGIC -- LAYER        : GOLD_PRESENTATION
# MAGIC -- GRAIN        : 1 row per (tournament_id, nation_name)
# MAGIC -- SOURCES      : gold_summary.tournament_nation_summary, gold_dim.nation_dim
# MAGIC -- NOTES        : Placeholder — joins to nation_dim by upper-trimmed nation_name.
# MAGIC --                Match rate is best-effort; canonical nation_id should come from
# MAGIC --                upstream once tournament_nation_summary carries it directly.
# MAGIC -- =====================================================================
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.tournament_nation_meta AS
# MAGIC SELECT
# MAGIC     tns.tournament_id,
# MAGIC     tns.nation_name,
# MAGIC
# MAGIC     n.nation_id,
# MAGIC     n.nation_name_display,
# MAGIC     n.iso2_code,
# MAGIC     n.iso3_code,
# MAGIC     n.fifa_code,
# MAGIC     n.flag_code,
# MAGIC
# MAGIC     tns.participants_count_nation AS num_coaches_from_nation,
# MAGIC     tns.games_count_nation        AS num_games_by_nation,
# MAGIC     tns.games_pct                 AS pct_of_games,
# MAGIC     tns.participants_pct          AS pct_of_participants,
# MAGIC     tns.points_per_game           AS avg_result_numeric
# MAGIC
# MAGIC FROM naf_catalog.gold_summary.tournament_nation_summary tns
# MAGIC LEFT JOIN naf_catalog.gold_dim.nation_dim n
# MAGIC     ON UPPER(TRIM(tns.nation_name)) = UPPER(TRIM(n.nation_name));
