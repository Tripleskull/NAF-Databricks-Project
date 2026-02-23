# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Presentation — Coach (`naf_catalog.gold_presentation`)
# MAGIC
# MAGIC Build **dashboard-facing contracts** (thin shapes) for coach analytics.
# MAGIC
# MAGIC **Inputs (only):**
# MAGIC - `naf_catalog.gold_dim` = canonical attributes + stable IDs
# MAGIC - `naf_catalog.gold_summary` = all KPIs / aggregates / snapshots
# MAGIC
# MAGIC **Presentation is allowed to:**
# MAGIC - join dims + summaries
# MAGIC - add UI helpers (labels/buckets/ranks, stable sort fields like `order_index`)
# MAGIC - compute time-dependent UI fields at query time (e.g. `*_age_days` via `CURRENT_DATE()`)
# MAGIC
# MAGIC **Presentation is NOT allowed to:**
# MAGIC - re-aggregate facts
# MAGIC - redefine KPI logic (metrics belong to `gold_summary`)
# MAGIC
# MAGIC ## Canonical coach contracts (dashboards should prefer these)
# MAGIC - `naf_catalog.gold_presentation.coach_profile` — 1 row per `coach_id`
# MAGIC - `naf_catalog.gold_presentation.coach_race_performance` — 1 row per (`coach_id`, `race_id`)
# MAGIC
# MAGIC ## Stability rules
# MAGIC - Prefer **explicit column lists** (avoid `SELECT *`)
# MAGIC - Keep canonical keys explicit (`opponent_coach_id`, `tournament_id`, `race_id`); if adding UI aliases, keep the canonical key too
# MAGIC - Time-dependent fields should be **views only** (no stored tables)
# MAGIC
# MAGIC ## Notebook conventions
# MAGIC - **One object per SQL cell** (one `CREATE OR REPLACE VIEW` per cell)
# MAGIC - Put the target object on the first line (e.g. `%sql -- VIEW: naf_catalog.gold_presentation.coach_profile`)
# MAGIC - Views use `CREATE OR REPLACE VIEW ... AS` (no `USING DELTA`)
# MAGIC
# MAGIC **Design authority (wins):**
# MAGIC - Project Design → `00_design_decisions.md`
# MAGIC - Project Design → `02_schema_design.md`
# MAGIC - Project Design → `03_style_guides.md`
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DANGER: drops EVERYTHING in naf_catalog.gold_presentation (views + tables) incl. dependencies.
# MAGIC -- DROP SCHEMA IF EXISTS naf_catalog.gold_presentation CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS naf_catalog.gold_presentation;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- *VIEW*: naf_catalog.gold_presentation.coach_profile
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Canonical coach "profile" contract used by dashboards.
# MAGIC --                Joins summary-level performance + ratings + streaks + top races.
# MAGIC --                UI-friendly fields included (pct, display-ready race slots).
# MAGIC -- LAYER        : GOLD_PRESENTATION
# MAGIC -- GRAIN        : 1 row per coach_id
# MAGIC -- PRIMARY KEY  : (coach_id)
# MAGIC -- SOURCES      : naf_catalog.gold_summary.coach_performance_summary,
# MAGIC --                naf_catalog.gold_summary.coach_rating_global_elo_summary,
# MAGIC --                naf_catalog.gold_summary.coach_opponent_global_elo_mean_summary_v,
# MAGIC --                naf_catalog.gold_summary.coach_race_summary,
# MAGIC --                naf_catalog.gold_summary.coach_streak_summary,
# MAGIC --                naf_catalog.gold_summary.coach_race_performance (via coach_race_performance view),
# MAGIC --                naf_catalog.gold_summary.coach_form_summary,
# MAGIC --                naf_catalog.gold_presentation.coach_identity_v
# MAGIC -- NOTES        : "GLOBAL" scope is represented by scope_race_id = 0 (not NULL)
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.coach_profile AS
# MAGIC WITH race_ranked AS (
# MAGIC   SELECT
# MAGIC     cr.coach_id,
# MAGIC     cr.race_id,
# MAGIC     rd.race_name,
# MAGIC     cr.games_played,
# MAGIC     cr.win_frac,
# MAGIC     cr.points_per_game,
# MAGIC     cr.min_games_threshold_race_performance,
# MAGIC     cr.is_valid_min_games_race_performance,
# MAGIC     cr.load_timestamp AS race_load_timestamp,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY cr.coach_id
# MAGIC       ORDER BY
# MAGIC         cr.is_valid_min_games_race_performance DESC,
# MAGIC         cr.games_played DESC,
# MAGIC         cr.win_frac DESC,
# MAGIC         cr.race_id ASC
# MAGIC     ) AS rn
# MAGIC   FROM naf_catalog.gold_summary.coach_race_summary AS cr
# MAGIC   LEFT JOIN naf_catalog.gold_dim.race_dim AS rd
# MAGIC     ON cr.race_id = rd.race_id
# MAGIC   WHERE cr.rating_system = 'NAF_ELO'
# MAGIC ),
# MAGIC
# MAGIC top_races AS (
# MAGIC   SELECT
# MAGIC     coach_id,
# MAGIC
# MAGIC     MAX(CASE WHEN rn = 1 THEN race_id END)        AS race_1_id,
# MAGIC     MAX(CASE WHEN rn = 1 THEN race_name END)      AS race_1_name,
# MAGIC     MAX(CASE WHEN rn = 1 THEN games_played END)   AS race_1_games_played,
# MAGIC     MAX(CASE WHEN rn = 1 THEN win_frac END)       AS race_1_win_frac,
# MAGIC     MAX(CASE WHEN rn = 1 THEN points_per_game END) AS race_1_points_per_game,
# MAGIC     MAX(CASE WHEN rn = 1 THEN is_valid_min_games_race_performance END)      AS race_1_is_valid_min_games,
# MAGIC     MAX(CASE WHEN rn = 1 THEN min_games_threshold_race_performance END)     AS race_1_min_games_threshold,
# MAGIC
# MAGIC     MAX(CASE WHEN rn = 2 THEN race_id END)        AS race_2_id,
# MAGIC     MAX(CASE WHEN rn = 2 THEN race_name END)      AS race_2_name,
# MAGIC     MAX(CASE WHEN rn = 2 THEN games_played END)   AS race_2_games_played,
# MAGIC     MAX(CASE WHEN rn = 2 THEN win_frac END)       AS race_2_win_frac,
# MAGIC
# MAGIC     MAX(CASE WHEN rn = 3 THEN race_id END)        AS race_3_id,
# MAGIC     MAX(CASE WHEN rn = 3 THEN race_name END)      AS race_3_name,
# MAGIC     MAX(CASE WHEN rn = 3 THEN games_played END)   AS race_3_games_played,
# MAGIC     MAX(CASE WHEN rn = 3 THEN win_frac END)       AS race_3_win_frac,
# MAGIC
# MAGIC     MAX(race_load_timestamp) AS race_load_timestamp
# MAGIC   FROM race_ranked
# MAGIC   WHERE rn <= 3
# MAGIC   GROUP BY coach_id
# MAGIC ),
# MAGIC
# MAGIC race_agg AS (
# MAGIC   SELECT
# MAGIC     coach_id,
# MAGIC     CAST(COUNT(DISTINCT race_id) AS INT) AS distinct_races_played,
# MAGIC     MAX(CASE WHEN is_valid_min_games_race_performance THEN win_frac END) AS best_race_win_frac,
# MAGIC     PERCENTILE_APPROX(points_per_game, 0.5) AS median_race_ppg
# MAGIC   FROM race_ranked
# MAGIC   WHERE games_played > 0
# MAGIC   GROUP BY coach_id
# MAGIC ),
# MAGIC
# MAGIC streak_global AS (
# MAGIC   SELECT
# MAGIC     coach_id,
# MAGIC     best_win_streak,
# MAGIC     best_unbeaten_streak,
# MAGIC     current_win_streak,
# MAGIC     current_unbeaten_streak,
# MAGIC     load_timestamp AS streak_load_timestamp
# MAGIC   FROM naf_catalog.gold_summary.coach_streak_summary
# MAGIC   WHERE scope = 'GLOBAL'
# MAGIC     AND scope_race_id = 0
# MAGIC ),
# MAGIC
# MAGIC glo_ranks AS (
# MAGIC   SELECT
# MAGIC     rg.coach_id,
# MAGIC     ci.nation_id,
# MAGIC     rg.global_elo_current,
# MAGIC     rg.global_elo_peak_last_50,
# MAGIC     DENSE_RANK() OVER (ORDER BY rg.global_elo_current DESC)   AS world_rank_current,
# MAGIC     DENSE_RANK() OVER (
# MAGIC       PARTITION BY ci.nation_id
# MAGIC       ORDER BY rg.global_elo_current DESC
# MAGIC     ) AS nation_rank_current,
# MAGIC     DENSE_RANK() OVER (ORDER BY rg.global_elo_peak_last_50 DESC) AS world_rank_peak,
# MAGIC     DENSE_RANK() OVER (
# MAGIC       PARTITION BY ci.nation_id
# MAGIC       ORDER BY rg.global_elo_peak_last_50 DESC
# MAGIC     ) AS nation_rank_peak
# MAGIC   FROM naf_catalog.gold_summary.coach_rating_global_elo_summary AS rg
# MAGIC   JOIN naf_catalog.gold_presentation.coach_identity_v AS ci
# MAGIC     ON rg.coach_id = ci.coach_id
# MAGIC   WHERE rg.rating_system = 'NAF_ELO'
# MAGIC     AND rg.global_elo_current IS NOT NULL
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   -- Identity
# MAGIC   ci.coach_id,
# MAGIC   ci.coach_name,
# MAGIC   ci.nation_id,
# MAGIC   ci.nation_name_display,
# MAGIC   ci.flag_emoji,
# MAGIC
# MAGIC   -- Global performance (all games universe)
# MAGIC   p.wins,
# MAGIC   p.draws,
# MAGIC   p.losses,
# MAGIC
# MAGIC   COALESCE(p.win_frac, 0.0)  AS win_frac,
# MAGIC   COALESCE(p.draw_frac, 0.0) AS draw_frac,
# MAGIC   COALESCE(p.loss_frac, 0.0) AS loss_frac,
# MAGIC   (100.0D * COALESCE(p.win_frac, 0.0))  AS win_pct,
# MAGIC   (100.0D * COALESCE(p.draw_frac, 0.0)) AS draw_pct,
# MAGIC   (100.0D * COALESCE(p.loss_frac, 0.0)) AS loss_pct,
# MAGIC
# MAGIC   p.points_total,
# MAGIC   p.points_per_game,
# MAGIC
# MAGIC   p.td_for,
# MAGIC   p.td_against,
# MAGIC   p.td_diff,
# MAGIC   p.td_for_per_game,
# MAGIC   p.td_against_per_game,
# MAGIC   p.td_diff_per_game,
# MAGIC
# MAGIC   p.tournaments_played,
# MAGIC   p.nations_played,
# MAGIC
# MAGIC   p.date_first_game_id,
# MAGIC   p.date_last_game_id,
# MAGIC   p.last_game_event_timestamp,
# MAGIC   YEAR(TO_DATE(CAST(p.date_first_game_id AS STRING), 'yyyyMMdd')) AS active_since_year,
# MAGIC   YEAR(TO_DATE(CAST(p.date_last_game_id AS STRING), 'yyyyMMdd')) AS last_active_year,
# MAGIC
# MAGIC   -- Global rating snapshot (rated games universe; aligned to summary contract)
# MAGIC   rg.global_elo_current,
# MAGIC   rg.global_elo_current_game_index,
# MAGIC   rg.global_elo_current_date_id,
# MAGIC   rg.global_elo_current_event_timestamp,
# MAGIC   rg.global_elo_current_game_id,
# MAGIC
# MAGIC   rg.global_elo_peak_all,
# MAGIC   rg.global_elo_peak_all_game_index,
# MAGIC   rg.global_elo_peak_all_date_id,
# MAGIC   rg.global_elo_peak_all_event_timestamp,
# MAGIC   rg.global_elo_peak_all_game_id,
# MAGIC   rg.global_elo_mean_all,
# MAGIC   rg.global_elo_median_all,
# MAGIC
# MAGIC   rg.global_elo_peak_post_threshold,
# MAGIC   rg.global_elo_peak_post_threshold_game_index,
# MAGIC   rg.global_elo_peak_post_threshold_date_id,
# MAGIC   rg.global_elo_peak_post_threshold_event_timestamp,
# MAGIC   rg.global_elo_peak_post_threshold_game_id,
# MAGIC   rg.global_elo_mean_post_threshold,
# MAGIC   rg.global_elo_median_post_threshold,
# MAGIC
# MAGIC   rg.global_elo_peak_last_50,
# MAGIC   rg.global_elo_peak_last_50_game_index,
# MAGIC   rg.global_elo_peak_last_50_date_id,
# MAGIC   rg.global_elo_peak_last_50_event_timestamp,
# MAGIC   rg.global_elo_peak_last_50_game_id,
# MAGIC   rg.global_elo_mean_last_50,
# MAGIC   rg.global_elo_median_last_50,
# MAGIC
# MAGIC   rg.threshold_games,
# MAGIC   rg.last_n_games_window,
# MAGIC
# MAGIC   -- Rating coverage (rated vs all games)
# MAGIC   COALESCE(p.games_played, 0)       AS games_played_all,
# MAGIC   COALESCE(rg.global_elo_games, 0)  AS games_played_rated,
# MAGIC   GREATEST(COALESCE(p.games_played, 0) - COALESCE(rg.global_elo_games, 0), 0) AS games_played_unrated,
# MAGIC   CASE
# MAGIC     WHEN COALESCE(p.games_played, 0) > 0
# MAGIC       THEN COALESCE(rg.global_elo_games, 0) / CAST(p.games_played AS DOUBLE)
# MAGIC     ELSE 0.0
# MAGIC   END AS rated_games_frac,
# MAGIC   CASE
# MAGIC     WHEN COALESCE(p.games_played, 0) > 0
# MAGIC       THEN 100.0D * COALESCE(rg.global_elo_games, 0) / CAST(p.games_played AS DOUBLE)
# MAGIC     ELSE 0.0
# MAGIC   END AS rated_games_pct,
# MAGIC
# MAGIC   -- GLO ranks (world and nation, current and peak)
# MAGIC   gr.world_rank_current,
# MAGIC   gr.nation_rank_current,
# MAGIC   gr.world_rank_peak,
# MAGIC   gr.nation_rank_peak,
# MAGIC
# MAGIC   -- Opponent strength (weighted mean of opponent median GLO)
# MAGIC   og.opponent_global_elo_median_weighted,
# MAGIC   og.opponents_count,
# MAGIC   og.opponent_games_weight,
# MAGIC   og.opponent_games_rated,
# MAGIC   og.opponent_games_imputed,
# MAGIC
# MAGIC   -- Opponent strength context (all games + last 50)
# MAGIC   p.avg_opponent_glo_peak,
# MAGIC   p.avg_opponent_glo_peak_last_50,
# MAGIC
# MAGIC   -- Form score (recent outperformance of Elo expectations)
# MAGIC   fs.form_score,
# MAGIC   fs.form_pctl,
# MAGIC   fs.form_label,
# MAGIC   fs.form_games_in_window,
# MAGIC
# MAGIC   -- Top races
# MAGIC   tr.race_1_id            AS main_race_id,
# MAGIC   tr.race_1_name          AS main_race_name,
# MAGIC   tr.race_1_games_played  AS main_race_games_played,
# MAGIC   COALESCE(tr.race_1_win_frac, 0.0) AS main_race_win_frac,
# MAGIC   (100.0D * COALESCE(tr.race_1_win_frac, 0.0)) AS main_race_win_pct,
# MAGIC   COALESCE(tr.race_1_points_per_game, 0.0) AS main_race_ppg,
# MAGIC   tr.race_1_is_valid_min_games AS main_race_is_valid_min_games,
# MAGIC   tr.race_1_min_games_threshold AS main_race_min_games_threshold,
# MAGIC
# MAGIC   tr.race_2_id,
# MAGIC   tr.race_2_name,
# MAGIC   tr.race_2_games_played,
# MAGIC   COALESCE(tr.race_2_win_frac, 0.0) AS race_2_win_frac,
# MAGIC   (100.0D * COALESCE(tr.race_2_win_frac, 0.0)) AS race_2_win_pct,
# MAGIC
# MAGIC   tr.race_3_id,
# MAGIC   tr.race_3_name,
# MAGIC   tr.race_3_games_played,
# MAGIC   COALESCE(tr.race_3_win_frac, 0.0) AS race_3_win_frac,
# MAGIC   (100.0D * COALESCE(tr.race_3_win_frac, 0.0)) AS race_3_win_pct,
# MAGIC
# MAGIC   -- Race aggregates
# MAGIC   COALESCE(ra.distinct_races_played, 0)            AS distinct_races_played,
# MAGIC   COALESCE(ra.best_race_win_frac, 0.0)             AS best_race_win_frac,
# MAGIC   (100.0D * COALESCE(ra.best_race_win_frac, 0.0))  AS best_race_win_pct,
# MAGIC   COALESCE(ra.median_race_ppg, 0.0)               AS median_race_ppg,
# MAGIC
# MAGIC   -- Streaks
# MAGIC   sg.best_win_streak,
# MAGIC   sg.best_unbeaten_streak,
# MAGIC   sg.current_win_streak,
# MAGIC   sg.current_unbeaten_streak,
# MAGIC
# MAGIC   -- Unified pipeline timestamp
# MAGIC   GREATEST(
# MAGIC     COALESCE(p.load_timestamp,  TIMESTAMP('1900-01-01')),
# MAGIC     COALESCE(rg.load_timestamp, TIMESTAMP('1900-01-01')),
# MAGIC     COALESCE(og.load_timestamp, TIMESTAMP('1900-01-01')),
# MAGIC     COALESCE(tr.race_load_timestamp, TIMESTAMP('1900-01-01')),
# MAGIC     COALESCE(sg.streak_load_timestamp, TIMESTAMP('1900-01-01')),
# MAGIC     COALESCE(fs.load_timestamp, TIMESTAMP('1900-01-01'))
# MAGIC   ) AS load_timestamp
# MAGIC
# MAGIC FROM naf_catalog.gold_presentation.coach_identity_v AS ci
# MAGIC LEFT JOIN naf_catalog.gold_summary.coach_performance_summary AS p
# MAGIC   ON ci.coach_id = p.coach_id
# MAGIC LEFT JOIN naf_catalog.gold_summary.coach_rating_global_elo_summary AS rg
# MAGIC   ON ci.coach_id = rg.coach_id
# MAGIC  AND rg.rating_system = 'NAF_ELO'
# MAGIC LEFT JOIN naf_catalog.gold_summary.coach_opponent_global_elo_mean_summary_v AS og
# MAGIC   ON ci.coach_id = og.coach_id
# MAGIC  AND og.rating_system = 'NAF_ELO'
# MAGIC LEFT JOIN top_races AS tr
# MAGIC   ON ci.coach_id = tr.coach_id
# MAGIC LEFT JOIN race_agg AS ra
# MAGIC   ON ci.coach_id = ra.coach_id
# MAGIC LEFT JOIN streak_global AS sg
# MAGIC   ON ci.coach_id = sg.coach_id
# MAGIC LEFT JOIN naf_catalog.gold_summary.coach_form_summary AS fs
# MAGIC   ON ci.coach_id = fs.coach_id
# MAGIC LEFT JOIN glo_ranks AS gr
# MAGIC   ON ci.coach_id = gr.coach_id
# MAGIC ;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- *VIEW*: naf_catalog.gold_presentation.coach_opponent_highlights
# MAGIC -- =====================================================================
# MAGIC -- LAYER        : GOLD_PRESENTATION
# MAGIC -- CONTRACT TYPE: Dashboard-facing, flattened (no nested structs in final output)
# MAGIC --
# MAGIC -- PURPOSE
# MAGIC --   One-row-per-coach opponent "highlights" used for Coach Dashboard tiles and exports:
# MAGIC --     1) Most played opponent
# MAGIC --     2) Highest-rated opponent (by opponent_global_elo_for_binning)
# MAGIC --     3) Highest-rated opponent that the coach has beaten (wins > 0)
# MAGIC --
# MAGIC -- GRAIN / KEYS
# MAGIC --   GRAIN       : (coach_id)
# MAGIC --   PRIMARY KEY : (coach_id)
# MAGIC --
# MAGIC -- SOURCES
# MAGIC --   - naf_catalog.gold_presentation.coach_profile (coach universe + load_timestamp)
# MAGIC --   - naf_catalog.gold_summary.coach_opponent_global_elo_enriched_v (opponent aggregates + opponent rating snapshots)
# MAGIC --   - naf_catalog.gold_presentation.coach_identity_v (opponent display identity)
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.coach_opponent_highlights AS
# MAGIC WITH base_coaches AS (
# MAGIC   SELECT coach_id, load_timestamp
# MAGIC   FROM naf_catalog.gold_presentation.coach_profile
# MAGIC ),
# MAGIC
# MAGIC opp_base AS (
# MAGIC   SELECT
# MAGIC     s.coach_id,
# MAGIC     s.opponent_coach_id,
# MAGIC
# MAGIC     oi.coach_name          AS opponent_coach_name,
# MAGIC     oi.nation_name_display AS opponent_nation_name_display,
# MAGIC     oi.flag_emoji           AS opponent_flag_emoji,
# MAGIC
# MAGIC     s.games_played,
# MAGIC     s.wins,
# MAGIC     s.draws,
# MAGIC     s.losses,
# MAGIC
# MAGIC     CASE
# MAGIC       WHEN s.games_played > 0 THEN COALESCE(s.wins, 0) / CAST(s.games_played AS DOUBLE)
# MAGIC       ELSE 0.0
# MAGIC     END AS win_frac,
# MAGIC
# MAGIC     CASE
# MAGIC       WHEN s.games_played > 0 THEN 100.0D * COALESCE(s.wins, 0) / CAST(s.games_played AS DOUBLE)
# MAGIC       ELSE 0.0
# MAGIC     END AS win_pct,
# MAGIC
# MAGIC     CAST(s.win_points AS DOUBLE) AS win_points,
# MAGIC
# MAGIC     CASE
# MAGIC       WHEN s.games_played > 0 THEN CAST(s.win_points AS DOUBLE) / CAST(s.games_played AS DOUBLE)
# MAGIC       ELSE 0.0
# MAGIC     END AS win_points_per_game,
# MAGIC
# MAGIC     CAST(s.opponent_global_elo_current     AS DOUBLE) AS opponent_global_elo_current,
# MAGIC     CAST(s.opponent_global_elo_peak_all    AS DOUBLE) AS opponent_global_elo_peak_all,
# MAGIC     CAST(s.opponent_global_elo_for_binning AS DOUBLE) AS opponent_global_elo_for_binning,
# MAGIC
# MAGIC     s.load_timestamp
# MAGIC   FROM naf_catalog.gold_summary.coach_opponent_global_elo_enriched_v AS s
# MAGIC   LEFT JOIN naf_catalog.gold_presentation.coach_identity_v AS oi
# MAGIC     ON s.opponent_coach_id = oi.coach_id
# MAGIC   WHERE s.rating_system = 'NAF_ELO'
# MAGIC     AND s.opponent_coach_id IS NOT NULL
# MAGIC     AND s.games_played > 0
# MAGIC ),
# MAGIC
# MAGIC picked AS (
# MAGIC   SELECT
# MAGIC     coach_id,
# MAGIC
# MAGIC     MAX_BY(
# MAGIC       NAMED_STRUCT(
# MAGIC         'opponent_coach_id', opponent_coach_id,
# MAGIC         'opponent_coach_name', opponent_coach_name,
# MAGIC         'opponent_nation_name_display', opponent_nation_name_display,
# MAGIC         'opponent_flag_emoji', opponent_flag_emoji,
# MAGIC         'games_played', games_played,
# MAGIC         'wins', wins,
# MAGIC         'draws', draws,
# MAGIC         'losses', losses,
# MAGIC         'win_frac', win_frac,
# MAGIC         'win_pct', win_pct,
# MAGIC         'win_points', win_points,
# MAGIC         'win_points_per_game', win_points_per_game,
# MAGIC         'opponent_global_elo_current', opponent_global_elo_current,
# MAGIC         'opponent_global_elo_peak_all', opponent_global_elo_peak_all,
# MAGIC         'opponent_global_elo_for_binning', opponent_global_elo_for_binning,
# MAGIC         'load_timestamp', load_timestamp
# MAGIC       ),
# MAGIC       NAMED_STRUCT(
# MAGIC         'games_played', games_played,
# MAGIC         'opponent_coach_id', opponent_coach_id
# MAGIC       )
# MAGIC     ) AS by_games,
# MAGIC
# MAGIC     MAX_BY(
# MAGIC       NAMED_STRUCT(
# MAGIC         'opponent_coach_id', opponent_coach_id,
# MAGIC         'opponent_coach_name', opponent_coach_name,
# MAGIC         'opponent_nation_name_display', opponent_nation_name_display,
# MAGIC         'opponent_flag_emoji', opponent_flag_emoji,
# MAGIC         'games_played', games_played,
# MAGIC         'wins', wins,
# MAGIC         'draws', draws,
# MAGIC         'losses', losses,
# MAGIC         'win_frac', win_frac,
# MAGIC         'win_pct', win_pct,
# MAGIC         'win_points', win_points,
# MAGIC         'win_points_per_game', win_points_per_game,
# MAGIC         'opponent_global_elo_current', opponent_global_elo_current,
# MAGIC         'opponent_global_elo_peak_all', opponent_global_elo_peak_all,
# MAGIC         'opponent_global_elo_for_binning', opponent_global_elo_for_binning,
# MAGIC         'load_timestamp', load_timestamp
# MAGIC       ),
# MAGIC       NAMED_STRUCT(
# MAGIC         'opponent_global_elo_for_binning', COALESCE(opponent_global_elo_for_binning, -1e18D),
# MAGIC         'opponent_coach_id', opponent_coach_id
# MAGIC       )
# MAGIC     ) AS by_rank,
# MAGIC
# MAGIC     MAX_BY(
# MAGIC       NAMED_STRUCT(
# MAGIC         'opponent_coach_id', opponent_coach_id,
# MAGIC         'opponent_coach_name', opponent_coach_name,
# MAGIC         'opponent_nation_name_display', opponent_nation_name_display,
# MAGIC         'opponent_flag_emoji', opponent_flag_emoji,
# MAGIC         'games_played', games_played,
# MAGIC         'wins', wins,
# MAGIC         'draws', draws,
# MAGIC         'losses', losses,
# MAGIC         'win_frac', win_frac,
# MAGIC         'win_pct', win_pct,
# MAGIC         'win_points', win_points,
# MAGIC         'win_points_per_game', win_points_per_game,
# MAGIC         'opponent_global_elo_current', opponent_global_elo_current,
# MAGIC         'opponent_global_elo_peak_all', opponent_global_elo_peak_all,
# MAGIC         'opponent_global_elo_for_binning', opponent_global_elo_for_binning,
# MAGIC         'load_timestamp', load_timestamp
# MAGIC       ),
# MAGIC       NAMED_STRUCT(
# MAGIC         'opponent_global_elo_for_binning', COALESCE(opponent_global_elo_for_binning, -1e18D),
# MAGIC         'opponent_coach_id', opponent_coach_id
# MAGIC       )
# MAGIC     ) FILTER (WHERE wins > 0) AS by_rank_beaten,
# MAGIC
# MAGIC     MAX(load_timestamp) AS opp_load_timestamp
# MAGIC   FROM opp_base
# MAGIC   GROUP BY coach_id
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   bc.coach_id,
# MAGIC
# MAGIC   p.by_games.opponent_coach_id            AS top_opp_by_games_id,
# MAGIC   p.by_games.opponent_coach_name          AS top_opp_by_games_name,
# MAGIC   p.by_games.opponent_nation_name_display AS top_opp_by_games_nation_name,
# MAGIC   p.by_games.opponent_flag_emoji           AS top_opp_by_games_flag_code,
# MAGIC   p.by_games.games_played                 AS top_opp_by_games_games,
# MAGIC   p.by_games.wins                         AS top_opp_by_games_wins,
# MAGIC   p.by_games.draws                        AS top_opp_by_games_draws,
# MAGIC   p.by_games.losses                       AS top_opp_by_games_losses,
# MAGIC   p.by_games.win_pct                      AS top_opp_by_games_win_pct,
# MAGIC   p.by_games.win_points                   AS top_opp_by_games_win_points,
# MAGIC   p.by_games.win_points_per_game          AS top_opp_by_games_win_points_per_game,
# MAGIC   p.by_games.opponent_global_elo_current  AS top_opp_by_games_global_elo_current,
# MAGIC   p.by_games.opponent_global_elo_peak_all AS top_opp_by_games_global_elo_peak_all,
# MAGIC
# MAGIC   p.by_rank.opponent_coach_id             AS top_opp_by_rank_id,
# MAGIC   p.by_rank.opponent_coach_name           AS top_opp_by_rank_name,
# MAGIC   p.by_rank.opponent_nation_name_display  AS top_opp_by_rank_nation_name,
# MAGIC   p.by_rank.opponent_flag_emoji            AS top_opp_by_rank_flag_code,
# MAGIC   p.by_rank.opponent_global_elo_current   AS top_opp_by_rank_global_elo_current,
# MAGIC   p.by_rank.opponent_global_elo_peak_all  AS top_opp_by_rank_global_elo_peak_all,
# MAGIC
# MAGIC   p.by_rank_beaten.opponent_coach_id            AS top_win_opponent_id,
# MAGIC   p.by_rank_beaten.opponent_coach_name          AS top_win_opponent_name,
# MAGIC   p.by_rank_beaten.opponent_nation_name_display AS top_win_opponent_nation_name,
# MAGIC   p.by_rank_beaten.opponent_flag_emoji           AS top_win_opponent_flag_emoji,
# MAGIC   p.by_rank_beaten.opponent_global_elo_current  AS top_win_opponent_global_elo_current,
# MAGIC   p.by_rank_beaten.opponent_global_elo_peak_all AS top_win_opponent_global_elo_peak_all,
# MAGIC
# MAGIC   GREATEST(
# MAGIC     COALESCE(bc.load_timestamp, TIMESTAMP('1900-01-01')),
# MAGIC     COALESCE(p.opp_load_timestamp, TIMESTAMP('1900-01-01'))
# MAGIC   ) AS load_timestamp
# MAGIC
# MAGIC FROM base_coaches bc
# MAGIC LEFT JOIN picked p
# MAGIC   ON bc.coach_id = p.coach_id
# MAGIC ;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- *VIEW*: naf_catalog.gold_presentation.coach_opponent_rating
# MAGIC -- =====================================================================
# MAGIC -- LAYER        : GOLD_PRESENTATION
# MAGIC -- CONTRACT TYPE: Dashboard-facing contract (coach vs opponent, flattened with display fields)
# MAGIC --
# MAGIC -- PURPOSE
# MAGIC --   Provide a reusable dashboard/export contract at the coach-opponent grain, including:
# MAGIC --     - coach and opponent display identity (name + nation display + flag)
# MAGIC --     - head-to-head W/D/L aggregates and derived fractions/percentages
# MAGIC --     - GLOBAL Elo rating exchange aggregates and "last played" metadata
# MAGIC --
# MAGIC -- GRAIN / KEYS
# MAGIC --   GRAIN       : (coach_id, opponent_coach_id)
# MAGIC --   PRIMARY KEY : (coach_id, opponent_coach_id)
# MAGIC --
# MAGIC -- SOURCES
# MAGIC --   - naf_catalog.gold_summary.coach_opponent_summary
# MAGIC --   - naf_catalog.gold_presentation.coach_identity_v (coach + opponent display identity)
# MAGIC --
# MAGIC -- SEMANTICS / FILTERS
# MAGIC --   - rating_system currently uses 'NAF_ELO' as a placeholder single-system label.
# MAGIC --   - The WHERE clause locks the contract to that system for stability.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.coach_opponent_rating AS
# MAGIC SELECT
# MAGIC   s.coach_id,
# MAGIC   ci.coach_name,
# MAGIC   ci.nation_id,
# MAGIC   ci.nation_name_display,
# MAGIC   ci.flag_emoji,
# MAGIC
# MAGIC   s.opponent_coach_id,
# MAGIC   oi.coach_name           AS opponent_coach_name,
# MAGIC   oi.nation_id            AS opponent_nation_id,
# MAGIC   oi.nation_name_display  AS opponent_nation_name_display,
# MAGIC   oi.flag_emoji            AS opponent_flag_emoji,
# MAGIC
# MAGIC   s.games_played,
# MAGIC   s.wins,
# MAGIC   s.draws,
# MAGIC   s.losses,
# MAGIC
# MAGIC   COALESCE(s.win_frac, 0.0)  AS win_frac,
# MAGIC   COALESCE(s.draw_frac, 0.0) AS draw_frac,
# MAGIC   COALESCE(s.loss_frac, 0.0) AS loss_frac,
# MAGIC
# MAGIC   (100.0D * COALESCE(s.win_frac, 0.0))   AS win_pct,
# MAGIC   (100.0D * COALESCE(s.draw_frac, 0.0))  AS draw_pct,
# MAGIC   (100.0D * COALESCE(s.loss_frac, 0.0))  AS loss_pct,
# MAGIC
# MAGIC   -- uplift: explicit "win points" from W/D/L (wins=1, draws=0.5)
# MAGIC   (COALESCE(s.wins, 0) * 1.0D + COALESCE(s.draws, 0) * 0.5D) AS win_points,
# MAGIC   CASE WHEN COALESCE(s.games_played, 0) > 0
# MAGIC        THEN (COALESCE(s.wins, 0) * 1.0D + COALESCE(s.draws, 0) * 0.5D) / CAST(s.games_played AS DOUBLE)
# MAGIC        ELSE 0.0
# MAGIC   END AS win_points_per_game,
# MAGIC
# MAGIC   -- rating exchange (GLOBAL Elo)
# MAGIC   s.global_elo_exchange_total,
# MAGIC   s.global_elo_exchange_mean,
# MAGIC
# MAGIC   -- last played (IDs + timestamp only)
# MAGIC   s.last_game_index,
# MAGIC   s.last_game_id,
# MAGIC   s.last_game_date_id,
# MAGIC   s.last_game_event_timestamp,
# MAGIC
# MAGIC   -- pipeline timestamp
# MAGIC   s.load_timestamp
# MAGIC
# MAGIC FROM naf_catalog.gold_summary.coach_opponent_summary AS s
# MAGIC LEFT JOIN naf_catalog.gold_presentation.coach_identity_v AS ci
# MAGIC   ON s.coach_id = ci.coach_id
# MAGIC LEFT JOIN naf_catalog.gold_presentation.coach_identity_v AS oi
# MAGIC   ON s.opponent_coach_id = oi.coach_id
# MAGIC WHERE s.rating_system = 'NAF_ELO';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- *VIEW*: naf_catalog.gold_presentation.coach_race_performance
# MAGIC -- =====================================================================
# MAGIC -- LAYER        : GOLD_PRESENTATION
# MAGIC -- CONTRACT TYPE: Dashboard-facing contract (coach x race, flattened with display fields)
# MAGIC --
# MAGIC -- PURPOSE
# MAGIC --   Provide a stable dashboard/export contract at the (coach_id, race_id) grain by combining:
# MAGIC --     - coach identity (name + nation display fields)
# MAGIC --     - race display (race name)
# MAGIC --     - per-race performance aggregates and derived fractions/percentages
# MAGIC --     - race-scoped Elo snapshot and phase distributions (all / post-threshold / last-50)
# MAGIC --
# MAGIC -- GRAIN / KEYS
# MAGIC --   GRAIN       : (coach_id, race_id)
# MAGIC --   PRIMARY KEY : (coach_id, race_id)
# MAGIC --
# MAGIC -- SOURCES
# MAGIC --   - naf_catalog.gold_summary.coach_race_summary
# MAGIC --   - naf_catalog.gold_presentation.coach_identity_v
# MAGIC --   - naf_catalog.gold_dim.race_dim
# MAGIC --
# MAGIC -- SEMANTICS / FILTERS
# MAGIC --   - rating_system is locked to 'NAF_ELO' to preserve the (coach_id, race_id) dashboard contract.
# MAGIC --
# MAGIC -- LOAD / FRESHNESS
# MAGIC --   - load_timestamp is inherited directly from gold_summary.coach_race_summary.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.coach_race_performance AS
# MAGIC SELECT
# MAGIC   -- coach display
# MAGIC   s.coach_id,
# MAGIC   ci.coach_name,
# MAGIC   ci.nation_id,
# MAGIC   ci.nation_name_display,
# MAGIC   ci.flag_emoji,
# MAGIC
# MAGIC   -- race display
# MAGIC   s.race_id,
# MAGIC   CASE
# MAGIC     WHEN s.race_id = 0 THEN 'All races'
# MAGIC     ELSE COALESCE(r.race_name, 'Unknown race')
# MAGIC   END AS race_name,
# MAGIC
# MAGIC   -- performance
# MAGIC   s.games_played,
# MAGIC   s.wins,
# MAGIC   s.draws,
# MAGIC   s.losses,
# MAGIC
# MAGIC   COALESCE(s.win_frac, 0.0)  AS win_frac,
# MAGIC   COALESCE(s.draw_frac, 0.0) AS draw_frac,
# MAGIC   COALESCE(s.loss_frac, 0.0) AS loss_frac,
# MAGIC   (100.0D * COALESCE(s.win_frac, 0.0))  AS win_pct,
# MAGIC   (100.0D * COALESCE(s.draw_frac, 0.0)) AS draw_pct,
# MAGIC   (100.0D * COALESCE(s.loss_frac, 0.0)) AS loss_pct,
# MAGIC
# MAGIC   s.points_total,
# MAGIC   s.points_per_game,
# MAGIC
# MAGIC   s.td_for,
# MAGIC   s.td_against,
# MAGIC   s.td_diff,
# MAGIC   s.td_for_per_game,
# MAGIC   s.td_against_per_game,
# MAGIC   s.td_diff_per_game,
# MAGIC
# MAGIC   -- IDs + timestamp metadata (no calendar joins)
# MAGIC   s.date_first_game_id,
# MAGIC   s.date_last_game_id,
# MAGIC   s.last_game_event_timestamp,
# MAGIC
# MAGIC   s.tournaments_played,
# MAGIC
# MAGIC   -- sample validity (race performance)
# MAGIC   s.min_games_threshold_race_performance,
# MAGIC   s.is_valid_min_games_race_performance,
# MAGIC
# MAGIC   -- Elo phase thresholds + flags
# MAGIC   s.elo_games_with_race,
# MAGIC   s.min_games_threshold_race_elo,
# MAGIC   s.last_n_games_window,
# MAGIC   s.is_valid_min_games_race_elo,
# MAGIC   s.is_valid_post_threshold_race_elo,
# MAGIC   s.is_valid_last_50_race_elo,
# MAGIC
# MAGIC   -- Elo current
# MAGIC   s.elo_current,
# MAGIC   s.elo_current_game_index,
# MAGIC   s.elo_current_date_id,
# MAGIC   s.elo_current_event_timestamp,
# MAGIC   s.elo_current_game_id,
# MAGIC
# MAGIC   -- Elo all-games distribution
# MAGIC   s.elo_peak_all,
# MAGIC   s.elo_peak_all_game_index,
# MAGIC   s.elo_peak_all_date_id,
# MAGIC   s.elo_peak_all_event_timestamp,
# MAGIC   s.elo_peak_all_game_id,
# MAGIC   s.elo_mean_all,
# MAGIC   s.elo_median_all,
# MAGIC
# MAGIC   -- Elo post-threshold
# MAGIC   s.elo_peak_post_threshold,
# MAGIC   s.elo_peak_post_threshold_game_index,
# MAGIC   s.elo_peak_post_threshold_date_id,
# MAGIC   s.elo_peak_post_threshold_event_timestamp,
# MAGIC   s.elo_peak_post_threshold_game_id,
# MAGIC   s.elo_mean_post_threshold,
# MAGIC   s.elo_median_post_threshold,
# MAGIC
# MAGIC   -- Elo last-50
# MAGIC   s.elo_peak_last_50,
# MAGIC   s.elo_peak_last_50_game_index,
# MAGIC   s.elo_peak_last_50_date_id,
# MAGIC   s.elo_peak_last_50_event_timestamp,
# MAGIC   s.elo_peak_last_50_game_id,
# MAGIC   s.elo_mean_last_50,
# MAGIC   s.elo_median_last_50,
# MAGIC
# MAGIC   -- unified timestamp
# MAGIC   s.load_timestamp
# MAGIC FROM naf_catalog.gold_summary.coach_race_summary AS s
# MAGIC LEFT JOIN naf_catalog.gold_presentation.coach_identity_v AS ci
# MAGIC   ON s.coach_id = ci.coach_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.race_dim AS r
# MAGIC   ON s.race_id = r.race_id
# MAGIC WHERE s.rating_system = 'NAF_ELO'
# MAGIC ;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- NOTE: global_elo_bin_scheme is defined in 340_gold_presentation_core.py (single source of truth).
# MAGIC -- Duplicate definition removed from this notebook (2026-02-20).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- VIEW: naf_catalog.gold_presentation.coach_opponent_global_elo_bin_insights
# MAGIC -- =====================================================================
# MAGIC -- LAYER        : GOLD_PRESENTATION
# MAGIC -- CONTRACT TYPE: Dashboard-facing opponent bin insights (GLOBAL Elo) — BINS ONLY
# MAGIC --
# MAGIC -- PURPOSE
# MAGIC --   Provide one row per coach + opponent-bin (no “Overall” rollup rows).
# MAGIC --   Enriched with coach identity fields and bin display labels.
# MAGIC --
# MAGIC -- GRAIN / KEYS
# MAGIC --   GRAIN       : (coach_id, bin_scheme_id, bin_index)
# MAGIC --   PRIMARY KEY : (coach_id, bin_scheme_id, bin_index)
# MAGIC --
# MAGIC -- SOURCES
# MAGIC --   - naf_catalog.gold_summary.coach_opponent_global_elo_bin_summary
# MAGIC --   - naf_catalog.gold_presentation.global_elo_bin_scheme
# MAGIC --   - naf_catalog.gold_presentation.coach_identity_v
# MAGIC --
# MAGIC -- SEMANTICS / FILTERS
# MAGIC --   - Locked to rating_system = 'NAF_ELO'.
# MAGIC --
# MAGIC -- NOTES
# MAGIC --   - “Overall” results are available separately (e.g., gold_summary.coach_opponent_global_elo_all_opponents_summary_v).
# MAGIC --   - Uses bin labels from gold_presentation.global_elo_bin_scheme for consistent UI naming.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.coach_opponent_global_elo_bin_insights AS
# MAGIC WITH scheme_bins AS (
# MAGIC   SELECT
# MAGIC     bin_scheme_id,
# MAGIC     bin_index,
# MAGIC     bin_min_global_elo AS bin_lower,
# MAGIC     bin_max_global_elo AS bin_upper,
# MAGIC     bin_scheme_name_display,
# MAGIC     bin_label_display
# MAGIC   FROM naf_catalog.gold_presentation.global_elo_bin_scheme
# MAGIC ),
# MAGIC bins AS (
# MAGIC   SELECT
# MAGIC     b.coach_id,
# MAGIC     b.rating_system,
# MAGIC     b.bin_scheme_id,
# MAGIC     b.bin_index,
# MAGIC
# MAGIC     sb.bin_lower,
# MAGIC     sb.bin_upper,
# MAGIC     sb.bin_label_display AS bin_label,
# MAGIC
# MAGIC     b.opponents_count,
# MAGIC     b.games_played,
# MAGIC     b.wins,
# MAGIC     b.draws,
# MAGIC     b.losses,
# MAGIC     b.win_points,
# MAGIC
# MAGIC     CASE WHEN COALESCE(b.games_played, 0) > 0
# MAGIC       THEN COALESCE(b.wins, 0) / CAST(b.games_played AS DOUBLE)
# MAGIC       ELSE 0.0
# MAGIC     END AS win_frac,
# MAGIC     CASE WHEN COALESCE(b.games_played, 0) > 0
# MAGIC       THEN COALESCE(b.draws, 0) / CAST(b.games_played AS DOUBLE)
# MAGIC       ELSE 0.0
# MAGIC     END AS draw_frac,
# MAGIC     CASE WHEN COALESCE(b.games_played, 0) > 0
# MAGIC       THEN COALESCE(b.losses, 0) / CAST(b.games_played AS DOUBLE)
# MAGIC       ELSE 0.0
# MAGIC     END AS loss_frac,
# MAGIC
# MAGIC     CASE WHEN COALESCE(b.games_played, 0) > 0
# MAGIC       THEN COALESCE(b.win_points, 0.0D) / CAST(b.games_played AS DOUBLE)
# MAGIC       ELSE 0.0
# MAGIC     END AS win_points_per_game,
# MAGIC
# MAGIC     b.load_timestamp
# MAGIC   FROM naf_catalog.gold_summary.coach_opponent_global_elo_bin_summary b
# MAGIC   INNER JOIN scheme_bins sb
# MAGIC     ON b.bin_scheme_id = sb.bin_scheme_id
# MAGIC    AND b.bin_index     = sb.bin_index
# MAGIC   WHERE b.rating_system = 'NAF_ELO'
# MAGIC )
# MAGIC SELECT
# MAGIC   x.coach_id,
# MAGIC   ci.coach_name,
# MAGIC   ci.nation_id,
# MAGIC   ci.nation_name_display,
# MAGIC   ci.flag_emoji,
# MAGIC
# MAGIC   x.bin_scheme_id,
# MAGIC   sb.bin_scheme_name_display,
# MAGIC
# MAGIC   x.bin_index,
# MAGIC   x.bin_lower,
# MAGIC   x.bin_upper,
# MAGIC   x.bin_label,
# MAGIC
# MAGIC   x.opponents_count,
# MAGIC   x.games_played,
# MAGIC   x.wins,
# MAGIC   x.draws,
# MAGIC   x.losses,
# MAGIC   x.win_points,
# MAGIC
# MAGIC   COALESCE(x.win_frac, 0.0)  AS win_frac,
# MAGIC   COALESCE(x.draw_frac, 0.0) AS draw_frac,
# MAGIC   COALESCE(x.loss_frac, 0.0) AS loss_frac,
# MAGIC
# MAGIC   (100.0D * COALESCE(x.win_frac, 0.0))  AS win_pct,
# MAGIC   (100.0D * COALESCE(x.draw_frac, 0.0)) AS draw_pct,
# MAGIC   (100.0D * COALESCE(x.loss_frac, 0.0)) AS loss_pct,
# MAGIC
# MAGIC   COALESCE(x.win_points_per_game, 0.0) AS win_points_per_game,
# MAGIC   x.load_timestamp
# MAGIC FROM bins x
# MAGIC LEFT JOIN naf_catalog.gold_presentation.coach_identity_v ci
# MAGIC   ON x.coach_id = ci.coach_id
# MAGIC LEFT JOIN (
# MAGIC   SELECT DISTINCT bin_scheme_id, bin_scheme_name_display
# MAGIC   FROM naf_catalog.gold_presentation.global_elo_bin_scheme
# MAGIC ) sb
# MAGIC   ON x.bin_scheme_id = sb.bin_scheme_id
# MAGIC ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Untitled
# MAGIC %sql
# MAGIC -- *VIEW*: naf_catalog.gold_presentation.coach_opponent_global_elo_bin_results_long
# MAGIC -- =====================================================================
# MAGIC -- LAYER        : GOLD_PRESENTATION
# MAGIC -- CONTRACT TYPE: Dashboard-facing long-format results (coach opponent bins)
# MAGIC --
# MAGIC -- PURPOSE
# MAGIC --   Expand each (coach_id, bin_scheme_id, bin row) into 3 rows (wins/draws/losses),
# MAGIC --   carrying both fractions and percentages for tidy charts.
# MAGIC --
# MAGIC -- GRAIN / KEYS
# MAGIC --   GRAIN       : (coach_id, bin_scheme_id, bin_index, result_order)
# MAGIC --   PRIMARY KEY : (coach_id, bin_scheme_id, bin_index, result_order)
# MAGIC --
# MAGIC -- SOURCES
# MAGIC --   - naf_catalog.gold_presentation.coach_opponent_global_elo_bin_insights
# MAGIC --
# MAGIC -- LOAD / FRESHNESS
# MAGIC --   - load_timestamp is inherited from the underlying bin insight rows.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.coach_opponent_global_elo_bin_results_long AS
# MAGIC WITH base AS (
# MAGIC   SELECT
# MAGIC     coach_id,
# MAGIC     coach_name,
# MAGIC     nation_id,
# MAGIC     nation_name_display,
# MAGIC     flag_emoji,
# MAGIC
# MAGIC     bin_scheme_id,
# MAGIC     bin_scheme_name_display,
# MAGIC
# MAGIC     bin_index,
# MAGIC     bin_lower,
# MAGIC     bin_upper,
# MAGIC     bin_label,
# MAGIC
# MAGIC     opponents_count,
# MAGIC     games_played,
# MAGIC     wins,
# MAGIC     draws,
# MAGIC     losses,
# MAGIC     win_points,
# MAGIC
# MAGIC     win_frac,
# MAGIC     draw_frac,
# MAGIC     loss_frac,
# MAGIC     win_pct,
# MAGIC     draw_pct,
# MAGIC     loss_pct,
# MAGIC
# MAGIC     win_points_per_game,
# MAGIC     load_timestamp
# MAGIC   FROM naf_catalog.gold_presentation.coach_opponent_global_elo_bin_insights
# MAGIC ),
# MAGIC
# MAGIC long AS (
# MAGIC   SELECT
# MAGIC     coach_id,
# MAGIC     coach_name,
# MAGIC     nation_id,
# MAGIC     nation_name_display,
# MAGIC     flag_emoji,
# MAGIC
# MAGIC     bin_scheme_id,
# MAGIC     bin_scheme_name_display,
# MAGIC     bin_index,
# MAGIC     bin_lower,
# MAGIC     bin_upper,
# MAGIC     bin_label,
# MAGIC     opponents_count,
# MAGIC     games_played,
# MAGIC     wins,
# MAGIC     draws,
# MAGIC     losses,
# MAGIC     win_points,
# MAGIC     win_points_per_game,
# MAGIC     load_timestamp,
# MAGIC
# MAGIC     'wins'    AS result,
# MAGIC     1         AS result_order,
# MAGIC     win_frac  AS result_frac,
# MAGIC     win_pct   AS result_pct
# MAGIC   FROM base
# MAGIC
# MAGIC   UNION ALL
# MAGIC
# MAGIC   SELECT
# MAGIC     coach_id,
# MAGIC     coach_name,
# MAGIC     nation_id,
# MAGIC     nation_name_display,
# MAGIC     flag_emoji,
# MAGIC
# MAGIC     bin_scheme_id,
# MAGIC     bin_scheme_name_display,
# MAGIC     bin_index,
# MAGIC     bin_lower,
# MAGIC     bin_upper,
# MAGIC     bin_label,
# MAGIC     opponents_count,
# MAGIC     games_played,
# MAGIC     wins,
# MAGIC     draws,
# MAGIC     losses,
# MAGIC     win_points,
# MAGIC     win_points_per_game,
# MAGIC     load_timestamp,
# MAGIC
# MAGIC     'draws'    AS result,
# MAGIC     2          AS result_order,
# MAGIC     draw_frac  AS result_frac,
# MAGIC     draw_pct   AS result_pct
# MAGIC   FROM base
# MAGIC
# MAGIC   UNION ALL
# MAGIC
# MAGIC   SELECT
# MAGIC     coach_id,
# MAGIC     coach_name,
# MAGIC     nation_id,
# MAGIC     nation_name_display,
# MAGIC     flag_emoji,
# MAGIC
# MAGIC     bin_scheme_id,
# MAGIC     bin_scheme_name_display,
# MAGIC     bin_index,
# MAGIC     bin_lower,
# MAGIC     bin_upper,
# MAGIC     bin_label,
# MAGIC     opponents_count,
# MAGIC     games_played,
# MAGIC     wins,
# MAGIC     draws,
# MAGIC     losses,
# MAGIC     win_points,
# MAGIC     win_points_per_game,
# MAGIC     load_timestamp,
# MAGIC
# MAGIC     'losses'   AS result,
# MAGIC     3          AS result_order,
# MAGIC     loss_frac  AS result_frac,
# MAGIC     loss_pct   AS result_pct
# MAGIC   FROM base
# MAGIC )
# MAGIC
# MAGIC SELECT * FROM long
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- *VIEW*: naf_catalog.gold_presentation.coach_cumulative_results_daily_series
# MAGIC -- =====================================================================
# MAGIC -- LAYER        : GOLD_PRESENTATION
# MAGIC -- PURPOSE      : Dashboard-facing daily cumulative W/D/L (spike-proof, exactly 1 value per day).
# MAGIC --
# MAGIC -- GRAIN / KEYS
# MAGIC --   GRAIN       : (scope, race_id, coach_id, event_day, result)
# MAGIC --   PRIMARY KEY : (scope, race_id, coach_id, event_day, result_order)
# MAGIC --
# MAGIC -- SOURCE
# MAGIC --   - naf_catalog.gold_summary.coach_results_cumulative_series_v
# MAGIC --   - naf_catalog.gold_dim.coach_dim (display)
# MAGIC --
# MAGIC -- NOTES
# MAGIC --   - Uses coach_results_cumulative_series_v end-of-day flags (is_end_of_day_row) + *_latest fields.
# MAGIC --   - race_id here is the scope context: 0 for GLOBAL, playable race_id for RACE (from scope_race_id).
# MAGIC --   - No rating_system here (results-universe series).
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.coach_cumulative_results_daily_series AS
# MAGIC WITH eod AS (
# MAGIC   SELECT
# MAGIC     c.scope,
# MAGIC     c.scope_race_id AS race_id,
# MAGIC     c.coach_id,
# MAGIC     TO_DATE(TO_TIMESTAMP(CAST(c.date_id AS STRING), 'yyyyMMdd')) AS event_day,
# MAGIC     c.cum_win_latest  AS cum_win,
# MAGIC     c.cum_draw_latest AS cum_draw,
# MAGIC     c.cum_loss_latest AS cum_loss
# MAGIC   FROM naf_catalog.gold_summary.coach_results_cumulative_series_v c
# MAGIC   WHERE c.is_end_of_day_row = TRUE
# MAGIC ),
# MAGIC long AS (
# MAGIC   SELECT
# MAGIC     scope,
# MAGIC     race_id,
# MAGIC     coach_id,
# MAGIC     event_day,
# MAGIC     result,
# MAGIC     result_order,
# MAGIC     CAST(value AS BIGINT) AS value
# MAGIC   FROM eod
# MAGIC   LATERAL VIEW STACK(
# MAGIC     3,
# MAGIC     'win',  cum_win,  1,
# MAGIC     'draw', cum_draw, 2,
# MAGIC     'loss', cum_loss, 3
# MAGIC   ) s AS result, value, result_order
# MAGIC )
# MAGIC SELECT
# MAGIC   l.scope,
# MAGIC   l.race_id,
# MAGIC   l.coach_id,
# MAGIC   cd.coach_name,
# MAGIC   l.event_day,
# MAGIC   l.result,
# MAGIC   l.result_order,
# MAGIC   l.value,
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM long l
# MAGIC LEFT JOIN naf_catalog.gold_dim.coach_dim cd
# MAGIC   ON l.coach_id = cd.coach_id
# MAGIC ;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- *VIEW*: naf_catalog.gold_presentation.coach_results_all_long
# MAGIC -- =====================================================================
# MAGIC -- LAYER        : GOLD_PRESENTATION
# MAGIC -- CONTRACT TYPE: Dashboard-facing long-format results (all games universe)
# MAGIC --
# MAGIC -- PURPOSE
# MAGIC --   Provide a long-format W/D/L dataset for “all games” results at coach grain.
# MAGIC --   Intended for stacked bars and simple result breakdown visuals.
# MAGIC --
# MAGIC -- GRAIN / KEYS
# MAGIC --   GRAIN       : (coach_id, level, result)
# MAGIC --   PRIMARY KEY : (coach_id, level, result_order)
# MAGIC --
# MAGIC -- SOURCES
# MAGIC --   - naf_catalog.gold_summary.coach_performance_summary   (all games universe)
# MAGIC --   - naf_catalog.gold_presentation.coach_identity_v       (display fields / coach universe)
# MAGIC --
# MAGIC -- DASHBOARD CONTRACT NOTES
# MAGIC --   - level is fixed to 'all' to make it easy to UNION with other levels later (eg 'rated').
# MAGIC --   - games_played_all is included to support coverage-aware visuals and labels.
# MAGIC --
# MAGIC -- LOAD / FRESHNESS
# MAGIC --   - load_timestamp is inherited from gold_summary.coach_performance_summary (NULL if no games).
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.coach_results_all_long AS
# MAGIC WITH base AS (
# MAGIC   SELECT
# MAGIC     ci.coach_id,
# MAGIC     ci.coach_name,
# MAGIC     ci.nation_id,
# MAGIC     ci.nation_name_display,
# MAGIC     ci.flag_emoji,
# MAGIC
# MAGIC     CAST(COALESCE(s.wins, 0)   AS BIGINT) AS wins,
# MAGIC     CAST(COALESCE(s.draws, 0)  AS BIGINT) AS draws,
# MAGIC     CAST(COALESCE(s.losses, 0) AS BIGINT) AS losses,
# MAGIC     CAST(COALESCE(s.games_played, 0) AS BIGINT) AS games_played_all,
# MAGIC
# MAGIC     s.load_timestamp
# MAGIC   FROM naf_catalog.gold_presentation.coach_identity_v AS ci
# MAGIC   LEFT JOIN naf_catalog.gold_summary.coach_performance_summary AS s
# MAGIC     ON ci.coach_id = s.coach_id
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   coach_id,
# MAGIC   coach_name,
# MAGIC   nation_id,
# MAGIC   nation_name_display,
# MAGIC   flag_emoji,
# MAGIC   games_played_all,
# MAGIC   'all' AS level,
# MAGIC   result,
# MAGIC   result_order,
# MAGIC   value,
# MAGIC   load_timestamp
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     STACK(
# MAGIC       3,
# MAGIC       'win',  1, wins,
# MAGIC       'draw', 2, draws,
# MAGIC       'loss', 3, losses
# MAGIC     ) AS (result, result_order, value)
# MAGIC   FROM base
# MAGIC ) x;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- *VIEW*: naf_catalog.gold_presentation.coach_opponent_top5_by_games
# MAGIC -- =====================================================================
# MAGIC -- LAYER        : GOLD_PRESENTATION
# MAGIC -- CONTRACT TYPE: Dashboard-facing top-N opponents (by games played)
# MAGIC --
# MAGIC -- PURPOSE
# MAGIC --   Provide the top 5 opponents per coach ranked by head-to-head games played,
# MAGIC --   enriched with opponent identity fields and opponent current GLOBAL Elo snapshot.
# MAGIC --
# MAGIC -- GRAIN / KEYS
# MAGIC --   GRAIN       : (coach_id, opponent_coach_id)
# MAGIC --   PRIMARY KEY : (coach_id, rivalry_rank_by_games)
# MAGIC --
# MAGIC -- SOURCES
# MAGIC --   - naf_catalog.gold_presentation.coach_opponent_rating   (coach x opponent aggregates + display)
# MAGIC --   - naf_catalog.gold_presentation.coach_profile           (opponent current rating snapshot + freshness)
# MAGIC --
# MAGIC -- DASHBOARD CONTRACT NOTES
# MAGIC --   - rivalry_rank_by_games provides stable ordering for visuals and labels.
# MAGIC --   - win_pct is COALESCE'd to 0.0 to ensure stable rendering.
# MAGIC --   - Rating fields may be NULL when opponent has no rated games (expected by design).
# MAGIC --
# MAGIC -- LOAD / FRESHNESS
# MAGIC --   - load_timestamp is the GREATEST of opponent-rating aggregate freshness and opponent profile freshness.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.coach_opponent_top5_by_games AS
# MAGIC WITH ranked AS (
# MAGIC   SELECT
# MAGIC     o.coach_id,
# MAGIC     o.opponent_coach_id,
# MAGIC
# MAGIC     o.coach_name,
# MAGIC     o.nation_id AS coach_nation_id,
# MAGIC     o.nation_name_display AS coach_nation_name_display,
# MAGIC     o.flag_emoji AS coach_flag_emoji,
# MAGIC
# MAGIC     o.opponent_coach_name,
# MAGIC     o.opponent_nation_id,
# MAGIC     o.opponent_nation_name_display,
# MAGIC     o.opponent_flag_emoji,
# MAGIC
# MAGIC     o.games_played,
# MAGIC     o.wins,
# MAGIC     o.draws,
# MAGIC     o.losses,
# MAGIC     COALESCE(o.win_pct, 0.0) AS win_pct,
# MAGIC
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY o.coach_id
# MAGIC       ORDER BY o.games_played DESC, o.opponent_coach_id ASC
# MAGIC     ) AS rivalry_rank_by_games,
# MAGIC
# MAGIC     o.load_timestamp
# MAGIC   FROM naf_catalog.gold_presentation.coach_opponent_rating AS o
# MAGIC   WHERE COALESCE(o.games_played, 0) > 0
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   r.coach_id,
# MAGIC   r.opponent_coach_id,
# MAGIC
# MAGIC   r.coach_name,
# MAGIC   r.coach_nation_id,
# MAGIC   r.coach_nation_name_display,
# MAGIC   r.coach_flag_emoji,
# MAGIC
# MAGIC   r.opponent_coach_name,
# MAGIC   r.opponent_nation_id,
# MAGIC   r.opponent_nation_name_display,
# MAGIC   r.opponent_flag_emoji,
# MAGIC
# MAGIC   r.games_played,
# MAGIC   r.wins,
# MAGIC   r.draws,
# MAGIC   r.losses,
# MAGIC   r.win_pct,
# MAGIC
# MAGIC   op.global_elo_current AS opponent_global_elo_current,
# MAGIC
# MAGIC   r.rivalry_rank_by_games,
# MAGIC
# MAGIC   GREATEST(
# MAGIC     COALESCE(r.load_timestamp,  TIMESTAMP('1900-01-01')),
# MAGIC     COALESCE(op.load_timestamp, TIMESTAMP('1900-01-01'))
# MAGIC   ) AS load_timestamp
# MAGIC
# MAGIC FROM ranked AS r
# MAGIC LEFT JOIN naf_catalog.gold_presentation.coach_profile AS op
# MAGIC   ON r.opponent_coach_id = op.coach_id
# MAGIC WHERE r.rivalry_rank_by_games <= 5;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- *VIEW*: naf_catalog.gold_presentation.coach_opponent_insights
# MAGIC -- =====================================================================
# MAGIC -- LAYER        : GOLD_PRESENTATION
# MAGIC -- CONTRACT TYPE: Dashboard-facing opponent insights (top rivals + highlight opponents)
# MAGIC --
# MAGIC -- PURPOSE
# MAGIC --   Provide a single, ordered dataset of opponent insights per coach:
# MAGIC --     1) Top 5 rivals (by games played)
# MAGIC --     2) "Big opponents" highlights:
# MAGIC --        - most played opponent
# MAGIC --        - highest-rated opponent overall (by GLOBAL Elo snapshot)
# MAGIC --        - highest-rated opponent beaten (wins > 0)
# MAGIC --
# MAGIC -- SOURCES
# MAGIC --   - naf_catalog.gold_presentation.coach_opponent_rating
# MAGIC --   - naf_catalog.gold_summary.coach_rating_global_elo_summary
# MAGIC --
# MAGIC -- SEMANTICS / FILTERS
# MAGIC --   - Locks rating snapshot to rating_system = 'NAF_ELO'.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.coach_opponent_insights AS
# MAGIC WITH opp_base AS (
# MAGIC   SELECT
# MAGIC     o.coach_id,
# MAGIC     o.opponent_coach_id,
# MAGIC     o.opponent_coach_name,
# MAGIC     o.opponent_nation_name_display,
# MAGIC     o.opponent_flag_emoji,
# MAGIC
# MAGIC     CAST(o.games_played AS BIGINT) AS games_played,
# MAGIC     CAST(o.wins        AS BIGINT) AS wins,
# MAGIC     CAST(o.draws       AS BIGINT) AS draws,
# MAGIC     CAST(o.losses      AS BIGINT) AS losses,
# MAGIC
# MAGIC     COALESCE(CAST(o.win_pct AS DOUBLE), 0.0) AS win_pct,
# MAGIC
# MAGIC     CAST(o.win_points AS DOUBLE) AS win_points,
# MAGIC     CAST(o.win_points_per_game AS DOUBLE) AS win_points_per_game,
# MAGIC
# MAGIC     o.load_timestamp
# MAGIC   FROM naf_catalog.gold_presentation.coach_opponent_rating AS o
# MAGIC   WHERE COALESCE(o.games_played, 0) > 0
# MAGIC ),
# MAGIC
# MAGIC opp_global_elo AS (
# MAGIC   SELECT
# MAGIC     coach_id,
# MAGIC     CAST(global_elo_current  AS DOUBLE) AS global_elo_current,
# MAGIC     CAST(global_elo_peak_all AS DOUBLE) AS global_elo_peak_all,
# MAGIC     CAST(COALESCE(global_elo_current, global_elo_peak_all) AS DOUBLE) AS global_elo_current_or_peak_all,
# MAGIC     load_timestamp
# MAGIC   FROM naf_catalog.gold_summary.coach_rating_global_elo_summary
# MAGIC   WHERE rating_system = 'NAF_ELO'
# MAGIC ),
# MAGIC
# MAGIC opp_enriched AS (
# MAGIC   SELECT
# MAGIC     b.*,
# MAGIC     g.global_elo_current,
# MAGIC     g.global_elo_peak_all,
# MAGIC     g.global_elo_current_or_peak_all,
# MAGIC     GREATEST(
# MAGIC       COALESCE(b.load_timestamp, TIMESTAMP('1900-01-01')),
# MAGIC       COALESCE(g.load_timestamp, TIMESTAMP('1900-01-01'))
# MAGIC     ) AS load_timestamp_enriched
# MAGIC   FROM opp_base AS b
# MAGIC   LEFT JOIN opp_global_elo AS g
# MAGIC     ON b.opponent_coach_id = g.coach_id
# MAGIC ),
# MAGIC
# MAGIC top5 AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY coach_id
# MAGIC       ORDER BY games_played DESC, opponent_coach_id ASC
# MAGIC     ) AS rivalry_rank
# MAGIC   FROM opp_enriched
# MAGIC ),
# MAGIC
# MAGIC big_played AS (
# MAGIC   SELECT
# MAGIC     coach_id,
# MAGIC     MAX_BY(
# MAGIC       NAMED_STRUCT(
# MAGIC         'opponent_coach_id', opponent_coach_id,
# MAGIC         'opponent_coach_name', opponent_coach_name,
# MAGIC         'opponent_nation_name_display', opponent_nation_name_display,
# MAGIC         'opponent_flag_emoji', opponent_flag_emoji,
# MAGIC         'games_played', games_played,
# MAGIC         'wins', wins,
# MAGIC         'draws', draws,
# MAGIC         'losses', losses,
# MAGIC         'global_elo_current_or_peak_all', global_elo_current_or_peak_all,
# MAGIC         'global_elo_current', global_elo_current,
# MAGIC         'global_elo_peak_all', global_elo_peak_all,
# MAGIC         'win_pct', win_pct,
# MAGIC         'win_points', win_points,
# MAGIC         'win_points_per_game', win_points_per_game,
# MAGIC         'load_timestamp', load_timestamp_enriched
# MAGIC       ),
# MAGIC       NAMED_STRUCT('games_played', games_played, 'opponent_coach_id', opponent_coach_id)
# MAGIC     ) AS s
# MAGIC   FROM opp_enriched
# MAGIC   GROUP BY coach_id
# MAGIC ),
# MAGIC
# MAGIC big_rank AS (
# MAGIC   SELECT
# MAGIC     coach_id,
# MAGIC     MAX_BY(
# MAGIC       NAMED_STRUCT(
# MAGIC         'opponent_coach_id', opponent_coach_id,
# MAGIC         'opponent_coach_name', opponent_coach_name,
# MAGIC         'opponent_nation_name_display', opponent_nation_name_display,
# MAGIC         'opponent_flag_emoji', opponent_flag_emoji,
# MAGIC         'games_played', games_played,
# MAGIC         'wins', wins,
# MAGIC         'draws', draws,
# MAGIC         'losses', losses,
# MAGIC         'global_elo_current_or_peak_all', global_elo_current_or_peak_all,
# MAGIC         'global_elo_current', global_elo_current,
# MAGIC         'global_elo_peak_all', global_elo_peak_all,
# MAGIC         'win_pct', win_pct,
# MAGIC         'win_points', win_points,
# MAGIC         'win_points_per_game', win_points_per_game,
# MAGIC         'load_timestamp', load_timestamp_enriched
# MAGIC       ),
# MAGIC       NAMED_STRUCT(
# MAGIC         'global_elo_current_or_peak_all', COALESCE(global_elo_current_or_peak_all, -1e18D),
# MAGIC         'opponent_coach_id', opponent_coach_id
# MAGIC       )
# MAGIC     ) AS s
# MAGIC   FROM opp_enriched
# MAGIC   GROUP BY coach_id
# MAGIC ),
# MAGIC
# MAGIC big_beaten AS (
# MAGIC   SELECT
# MAGIC     coach_id,
# MAGIC     MAX_BY(
# MAGIC       NAMED_STRUCT(
# MAGIC         'opponent_coach_id', opponent_coach_id,
# MAGIC         'opponent_coach_name', opponent_coach_name,
# MAGIC         'opponent_nation_name_display', opponent_nation_name_display,
# MAGIC         'opponent_flag_emoji', opponent_flag_emoji,
# MAGIC         'games_played', games_played,
# MAGIC         'wins', wins,
# MAGIC         'draws', draws,
# MAGIC         'losses', losses,
# MAGIC         'global_elo_current_or_peak_all', global_elo_current_or_peak_all,
# MAGIC         'global_elo_current', global_elo_current,
# MAGIC         'global_elo_peak_all', global_elo_peak_all,
# MAGIC         'win_pct', win_pct,
# MAGIC         'win_points', win_points,
# MAGIC         'win_points_per_game', win_points_per_game,
# MAGIC         'load_timestamp', load_timestamp_enriched
# MAGIC       ),
# MAGIC       NAMED_STRUCT(
# MAGIC         'global_elo_current_or_peak_all', COALESCE(global_elo_current_or_peak_all, -1e18D),
# MAGIC         'opponent_coach_id', opponent_coach_id
# MAGIC       )
# MAGIC     ) AS s
# MAGIC   FROM opp_enriched
# MAGIC   WHERE COALESCE(wins, 0) > 0
# MAGIC   GROUP BY coach_id
# MAGIC ),
# MAGIC
# MAGIC -- Nemesis: opponent with worst record against (most losses minus wins)
# MAGIC nemesis AS (
# MAGIC   SELECT
# MAGIC     coach_id,
# MAGIC     MAX_BY(
# MAGIC       NAMED_STRUCT(
# MAGIC         'opponent_coach_id', opponent_coach_id,
# MAGIC         'opponent_coach_name', opponent_coach_name,
# MAGIC         'opponent_nation_name_display', opponent_nation_name_display,
# MAGIC         'opponent_flag_emoji', opponent_flag_emoji,
# MAGIC         'games_played', games_played,
# MAGIC         'wins', wins,
# MAGIC         'draws', draws,
# MAGIC         'losses', losses,
# MAGIC         'global_elo_current_or_peak_all', global_elo_current_or_peak_all,
# MAGIC         'global_elo_current', global_elo_current,
# MAGIC         'global_elo_peak_all', global_elo_peak_all,
# MAGIC         'win_pct', win_pct,
# MAGIC         'win_points', win_points,
# MAGIC         'win_points_per_game', win_points_per_game,
# MAGIC         'load_timestamp', load_timestamp_enriched
# MAGIC       ),
# MAGIC       NAMED_STRUCT(
# MAGIC         'deficit', CAST(losses - wins AS BIGINT),
# MAGIC         'games_played', games_played,
# MAGIC         'opponent_coach_id', opponent_coach_id
# MAGIC       )
# MAGIC     ) AS s
# MAGIC   FROM opp_enriched
# MAGIC   WHERE COALESCE(losses, 0) > COALESCE(wins, 0)
# MAGIC   GROUP BY coach_id
# MAGIC ),
# MAGIC
# MAGIC -- Biggest upsets: from rating_history_fact (has opponent_rating_before)
# MAGIC upset_base AS (
# MAGIC   SELECT
# MAGIC     f.coach_id,
# MAGIC     f.opponent_coach_id,
# MAGIC     f.game_id,
# MAGIC     f.date_id,
# MAGIC     f.result_numeric,
# MAGIC     f.rating_before,
# MAGIC     f.opponent_rating_before,
# MAGIC     (f.opponent_rating_before - f.rating_before) AS rating_gap
# MAGIC   FROM naf_catalog.gold_fact.rating_history_fact f
# MAGIC   WHERE f.scope = 'GLOBAL'
# MAGIC     AND f.rating_system = 'NAF_ELO'
# MAGIC     AND f.opponent_rating_before IS NOT NULL
# MAGIC ),
# MAGIC
# MAGIC -- Biggest upset done: coach won against higher-rated opponent (max positive rating_gap where win)
# MAGIC biggest_upset_done AS (
# MAGIC   SELECT
# MAGIC     u.coach_id,
# MAGIC     MAX_BY(
# MAGIC       NAMED_STRUCT(
# MAGIC         'opponent_coach_id', u.opponent_coach_id,
# MAGIC         'game_id', u.game_id,
# MAGIC         'date_id', u.date_id,
# MAGIC         'coach_rating', u.rating_before,
# MAGIC         'opponent_rating', u.opponent_rating_before,
# MAGIC         'rating_gap', u.rating_gap
# MAGIC       ),
# MAGIC       u.rating_gap
# MAGIC     ) AS s
# MAGIC   FROM upset_base u
# MAGIC   WHERE u.result_numeric = 1.0 AND u.rating_gap > 0
# MAGIC   GROUP BY u.coach_id
# MAGIC ),
# MAGIC
# MAGIC -- Biggest upset received: coach lost to lower-rated opponent (max negative rating_gap where loss)
# MAGIC biggest_upset_received AS (
# MAGIC   SELECT
# MAGIC     u.coach_id,
# MAGIC     MAX_BY(
# MAGIC       NAMED_STRUCT(
# MAGIC         'opponent_coach_id', u.opponent_coach_id,
# MAGIC         'game_id', u.game_id,
# MAGIC         'date_id', u.date_id,
# MAGIC         'coach_rating', u.rating_before,
# MAGIC         'opponent_rating', u.opponent_rating_before,
# MAGIC         'rating_gap', u.rating_gap
# MAGIC       ),
# MAGIC       -u.rating_gap   -- largest gap where coach was higher rated but lost
# MAGIC     ) AS s
# MAGIC   FROM upset_base u
# MAGIC   WHERE u.result_numeric = 0.0 AND u.rating_gap < 0
# MAGIC   GROUP BY u.coach_id
# MAGIC ),
# MAGIC
# MAGIC rows AS (
# MAGIC   -- 1) Top 5 rivals
# MAGIC   SELECT
# MAGIC     coach_id,
# MAGIC     1 AS group_order,
# MAGIC     rivalry_rank AS row_order,
# MAGIC     CONCAT('Top 5 rivals: #', CAST(rivalry_rank AS STRING)) AS category,
# MAGIC
# MAGIC     opponent_coach_id,
# MAGIC     opponent_coach_name,
# MAGIC     opponent_nation_name_display,
# MAGIC     opponent_flag_emoji,
# MAGIC
# MAGIC     games_played,
# MAGIC     wins,
# MAGIC     draws,
# MAGIC     losses,
# MAGIC
# MAGIC     global_elo_current_or_peak_all,
# MAGIC     global_elo_current,
# MAGIC     global_elo_peak_all,
# MAGIC
# MAGIC     ROUND(COALESCE(win_pct, 0.0), 1) AS win_pct,
# MAGIC     win_points,
# MAGIC     win_points_per_game,
# MAGIC
# MAGIC     load_timestamp_enriched AS load_timestamp,
# MAGIC     CAST(NULL AS DOUBLE) AS rating_gap,
# MAGIC     CAST(NULL AS DOUBLE) AS coach_rating_at_game,
# MAGIC     CAST(NULL AS DOUBLE) AS opponent_rating_at_game
# MAGIC   FROM top5
# MAGIC   WHERE rivalry_rank <= 5
# MAGIC
# MAGIC   UNION ALL
# MAGIC
# MAGIC   -- 2) Big opponents: highest-rated overall
# MAGIC   SELECT
# MAGIC     coach_id, 2, 2,
# MAGIC     'Big opponents: top opponent by rank',
# MAGIC     s.opponent_coach_id,
# MAGIC     s.opponent_coach_name,
# MAGIC     s.opponent_nation_name_display,
# MAGIC     s.opponent_flag_emoji,
# MAGIC     s.games_played,
# MAGIC     s.wins,
# MAGIC     s.draws,
# MAGIC     s.losses,
# MAGIC     s.global_elo_current_or_peak_all,
# MAGIC     s.global_elo_current,
# MAGIC     s.global_elo_peak_all,
# MAGIC     ROUND(COALESCE(s.win_pct, 0.0), 1),
# MAGIC     s.win_points,
# MAGIC     s.win_points_per_game,
# MAGIC     s.load_timestamp,
# MAGIC     CAST(NULL AS DOUBLE),
# MAGIC     CAST(NULL AS DOUBLE),
# MAGIC     CAST(NULL AS DOUBLE)
# MAGIC   FROM big_rank
# MAGIC
# MAGIC   UNION ALL
# MAGIC
# MAGIC   -- 2) Big opponents: highest-rated beaten
# MAGIC   SELECT
# MAGIC     coach_id, 2, 3,
# MAGIC     'Big opponents: top opponent beaten',
# MAGIC     s.opponent_coach_id,
# MAGIC     s.opponent_coach_name,
# MAGIC     s.opponent_nation_name_display,
# MAGIC     s.opponent_flag_emoji,
# MAGIC     s.games_played,
# MAGIC     s.wins,
# MAGIC     s.draws,
# MAGIC     s.losses,
# MAGIC     s.global_elo_current_or_peak_all,
# MAGIC     s.global_elo_current,
# MAGIC     s.global_elo_peak_all,
# MAGIC     ROUND(COALESCE(s.win_pct, 0.0), 1),
# MAGIC     s.win_points,
# MAGIC     s.win_points_per_game,
# MAGIC     s.load_timestamp,
# MAGIC     CAST(NULL AS DOUBLE) AS rating_gap,
# MAGIC     CAST(NULL AS DOUBLE) AS coach_rating_at_game,
# MAGIC     CAST(NULL AS DOUBLE) AS opponent_rating_at_game
# MAGIC   FROM big_beaten
# MAGIC
# MAGIC   UNION ALL
# MAGIC
# MAGIC   -- 3) Nemesis: opponent with worst record against
# MAGIC   SELECT
# MAGIC     coach_id, 3, 1,
# MAGIC     'Nemesis',
# MAGIC     s.opponent_coach_id,
# MAGIC     s.opponent_coach_name,
# MAGIC     s.opponent_nation_name_display,
# MAGIC     s.opponent_flag_emoji,
# MAGIC     s.games_played,
# MAGIC     s.wins,
# MAGIC     s.draws,
# MAGIC     s.losses,
# MAGIC     s.global_elo_current_or_peak_all,
# MAGIC     s.global_elo_current,
# MAGIC     s.global_elo_peak_all,
# MAGIC     ROUND(COALESCE(s.win_pct, 0.0), 1),
# MAGIC     s.win_points,
# MAGIC     s.win_points_per_game,
# MAGIC     s.load_timestamp,
# MAGIC     CAST(NULL AS DOUBLE),
# MAGIC     CAST(NULL AS DOUBLE),
# MAGIC     CAST(NULL AS DOUBLE)
# MAGIC   FROM nemesis
# MAGIC
# MAGIC   UNION ALL
# MAGIC
# MAGIC   -- 4) Biggest upset done: won against highest-rated opponent
# MAGIC   SELECT
# MAGIC     ud.coach_id, 4, 1,
# MAGIC     'Biggest upset: done',
# MAGIC     ud.s.opponent_coach_id,
# MAGIC     oi_ud.coach_name,
# MAGIC     oi_ud.nation_name_display,
# MAGIC     oi_ud.flag_emoji,
# MAGIC     CAST(NULL AS BIGINT),   -- no h2h games_played for single game
# MAGIC     CAST(NULL AS BIGINT),
# MAGIC     CAST(NULL AS BIGINT),
# MAGIC     CAST(NULL AS BIGINT),
# MAGIC     CAST(NULL AS DOUBLE),
# MAGIC     CAST(NULL AS DOUBLE),
# MAGIC     CAST(NULL AS DOUBLE),
# MAGIC     CAST(NULL AS DOUBLE),
# MAGIC     CAST(NULL AS DOUBLE),
# MAGIC     CAST(NULL AS DOUBLE),
# MAGIC     CURRENT_TIMESTAMP(),
# MAGIC     ud.s.rating_gap,
# MAGIC     ud.s.coach_rating,
# MAGIC     ud.s.opponent_rating
# MAGIC   FROM biggest_upset_done ud
# MAGIC   LEFT JOIN naf_catalog.gold_presentation.coach_identity_v oi_ud
# MAGIC     ON ud.s.opponent_coach_id = oi_ud.coach_id
# MAGIC
# MAGIC   UNION ALL
# MAGIC
# MAGIC   -- 5) Biggest upset received: lost to lowest-rated opponent
# MAGIC   SELECT
# MAGIC     ur.coach_id, 4, 2,
# MAGIC     'Biggest upset: received',
# MAGIC     ur.s.opponent_coach_id,
# MAGIC     oi_ur.coach_name,
# MAGIC     oi_ur.nation_name_display,
# MAGIC     oi_ur.flag_emoji,
# MAGIC     CAST(NULL AS BIGINT),
# MAGIC     CAST(NULL AS BIGINT),
# MAGIC     CAST(NULL AS BIGINT),
# MAGIC     CAST(NULL AS BIGINT),
# MAGIC     CAST(NULL AS DOUBLE),
# MAGIC     CAST(NULL AS DOUBLE),
# MAGIC     CAST(NULL AS DOUBLE),
# MAGIC     CAST(NULL AS DOUBLE),
# MAGIC     CAST(NULL AS DOUBLE),
# MAGIC     CAST(NULL AS DOUBLE),
# MAGIC     CURRENT_TIMESTAMP(),
# MAGIC     ur.s.rating_gap,
# MAGIC     ur.s.coach_rating,
# MAGIC     ur.s.opponent_rating
# MAGIC   FROM biggest_upset_received ur
# MAGIC   LEFT JOIN naf_catalog.gold_presentation.coach_identity_v oi_ur
# MAGIC     ON ur.s.opponent_coach_id = oi_ur.coach_id
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   coach_id,
# MAGIC   category,
# MAGIC
# MAGIC   opponent_coach_id,
# MAGIC   opponent_coach_name,
# MAGIC   opponent_nation_name_display,
# MAGIC   opponent_flag_emoji,
# MAGIC
# MAGIC   games_played,
# MAGIC   wins,
# MAGIC   draws,
# MAGIC   losses,
# MAGIC
# MAGIC   global_elo_current_or_peak_all,
# MAGIC   global_elo_current,
# MAGIC   global_elo_peak_all,
# MAGIC
# MAGIC   win_pct,
# MAGIC   win_points,
# MAGIC   win_points_per_game,
# MAGIC
# MAGIC   rating_gap,
# MAGIC   coach_rating_at_game,
# MAGIC   opponent_rating_at_game,
# MAGIC
# MAGIC   group_order,
# MAGIC   row_order,
# MAGIC
# MAGIC   load_timestamp
# MAGIC FROM rows
# MAGIC ;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- *VIEW*: naf_catalog.gold_presentation.coach_streak_detail
# MAGIC -- =====================================================================
# MAGIC -- LAYER        : GOLD_PRESENTATION
# MAGIC -- CONTRACT TYPE: Dashboard-facing streak segments (event-level segments with display fields)
# MAGIC --
# MAGIC -- PURPOSE
# MAGIC --   Provide streak "segments" (continuous runs) for coach streak analysis, enriched with:
# MAGIC --     - coach identity fields
# MAGIC --     - race display fields (race_id, race_name)
# MAGIC --     - opponent identity for streak-ending opponent (when applicable)
# MAGIC --     - optional day-delta fields derived from date_dim for convenient dashboard labelling
# MAGIC --
# MAGIC -- GRAIN / KEYS
# MAGIC --   GRAIN       : (scope, coach_id, race_id, streak_type, group_id)
# MAGIC --   PRIMARY KEY : (scope, coach_id, race_id, streak_type, group_id)
# MAGIC --
# MAGIC -- SOURCES
# MAGIC --   - naf_catalog.gold_summary.coach_streak_segments          (segment facts; order-based contract)
# MAGIC --   - naf_catalog.gold_presentation.coach_identity_v          (coach + opponent display fields)
# MAGIC --   - naf_catalog.gold_dim.race_dim                           (race_name; includes race_id = 0 => 'none')
# MAGIC --   - naf_catalog.gold_dim.date_dim                           (date keys to calendar date for day-deltas)
# MAGIC --
# MAGIC -- DASHBOARD CONTRACT NOTES
# MAGIC --   - The streak contract uses *_game_order (order) rather than *_game_index.
# MAGIC --   - Date fields are exposed as IDs (start_date_id/end_date_id) and enriched to "age in days" via date_dim.
# MAGIC --   - "Age in days" fields are intentionally dynamic (depend on CURRENT_DATE) and will change daily
# MAGIC --     without a pipeline reload; this is expected dashboard behaviour (not data drift).
# MAGIC --   - race_id is COALESCE(scope_race_id, 0). In race_dim, race_id = 0 represents 'none' (global bucket).
# MAGIC --
# MAGIC -- LOAD / FRESHNESS
# MAGIC --   - load_timestamp is inherited from gold_summary.coach_streak_segments.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.coach_streak_detail AS
# MAGIC SELECT
# MAGIC   s.scope,
# MAGIC   s.coach_id,
# MAGIC   ci.coach_name,
# MAGIC   ci.nation_id,
# MAGIC   ci.nation_name_display,
# MAGIC   ci.flag_emoji,
# MAGIC
# MAGIC   COALESCE(s.scope_race_id, 0) AS race_id,
# MAGIC   r.race_name,
# MAGIC
# MAGIC   s.streak_type,
# MAGIC   s.group_id,
# MAGIC
# MAGIC   -- Summary contract uses *order*, not *index*
# MAGIC   s.first_game_order,
# MAGIC   s.last_game_order,
# MAGIC
# MAGIC   -- Hard cutover: IDs, not dates
# MAGIC   s.start_date_id,
# MAGIC   s.end_date_id,
# MAGIC
# MAGIC   s.length,
# MAGIC
# MAGIC   s.ended_by_result,
# MAGIC   s.ended_by_opponent_coach_id,
# MAGIC   eoi.coach_name AS ended_by_opponent_coach_name,
# MAGIC   s.ended_by_tournament_id,
# MAGIC
# MAGIC   -- Dynamic "age in days" (derived from date_dim; depends on CURRENT_DATE)
# MAGIC   CASE WHEN dd_end.date IS NOT NULL
# MAGIC        THEN DATEDIFF(CURRENT_DATE(), dd_end.date)
# MAGIC   END AS end_game_age_days,
# MAGIC
# MAGIC   CASE WHEN dd_start.date IS NOT NULL
# MAGIC        THEN DATEDIFF(CURRENT_DATE(), dd_start.date)
# MAGIC   END AS start_game_age_days,
# MAGIC
# MAGIC   CASE WHEN dd_start.date IS NOT NULL AND dd_end.date IS NOT NULL
# MAGIC        THEN DATEDIFF(dd_end.date, dd_start.date)
# MAGIC   END AS streak_span_days,
# MAGIC
# MAGIC   s.load_timestamp
# MAGIC FROM naf_catalog.gold_summary.coach_streak_segments s
# MAGIC LEFT JOIN naf_catalog.gold_presentation.coach_identity_v ci
# MAGIC   ON s.coach_id = ci.coach_id
# MAGIC LEFT JOIN naf_catalog.gold_presentation.coach_identity_v eoi
# MAGIC   ON s.ended_by_opponent_coach_id = eoi.coach_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.race_dim r
# MAGIC   ON COALESCE(s.scope_race_id, 0) = r.race_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.date_dim dd_start
# MAGIC   ON s.start_date_id = dd_start.date_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.date_dim dd_end
# MAGIC   ON s.end_date_id = dd_end.date_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- *VIEW*: naf_catalog.gold_presentation.coach_streak_overview
# MAGIC -- =====================================================================
# MAGIC -- LAYER        : GOLD_PRESENTATION
# MAGIC -- CONTRACT TYPE: Dashboard-facing streak summary (best/current + current end age)
# MAGIC --
# MAGIC -- PURPOSE
# MAGIC --   Provide a per-coach streak summary by scope and race scope, enriched with:
# MAGIC --     - coach identity fields
# MAGIC --     - race display fields (race_id, race_name)
# MAGIC --     - best and current streak lengths (win + unbeaten)
# MAGIC --     - current streak end_date_id (from ACTIVE segments) and "age in days" since that end date
# MAGIC --
# MAGIC -- GRAIN / KEYS
# MAGIC --   GRAIN       : (scope, coach_id, race_id)
# MAGIC --   PRIMARY KEY : (scope, coach_id, race_id)
# MAGIC --
# MAGIC -- SOURCES
# MAGIC --   - naf_catalog.gold_summary.coach_streak_summary            (best/current streak lengths)
# MAGIC --   - naf_catalog.gold_summary.coach_streak_segments           (ACTIVE segment end_date_id per streak type)
# MAGIC --   - naf_catalog.gold_presentation.coach_identity_v           (coach display fields)
# MAGIC --   - naf_catalog.gold_dim.race_dim                            (race_name; includes race_id = 0 => 'none')
# MAGIC --   - naf_catalog.gold_dim.date_dim                            (date_id to calendar date for day-deltas)
# MAGIC --
# MAGIC -- DASHBOARD CONTRACT NOTES
# MAGIC --   - race_id is COALESCE(scope_race_id, 0). In race_dim, race_id = 0 represents 'none' (global bucket).
# MAGIC --   - current_*_end_age_days depends on CURRENT_DATE() and will change daily without a pipeline reload.
# MAGIC --     This is expected dashboard behaviour (not data drift).
# MAGIC --   - ACTIVE segments are expected to carry end_date_id...; if missing upstream, *_end_date_id and *_age_days may be NULL.
# MAGIC --
# MAGIC -- LOAD / FRESHNESS
# MAGIC --   - load_timestamp is the GREATEST of summary and ACTIVE-segment extraction timestamps.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.coach_streak_overview AS
# MAGIC WITH active_segments AS (
# MAGIC   SELECT
# MAGIC     scope,
# MAGIC     coach_id,
# MAGIC     scope_race_id,
# MAGIC
# MAGIC     -- For ACTIVE segments, end_date_id is expected to exist; we keep it as ID
# MAGIC     MAX(CASE WHEN streak_type = 'WIN'      THEN end_date_id END) AS current_win_end_date_id,
# MAGIC     MAX(CASE WHEN streak_type = 'UNBEATEN' THEN end_date_id END) AS current_unbeaten_end_date_id,
# MAGIC
# MAGIC     MAX(load_timestamp) AS load_timestamp
# MAGIC   FROM naf_catalog.gold_summary.coach_streak_segments
# MAGIC   WHERE ended_by_result = 'ACTIVE'
# MAGIC   GROUP BY scope, coach_id, scope_race_id
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   s.scope,
# MAGIC   s.coach_id,
# MAGIC   ci.coach_name,
# MAGIC   ci.nation_id,
# MAGIC   ci.nation_name_display,
# MAGIC   ci.flag_emoji,
# MAGIC
# MAGIC   COALESCE(s.scope_race_id, 0) AS race_id,
# MAGIC   r.race_name,
# MAGIC
# MAGIC   s.best_win_streak,
# MAGIC   s.best_unbeaten_streak,
# MAGIC   s.current_win_streak,
# MAGIC   s.current_unbeaten_streak,
# MAGIC
# MAGIC   a.current_win_end_date_id,
# MAGIC   CASE WHEN dd_win_end.date IS NOT NULL
# MAGIC        THEN DATEDIFF(CURRENT_DATE(), dd_win_end.date)
# MAGIC   END AS current_win_end_age_days,
# MAGIC
# MAGIC   a.current_unbeaten_end_date_id,
# MAGIC   CASE WHEN dd_unb_end.date IS NOT NULL
# MAGIC        THEN DATEDIFF(CURRENT_DATE(), dd_unb_end.date)
# MAGIC   END AS current_unbeaten_end_age_days,
# MAGIC
# MAGIC   GREATEST(
# MAGIC     COALESCE(s.load_timestamp, TIMESTAMP('1900-01-01')),
# MAGIC     COALESCE(a.load_timestamp, TIMESTAMP('1900-01-01'))
# MAGIC   ) AS load_timestamp
# MAGIC
# MAGIC FROM naf_catalog.gold_summary.coach_streak_summary AS s
# MAGIC LEFT JOIN active_segments AS a
# MAGIC   ON  a.scope = s.scope
# MAGIC   AND a.coach_id = s.coach_id
# MAGIC   AND a.scope_race_id <=> s.scope_race_id
# MAGIC LEFT JOIN naf_catalog.gold_presentation.coach_identity_v AS ci
# MAGIC   ON s.coach_id = ci.coach_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.race_dim AS r
# MAGIC   ON COALESCE(s.scope_race_id, 0) = r.race_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.date_dim dd_win_end
# MAGIC   ON a.current_win_end_date_id = dd_win_end.date_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.date_dim dd_unb_end
# MAGIC   ON a.current_unbeaten_end_date_id = dd_unb_end.date_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- *VIEW*: naf_catalog.gold_presentation.coach_tournament_performance
# MAGIC -- =====================================================================
# MAGIC -- LAYER        : GOLD_PRESENTATION
# MAGIC -- CONTRACT TYPE: Dashboard-facing coach tournament performance (1 row per coach x tournament)
# MAGIC --
# MAGIC -- PURPOSE
# MAGIC --   Provide a stable contract for coach performance per tournament, combining:
# MAGIC --     - tournament display fields (name, major flag, tournament nation display)
# MAGIC --     - tournament calendar keys (start/end date_id derived from tournament_dim date fields)
# MAGIC --     - coach tournament performance metrics from summary (no re-aggregation)
# MAGIC --     - convenience day-deltas for dashboard UX (age since last game, age since tournament end)
# MAGIC --
# MAGIC -- GRAIN / KEYS
# MAGIC --   GRAIN       : (coach_id, tournament_id)
# MAGIC --   PRIMARY KEY : (coach_id, tournament_id)
# MAGIC --
# MAGIC -- SOURCES
# MAGIC --   - naf_catalog.gold_summary.coach_tournament_performance_summary   (metrics)
# MAGIC --   - naf_catalog.gold_dim.tournament_dim                             (tournament meta + dates)
# MAGIC --   - naf_catalog.gold_dim.nation_dim                                 (tournament nation display)
# MAGIC --   - naf_catalog.gold_dim.date_dim                                   (date_id + calendar date)
# MAGIC --
# MAGIC -- DASHBOARD CONTRACT NOTES
# MAGIC --   - date_start_id/date_end_id are derived from tournament_dim date_start/date_end using date_dim.
# MAGIC --   - tournament_end_date_id uses date_end_id when present, otherwise date_start_id.
# MAGIC --   - last_game_age_days and tournament_end_age_days depend on CURRENT_DATE() and will change daily
# MAGIC --     without pipeline reload; this is expected dashboard behaviour (not data drift).
# MAGIC --
# MAGIC -- LOAD / FRESHNESS
# MAGIC --   - load_timestamp is inherited from the underlying summary table.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.coach_tournament_performance AS
# MAGIC WITH t_base AS (
# MAGIC   SELECT
# MAGIC     t.tournament_id,
# MAGIC     t.tournament_name,
# MAGIC     t.is_major_tournament,
# MAGIC     t.nation_id AS tournament_nation_id,
# MAGIC
# MAGIC     -- dim currently stores dates (mapped to date_id via date_dim)
# MAGIC     t.date_start,
# MAGIC     t.date_end
# MAGIC   FROM naf_catalog.gold_dim.tournament_dim t
# MAGIC ),
# MAGIC
# MAGIC s_base AS (
# MAGIC   SELECT
# MAGIC     s.coach_id,
# MAGIC     s.tournament_id,
# MAGIC
# MAGIC     -- metrics (no re-aggregation)
# MAGIC     s.games_played,
# MAGIC     s.wins,
# MAGIC     s.draws,
# MAGIC     s.losses,
# MAGIC     s.points_total,
# MAGIC     s.td_for,
# MAGIC     s.td_against,
# MAGIC     s.td_diff,
# MAGIC
# MAGIC     s.date_first_game_id,
# MAGIC     s.date_last_game_id,
# MAGIC
# MAGIC     s.global_elo_exchange_total,
# MAGIC     s.global_elo_exchange_mean,
# MAGIC     s.global_elo_median_rating_after,
# MAGIC
# MAGIC     s.load_timestamp
# MAGIC   FROM naf_catalog.gold_summary.coach_tournament_performance_summary s
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   s.coach_id,
# MAGIC   s.tournament_id,
# MAGIC
# MAGIC   t.tournament_name,
# MAGIC
# MAGIC   -- hard cutover: IDs only (derived)
# MAGIC   ds.date_id AS date_start_id,
# MAGIC   de.date_id AS date_end_id,
# MAGIC   COALESCE(de.date_id, ds.date_id) AS tournament_end_date_id,
# MAGIC
# MAGIC   -- dynamic span in days
# MAGIC   CASE WHEN ds.date IS NOT NULL
# MAGIC        THEN DATEDIFF(COALESCE(de.date, ds.date), ds.date)
# MAGIC   END AS tournament_span_days,
# MAGIC
# MAGIC   t.is_major_tournament,
# MAGIC
# MAGIC   t.tournament_nation_id,
# MAGIC   tn.nation_name_display AS tournament_nation_name_display,
# MAGIC   tn.flag_code           AS tournament_flag_code,
# MAGIC
# MAGIC   -- metrics
# MAGIC   s.games_played,
# MAGIC   s.wins,
# MAGIC   s.draws,
# MAGIC   s.losses,
# MAGIC   s.points_total,
# MAGIC   s.td_for,
# MAGIC   s.td_against,
# MAGIC   s.td_diff,
# MAGIC
# MAGIC   s.date_first_game_id,
# MAGIC   s.date_last_game_id,
# MAGIC
# MAGIC   -- dynamic ages
# MAGIC   CASE WHEN dlg.date IS NOT NULL
# MAGIC        THEN DATEDIFF(CURRENT_DATE(), dlg.date)
# MAGIC   END AS last_game_age_days,
# MAGIC
# MAGIC   CASE WHEN COALESCE(de.date, ds.date) IS NOT NULL
# MAGIC        THEN DATEDIFF(CURRENT_DATE(), COALESCE(de.date, ds.date))
# MAGIC   END AS tournament_end_age_days,
# MAGIC
# MAGIC   s.global_elo_exchange_total,
# MAGIC   s.global_elo_exchange_mean,
# MAGIC   s.global_elo_median_rating_after,
# MAGIC
# MAGIC   s.load_timestamp
# MAGIC
# MAGIC FROM s_base s
# MAGIC LEFT JOIN t_base t
# MAGIC   ON s.tournament_id = t.tournament_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.nation_dim tn
# MAGIC   ON t.tournament_nation_id = tn.nation_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.date_dim ds
# MAGIC   ON t.date_start = ds.date
# MAGIC LEFT JOIN naf_catalog.gold_dim.date_dim de
# MAGIC   ON t.date_end = de.date
# MAGIC LEFT JOIN naf_catalog.gold_dim.date_dim dlg
# MAGIC   ON s.date_last_game_id = dlg.date_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- VIEW: naf_catalog.gold_presentation.coach_global_elo_rating_history
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Dashboard-ready GLOBAL-scope GLO rating history.
# MAGIC -- LAYER        : GOLD_PRESENTATION
# MAGIC -- CONTRACT TYPE: Canonical timeseries (global scope) for dashboards/exports.
# MAGIC --
# MAGIC -- GRAIN        : 1 row per (rating_system, coach_id, game_id)
# MAGIC -- PRIMARY KEY  : (rating_system, coach_id, game_id)
# MAGIC --
# MAGIC -- SOURCES
# MAGIC --   - naf_catalog.gold_summary.coach_global_elo_rating_history_v   (canonical spine projection)
# MAGIC --   - naf_catalog.gold_presentation.coach_identity_v               (coach/opponent display fields)
# MAGIC --   - naf_catalog.gold_dim.date_dim                                (display date derived from date_id)
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.coach_global_elo_rating_history AS
# MAGIC SELECT
# MAGIC   -- Identifiers
# MAGIC   h.rating_system,
# MAGIC   h.coach_id,
# MAGIC   ci.coach_name,
# MAGIC   ci.nation_id,
# MAGIC   ci.nation_name_display,
# MAGIC   ci.flag_emoji,
# MAGIC
# MAGIC   h.opponent_coach_id,
# MAGIC   oi.coach_name          AS opponent_coach_name,
# MAGIC   oi.nation_name_display AS opponent_nation_name_display,
# MAGIC   oi.flag_emoji           AS opponent_flag_emoji,
# MAGIC   
# MAGIC   h.game_id,
# MAGIC
# MAGIC   -- Ordering / time
# MAGIC   h.game_index,
# MAGIC   h.date_id,
# MAGIC   dd.date                      AS game_date,
# MAGIC   h.event_timestamp,
# MAGIC
# MAGIC   -- Result
# MAGIC   h.result_numeric,
# MAGIC   CASE WHEN h.result_numeric = 1.0 THEN 1 ELSE 0 END AS win,
# MAGIC   CASE WHEN h.result_numeric = 0.5 THEN 1 ELSE 0 END AS draw,
# MAGIC   CASE WHEN h.result_numeric = 0.0 THEN 1 ELSE 0 END AS loss,
# MAGIC
# MAGIC   -- GLOBAL GLO rating change (clear naming)
# MAGIC   h.rating_before              AS glo_rating_before,
# MAGIC   h.rating_after               AS glo_rating_after,
# MAGIC   h.rating_delta               AS glo_rating_delta
# MAGIC
# MAGIC FROM naf_catalog.gold_summary.coach_global_elo_rating_history_v AS h
# MAGIC LEFT JOIN naf_catalog.gold_presentation.coach_identity_v AS ci
# MAGIC   ON h.coach_id = ci.coach_id
# MAGIC LEFT JOIN naf_catalog.gold_presentation.coach_identity_v AS oi
# MAGIC   ON h.opponent_coach_id = oi.coach_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.date_dim AS dd
# MAGIC   ON h.date_id = dd.date_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- VIEW: naf_catalog.gold_presentation.coach_race_elo_rating_history
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Dashboard-ready RACE-scope Elo rating history.
# MAGIC -- LAYER        : GOLD_PRESENTATION
# MAGIC -- CONTRACT TYPE: Canonical timeseries (race scope) for dashboards/exports.
# MAGIC --
# MAGIC -- GRAIN        : 1 row per (rating_system, coach_id, race_id, game_id)
# MAGIC -- PRIMARY KEY  : (rating_system, coach_id, race_id, game_id)
# MAGIC --
# MAGIC -- SOURCES
# MAGIC --   - naf_catalog.gold_summary.coach_race_elo_rating_history_v   (canonical spine projection)
# MAGIC --   - naf_catalog.gold_presentation.coach_identity_v             (coach/opponent display fields)
# MAGIC --   - naf_catalog.gold_dim.race_dim                              (race_name)
# MAGIC --   - naf_catalog.gold_dim.date_dim                              (display date derived from date_id)
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.coach_race_elo_rating_history AS
# MAGIC SELECT
# MAGIC   -- Identifiers
# MAGIC   h.rating_system,
# MAGIC   h.coach_id,
# MAGIC   ci.coach_name,
# MAGIC   ci.nation_id,
# MAGIC   ci.nation_name_display,
# MAGIC   ci.flag_emoji,
# MAGIC
# MAGIC   h.opponent_coach_id,
# MAGIC   oi.coach_name          AS opponent_coach_name,
# MAGIC   oi.nation_name_display AS opponent_nation_name_display,
# MAGIC   oi.flag_emoji           AS opponent_flag_emoji,
# MAGIC
# MAGIC   h.game_id,
# MAGIC
# MAGIC   -- Race scope
# MAGIC   h.race_id,
# MAGIC   rd.race_name,
# MAGIC
# MAGIC   -- Ordering / time
# MAGIC   h.game_index,
# MAGIC   h.date_id,
# MAGIC   dd.date               AS game_date,
# MAGIC   h.event_timestamp,
# MAGIC
# MAGIC   -- Result
# MAGIC   h.result_numeric,
# MAGIC   CASE WHEN h.result_numeric = 1.0 THEN 1 ELSE 0 END AS win,
# MAGIC   CASE WHEN h.result_numeric = 0.5 THEN 1 ELSE 0 END AS draw,
# MAGIC   CASE WHEN h.result_numeric = 0.0 THEN 1 ELSE 0 END AS loss,
# MAGIC
# MAGIC   -- RACE Elo rating change (clear naming)
# MAGIC   h.rating_before       AS elo_rating_before,
# MAGIC   h.rating_after        AS elo_rating_after,
# MAGIC   h.rating_delta        AS elo_rating_delta
# MAGIC
# MAGIC FROM naf_catalog.gold_summary.coach_race_elo_rating_history_v AS h
# MAGIC LEFT JOIN naf_catalog.gold_presentation.coach_identity_v AS ci
# MAGIC   ON h.coach_id = ci.coach_id
# MAGIC LEFT JOIN naf_catalog.gold_presentation.coach_identity_v AS oi
# MAGIC   ON h.opponent_coach_id = oi.coach_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.race_dim AS rd
# MAGIC   ON h.race_id = rd.race_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.date_dim AS dd
# MAGIC   ON h.date_id = dd.date_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- VIEW: naf_catalog.gold_presentation.coach_global_elo_rating_daily_series
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Daily-smoothed GLOBAL GLO rating history with running peak
# MAGIC --                and running median lines for dashboard charts.
# MAGIC -- LAYER        : GOLD_PRESENTATION
# MAGIC -- CONTRACT TYPE: Timeseries (1 row per coach per day), for line charts.
# MAGIC --
# MAGIC -- GRAIN        : 1 row per (rating_system, coach_id, game_date)
# MAGIC -- PRIMARY KEY  : (rating_system, coach_id, game_date)
# MAGIC --
# MAGIC -- LOGIC
# MAGIC --   - Picks the LAST game of each day (max game_index per date_id).
# MAGIC --   - Computes running peak GLO (all-time max up to that day).
# MAGIC --   - Computes running median GLO (median of all daily-closing values up to that day).
# MAGIC --
# MAGIC -- SOURCES
# MAGIC --   - naf_catalog.gold_presentation.coach_global_elo_rating_history (per-game)
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.coach_global_elo_rating_daily_series AS
# MAGIC WITH last_game_of_day AS (
# MAGIC   SELECT
# MAGIC     rating_system,
# MAGIC     coach_id,
# MAGIC     coach_name,
# MAGIC     nation_id,
# MAGIC     nation_name_display,
# MAGIC     flag_emoji,
# MAGIC     game_date,
# MAGIC     glo_rating_after,
# MAGIC     game_index,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY rating_system, coach_id, game_date
# MAGIC       ORDER BY game_index DESC
# MAGIC     ) AS rn
# MAGIC   FROM naf_catalog.gold_presentation.coach_global_elo_rating_history
# MAGIC )
# MAGIC SELECT
# MAGIC   rating_system,
# MAGIC   coach_id,
# MAGIC   coach_name,
# MAGIC   nation_id,
# MAGIC   nation_name_display,
# MAGIC   flag_emoji,
# MAGIC   game_date,
# MAGIC   glo_rating_after                                                          AS glo_daily,
# MAGIC   MAX(glo_rating_after) OVER (
# MAGIC     PARTITION BY rating_system, coach_id
# MAGIC     ORDER BY game_date
# MAGIC     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC   )                                                                         AS glo_running_peak,
# MAGIC   PERCENTILE_APPROX(glo_rating_after, 0.5) OVER (
# MAGIC     PARTITION BY rating_system, coach_id
# MAGIC     ORDER BY game_date
# MAGIC     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC   )                                                                         AS glo_running_median,
# MAGIC   game_index                                                                AS last_game_index_of_day
# MAGIC FROM last_game_of_day
# MAGIC WHERE rn = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- VIEW: naf_catalog.gold_presentation.coach_race_elo_rating_daily_series
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Daily-smoothed RACE Elo rating history with race rank
# MAGIC --                by total games played (for default top-6 filtering).
# MAGIC -- LAYER        : GOLD_PRESENTATION
# MAGIC -- CONTRACT TYPE: Timeseries (1 row per coach per race per day), for line charts.
# MAGIC --
# MAGIC -- GRAIN        : 1 row per (rating_system, coach_id, race_id, game_date)
# MAGIC -- PRIMARY KEY  : (rating_system, coach_id, race_id, game_date)
# MAGIC --
# MAGIC -- LOGIC
# MAGIC --   - Picks the LAST game of each day per race.
# MAGIC --   - Adds race_rank_by_games: DENSE_RANK by total games played per race
# MAGIC --     (most played race = 1). Used by dashboard to default to top 6.
# MAGIC --
# MAGIC -- SOURCES
# MAGIC --   - naf_catalog.gold_presentation.coach_race_elo_rating_history (per-game)
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.coach_race_elo_rating_daily_series AS
# MAGIC WITH last_game_of_day AS (
# MAGIC   SELECT
# MAGIC     rating_system,
# MAGIC     coach_id,
# MAGIC     coach_name,
# MAGIC     race_id,
# MAGIC     race_name,
# MAGIC     game_date,
# MAGIC     elo_rating_after,
# MAGIC     game_index,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY rating_system, coach_id, race_id, game_date
# MAGIC       ORDER BY game_index DESC
# MAGIC     ) AS rn
# MAGIC   FROM naf_catalog.gold_presentation.coach_race_elo_rating_history
# MAGIC ),
# MAGIC daily AS (
# MAGIC   SELECT
# MAGIC     rating_system,
# MAGIC     coach_id,
# MAGIC     coach_name,
# MAGIC     race_id,
# MAGIC     race_name,
# MAGIC     game_date,
# MAGIC     elo_rating_after    AS elo_daily,
# MAGIC     game_index          AS last_game_index_of_day
# MAGIC   FROM last_game_of_day
# MAGIC   WHERE rn = 1
# MAGIC ),
# MAGIC race_game_counts AS (
# MAGIC   SELECT
# MAGIC     rating_system,
# MAGIC     coach_id,
# MAGIC     race_id,
# MAGIC     COUNT(*) AS total_race_days
# MAGIC   FROM daily
# MAGIC   GROUP BY rating_system, coach_id, race_id
# MAGIC )
# MAGIC SELECT
# MAGIC   d.rating_system,
# MAGIC   d.coach_id,
# MAGIC   d.coach_name,
# MAGIC   d.race_id,
# MAGIC   d.race_name,
# MAGIC   d.game_date,
# MAGIC   d.elo_daily,
# MAGIC   d.last_game_index_of_day,
# MAGIC   DENSE_RANK() OVER (
# MAGIC     PARTITION BY d.rating_system, d.coach_id
# MAGIC     ORDER BY rc.total_race_days DESC, d.race_id ASC
# MAGIC   ) AS race_rank_by_games
# MAGIC FROM daily d
# MAGIC JOIN race_game_counts rc
# MAGIC   ON d.rating_system = rc.rating_system
# MAGIC  AND d.coach_id = rc.coach_id
# MAGIC  AND d.race_id = rc.race_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- VIEW: naf_catalog.gold_presentation.coach_cumulative_results_last_n_daily_series
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Cumulative W/D/L for the LAST N games only, re-cumulated
# MAGIC --                from zero. For "recent form" cumulative charts.
# MAGIC -- LAYER        : GOLD_PRESENTATION
# MAGIC -- CONTRACT TYPE: Timeseries (1 row per coach per day per result), for stacked area.
# MAGIC --
# MAGIC -- GRAIN        : (coach_id, event_day, result_order)
# MAGIC -- PRIMARY KEY  : (coach_id, event_day, result_order)
# MAGIC --
# MAGIC -- LOGIC
# MAGIC --   - Takes the last N rated games (from global elo history).
# MAGIC --   - N is read from analytical_config (last_n_games_window, default 50).
# MAGIC --   - Only produced for coaches with >= 2*N total games.
# MAGIC --   - Re-cumulates W/D/L from the Nth-from-last game to the most recent.
# MAGIC --   - Daily-smoothed: picks last game of each day.
# MAGIC --
# MAGIC -- SOURCES
# MAGIC --   - naf_catalog.gold_presentation.coach_global_elo_rating_history
# MAGIC --   - naf_catalog.gold_dim.analytical_config
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.coach_cumulative_results_last_n_daily_series AS
# MAGIC WITH cfg AS (
# MAGIC   SELECT CAST(last_n_games_window AS INT) AS n
# MAGIC   FROM naf_catalog.gold_dim.analytical_config
# MAGIC ),
# MAGIC eligible AS (
# MAGIC   -- Coaches with at least 2*N total rated games
# MAGIC   SELECT h.coach_id, h.rating_system, COUNT(*) AS total_games
# MAGIC   FROM naf_catalog.gold_presentation.coach_global_elo_rating_history h
# MAGIC   CROSS JOIN cfg
# MAGIC   GROUP BY h.coach_id, h.rating_system
# MAGIC   HAVING COUNT(*) >= 2 * MAX(cfg.n)
# MAGIC ),
# MAGIC last_n AS (
# MAGIC   SELECT
# MAGIC     h.rating_system,
# MAGIC     h.coach_id,
# MAGIC     h.coach_name,
# MAGIC     h.game_date,
# MAGIC     h.game_index,
# MAGIC     h.result_numeric,
# MAGIC     CASE WHEN h.result_numeric = 1.0 THEN 1 ELSE 0 END AS win,
# MAGIC     CASE WHEN h.result_numeric = 0.5 THEN 1 ELSE 0 END AS draw,
# MAGIC     CASE WHEN h.result_numeric = 0.0 THEN 1 ELSE 0 END AS loss,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY h.rating_system, h.coach_id
# MAGIC       ORDER BY h.game_index DESC
# MAGIC     ) AS reverse_idx
# MAGIC   FROM naf_catalog.gold_presentation.coach_global_elo_rating_history h
# MAGIC   JOIN eligible e
# MAGIC     ON h.coach_id = e.coach_id AND h.rating_system = e.rating_system
# MAGIC ),
# MAGIC last_n_filtered AS (
# MAGIC   SELECT *
# MAGIC   FROM last_n
# MAGIC   CROSS JOIN cfg
# MAGIC   WHERE reverse_idx <= cfg.n
# MAGIC ),
# MAGIC cumulated AS (
# MAGIC   SELECT
# MAGIC     rating_system,
# MAGIC     coach_id,
# MAGIC     coach_name,
# MAGIC     game_date,
# MAGIC     game_index,
# MAGIC     SUM(win)  OVER w AS cum_win,
# MAGIC     SUM(draw) OVER w AS cum_draw,
# MAGIC     SUM(loss) OVER w AS cum_loss,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY rating_system, coach_id, game_date
# MAGIC       ORDER BY game_index DESC
# MAGIC     ) AS day_rn
# MAGIC   FROM last_n_filtered
# MAGIC   WINDOW w AS (
# MAGIC     PARTITION BY rating_system, coach_id
# MAGIC     ORDER BY game_index
# MAGIC     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC   )
# MAGIC ),
# MAGIC eod AS (
# MAGIC   SELECT * FROM cumulated WHERE day_rn = 1
# MAGIC )
# MAGIC SELECT
# MAGIC   coach_id,
# MAGIC   coach_name,
# MAGIC   game_date  AS event_day,
# MAGIC   result,
# MAGIC   result_order,
# MAGIC   CAST(value AS BIGINT) AS value
# MAGIC FROM eod
# MAGIC LATERAL VIEW STACK(
# MAGIC   3,
# MAGIC   'win',  cum_win,  1,
# MAGIC   'draw', cum_draw, 2,
# MAGIC   'loss', cum_loss, 3
# MAGIC ) s AS result, value, result_order;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TEST: naf_catalog.gold_presentation (coach) smoke tests (dashboard-aligned)
# MAGIC -- =====================================================================
# MAGIC
# MAGIC WITH checks AS (
# MAGIC
# MAGIC   -- -------------------------
# MAGIC   -- Non-empty checks
# MAGIC   -- -------------------------
# MAGIC   SELECT 'pres.coach_profile non_empty' AS check_name,
# MAGIC          CASE WHEN (SELECT COUNT(*) FROM naf_catalog.gold_presentation.coach_profile) = 0 THEN 1 ELSE 0 END AS fail_rows
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'pres.coach_race_performance non_empty',
# MAGIC          CASE WHEN (SELECT COUNT(*) FROM naf_catalog.gold_presentation.coach_race_performance) = 0 THEN 1 ELSE 0 END
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'pres.coach_cumulative_results_daily_series non_empty',
# MAGIC          CASE WHEN (SELECT COUNT(*) FROM naf_catalog.gold_presentation.coach_cumulative_results_daily_series) = 0 THEN 1 ELSE 0 END
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'pres.coach_opponent_rating non_empty',
# MAGIC          CASE WHEN (SELECT COUNT(*) FROM naf_catalog.gold_presentation.coach_opponent_rating) = 0 THEN 1 ELSE 0 END
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'pres.coach_opponent_global_elo_bin_insights non_empty',
# MAGIC          CASE WHEN (SELECT COUNT(*) FROM naf_catalog.gold_presentation.coach_opponent_global_elo_bin_insights) = 0 THEN 1 ELSE 0 END
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'pres.coach_opponent_global_elo_bin_results_long non_empty',
# MAGIC          CASE WHEN (SELECT COUNT(*) FROM naf_catalog.gold_presentation.coach_opponent_global_elo_bin_results_long) = 0 THEN 1 ELSE 0 END
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'pres.coach_streak_segments non_empty',
# MAGIC          CASE WHEN (SELECT COUNT(*) FROM naf_catalog.gold_presentation.coach_streak_detail) = 0 THEN 1 ELSE 0 END
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'pres.coach_streak_summary non_empty',
# MAGIC          CASE WHEN (SELECT COUNT(*) FROM naf_catalog.gold_presentation.coach_streak_overview) = 0 THEN 1 ELSE 0 END
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'pres.coach_results_all_long non_empty',
# MAGIC          CASE WHEN (SELECT COUNT(*) FROM naf_catalog.gold_presentation.coach_results_all_long) = 0 THEN 1 ELSE 0 END
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'pres.coach_opponent_insights non_empty',
# MAGIC          CASE WHEN (SELECT COUNT(*) FROM naf_catalog.gold_presentation.coach_opponent_insights) = 0 THEN 1 ELSE 0 END
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'pres.coach_global_elo_rating_history non_empty',
# MAGIC          CASE WHEN (SELECT COUNT(*) FROM naf_catalog.gold_presentation.coach_global_elo_rating_history) = 0 THEN 1 ELSE 0 END
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'pres.coach_race_elo_rating_history non_empty',
# MAGIC          CASE WHEN (SELECT COUNT(*) FROM naf_catalog.gold_presentation.coach_race_elo_rating_history) = 0 THEN 1 ELSE 0 END
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'pres.coach_global_elo_rating_daily_series non_empty',
# MAGIC          CASE WHEN (SELECT COUNT(*) FROM naf_catalog.gold_presentation.coach_global_elo_rating_daily_series) = 0 THEN 1 ELSE 0 END
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'pres.coach_race_elo_rating_daily_series non_empty',
# MAGIC          CASE WHEN (SELECT COUNT(*) FROM naf_catalog.gold_presentation.coach_race_elo_rating_daily_series) = 0 THEN 1 ELSE 0 END
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'pres.coach_cumulative_results_last_n_daily_series non_empty',
# MAGIC          CASE WHEN (SELECT COUNT(*) FROM naf_catalog.gold_presentation.coach_cumulative_results_last_n_daily_series) = 0 THEN 1 ELSE 0 END
# MAGIC
# MAGIC
# MAGIC   -- -------------------------
# MAGIC   -- Grain uniqueness checks
# MAGIC   -- -------------------------
# MAGIC   UNION ALL
# MAGIC   SELECT 'pres.coach_profile PK duplicates (coach_id)',
# MAGIC          COUNT(*) AS fail_rows
# MAGIC   FROM (
# MAGIC     SELECT coach_id
# MAGIC     FROM naf_catalog.gold_presentation.coach_profile
# MAGIC     GROUP BY coach_id
# MAGIC     HAVING COUNT(*) > 1
# MAGIC   )
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'pres.coach_race_performance PK duplicates (coach_id, race_id)',
# MAGIC          COUNT(*) AS fail_rows
# MAGIC   FROM (
# MAGIC     SELECT coach_id, race_id
# MAGIC     FROM naf_catalog.gold_presentation.coach_race_performance
# MAGIC     GROUP BY coach_id, race_id
# MAGIC     HAVING COUNT(*) > 1
# MAGIC   )
# MAGIC
# MAGIC   -- NOTE: coach_cumulative_results_daily_series has NO rating_system
# MAGIC   -- PK is (scope, race_id, coach_id, event_day, result_order)
# MAGIC   UNION ALL
# MAGIC   SELECT 'pres.coach_cumulative_results_daily_series PK duplicates (scope, race_id, coach_id, event_day, result_order)',
# MAGIC          COUNT(*) AS fail_rows
# MAGIC   FROM (
# MAGIC     SELECT scope, race_id, coach_id, event_day, result_order
# MAGIC     FROM naf_catalog.gold_presentation.coach_cumulative_results_daily_series
# MAGIC     GROUP BY scope, race_id, coach_id, event_day, result_order
# MAGIC     HAVING COUNT(*) > 1
# MAGIC   )
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'pres.coach_opponent_rating PK duplicates (coach_id, opponent_coach_id)',
# MAGIC          COUNT(*) AS fail_rows
# MAGIC   FROM (
# MAGIC     SELECT coach_id, opponent_coach_id
# MAGIC     FROM naf_catalog.gold_presentation.coach_opponent_rating
# MAGIC     GROUP BY coach_id, opponent_coach_id
# MAGIC     HAVING COUNT(*) > 1
# MAGIC   )
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'pres.coach_opponent_global_elo_bin_insights PK duplicates (coach_id, bin_scheme_id, bin_index)',
# MAGIC          COUNT(*) AS fail_rows
# MAGIC   FROM (
# MAGIC     SELECT coach_id, bin_scheme_id, bin_index
# MAGIC     FROM naf_catalog.gold_presentation.coach_opponent_global_elo_bin_insights
# MAGIC     GROUP BY coach_id, bin_scheme_id, bin_index
# MAGIC     HAVING COUNT(*) > 1
# MAGIC   )
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'pres.coach_opponent_global_elo_bin_results_long PK duplicates (coach_id, bin_scheme_id, bin_index, result_order)',
# MAGIC          COUNT(*) AS fail_rows
# MAGIC   FROM (
# MAGIC     SELECT coach_id, bin_scheme_id, bin_index, result_order
# MAGIC     FROM naf_catalog.gold_presentation.coach_opponent_global_elo_bin_results_long
# MAGIC     GROUP BY coach_id, bin_scheme_id, bin_index, result_order
# MAGIC     HAVING COUNT(*) > 1
# MAGIC   )
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'pres.coach_streak_segments PK duplicates (scope, coach_id, race_id, streak_type, group_id)',
# MAGIC          COUNT(*) AS fail_rows
# MAGIC   FROM (
# MAGIC     SELECT scope, coach_id, race_id, streak_type, group_id
# MAGIC     FROM naf_catalog.gold_presentation.coach_streak_detail
# MAGIC     GROUP BY scope, coach_id, race_id, streak_type, group_id
# MAGIC     HAVING COUNT(*) > 1
# MAGIC   )
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'pres.coach_streak_summary PK duplicates (scope, coach_id, race_id)',
# MAGIC          COUNT(*) AS fail_rows
# MAGIC   FROM (
# MAGIC     SELECT scope, coach_id, race_id
# MAGIC     FROM naf_catalog.gold_presentation.coach_streak_overview
# MAGIC     GROUP BY scope, coach_id, race_id
# MAGIC     HAVING COUNT(*) > 1
# MAGIC   )
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'pres.coach_results_all_long PK duplicates (coach_id, level, result_order)',
# MAGIC          COUNT(*) AS fail_rows
# MAGIC   FROM (
# MAGIC     SELECT coach_id, level, result_order
# MAGIC     FROM naf_catalog.gold_presentation.coach_results_all_long
# MAGIC     GROUP BY coach_id, level, result_order
# MAGIC     HAVING COUNT(*) > 1
# MAGIC   )
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'pres.coach_opponent_insights PK duplicates (coach_id, bin_index)',
# MAGIC          COUNT(*) AS fail_rows
# MAGIC   FROM (
# MAGIC     SELECT coach_id
# MAGIC     FROM naf_catalog.gold_presentation.coach_opponent_insights
# MAGIC     GROUP BY coach_id
# MAGIC     HAVING COUNT(*) > 1
# MAGIC   )
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'pres.coach_global_elo_rating_history PK duplicates (rating_system, coach_id, game_id)',
# MAGIC          COUNT(*) AS fail_rows
# MAGIC   FROM (
# MAGIC     SELECT rating_system, coach_id, game_id
# MAGIC     FROM naf_catalog.gold_presentation.coach_global_elo_rating_history
# MAGIC     GROUP BY rating_system, coach_id, game_id
# MAGIC     HAVING COUNT(*) > 1
# MAGIC   )
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'pres.coach_race_elo_rating_history PK duplicates (rating_system, coach_id, race_id, game_id)',
# MAGIC          COUNT(*) AS fail_rows
# MAGIC   FROM (
# MAGIC     SELECT rating_system, coach_id, race_id, game_id
# MAGIC     FROM naf_catalog.gold_presentation.coach_race_elo_rating_history
# MAGIC     GROUP BY rating_system, coach_id, race_id, game_id
# MAGIC     HAVING COUNT(*) > 1
# MAGIC   )
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'pres.coach_global_elo_rating_daily_series PK duplicates (rating_system, coach_id, game_date)',
# MAGIC          COUNT(*) AS fail_rows
# MAGIC   FROM (
# MAGIC     SELECT rating_system, coach_id, game_date
# MAGIC     FROM naf_catalog.gold_presentation.coach_global_elo_rating_daily_series
# MAGIC     GROUP BY rating_system, coach_id, game_date
# MAGIC     HAVING COUNT(*) > 1
# MAGIC   )
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'pres.coach_race_elo_rating_daily_series PK duplicates (rating_system, coach_id, race_id, game_date)',
# MAGIC          COUNT(*) AS fail_rows
# MAGIC   FROM (
# MAGIC     SELECT rating_system, coach_id, race_id, game_date
# MAGIC     FROM naf_catalog.gold_presentation.coach_race_elo_rating_daily_series
# MAGIC     GROUP BY rating_system, coach_id, race_id, game_date
# MAGIC     HAVING COUNT(*) > 1
# MAGIC   )
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'pres.coach_cumulative_results_last_n_daily_series PK duplicates (coach_id, event_day, result_order)',
# MAGIC          COUNT(*) AS fail_rows
# MAGIC   FROM (
# MAGIC     SELECT coach_id, event_day, result_order
# MAGIC     FROM naf_catalog.gold_presentation.coach_cumulative_results_last_n_daily_series
# MAGIC     GROUP BY coach_id, event_day, result_order
# MAGIC     HAVING COUNT(*) > 1
# MAGIC   )
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   check_name,
# MAGIC   fail_rows,
# MAGIC   CASE WHEN fail_rows = 0 THEN 'PASS' ELSE 'FAIL' END AS status
# MAGIC FROM checks
# MAGIC ORDER BY status DESC, check_name;
# MAGIC
