# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Summary notebook contract (`naf_catalog.gold_summary`)
# MAGIC
# MAGIC **Purpose:** Derived analytics layer. Materialize **reusable measures** (aggregates, windows, streaks, distributions) from `naf_catalog.gold_fact` at **declared, stable grains** for downstream `naf_catalog.gold_presentation` / dashboards.
# MAGIC
# MAGIC ## Rules (minimal)
# MAGIC - **IDs + measures only** (keys + numeric outputs + deltas + `*_frac` rates + ordering fields + timestamps + availability/validity flags)
# MAGIC - **No display attributes** (names/labels/ISO/UI formatting → `gold_presentation`)
# MAGIC - **Dates:** use `date_id` only (no `*_date`/`*_timestamp` as business date columns in outputs)
# MAGIC - **Rates:** `*_frac` is **0–1**; `*_pct` (**0–100**) is presentation-only
# MAGIC - **Don’t hide data:** keep metrics populated; add `has_*` / `is_valid_*` flags + explicit threshold columns for filtering/interpretation
# MAGIC - **No event truth duplication:** event detail stays in `gold_fact`; summary = derived measures at stated grain
# MAGIC
# MAGIC ## Naming conventions
# MAGIC - **`glo` = `global_elo`**: Abbreviated form used in column names (e.g., `avg_opponent_glo_peak`).
# MAGIC   Full prefix `global_elo_` used in the Global Elo summary table itself.
# MAGIC
# MAGIC ## Design decisions (331-specific)
# MAGIC - **`form_label` / `bin_label` in summary**: Intentionally kept here (not deferred to presentation)
# MAGIC   because the label thresholds are analytical rules, not UI formatting.
# MAGIC - **`coach_form_summary` reads from `gold_fact`**: Intentional — form score requires
# MAGIC   `score_expected` which only exists in `rating_history_fact`, not in any summary spine.
# MAGIC - **25-game specialist threshold**: Fixed rule (not config-driven). Documented in
# MAGIC   `Analytical_Parameters.md`. Used in `coach_race_relative_strength` and `coach_race_nation_rank`.
# MAGIC - **Opponent GLO bins**: Fixed 4-bin scheme (0-150 / 150-200 / 200-250 / 250+) in
# MAGIC   `coach_opponent_median_glo_bin_summary`. Legacy configurable bin framework removed.
# MAGIC
# MAGIC **Design authority (wins):**
# MAGIC - Project Design → `00_design_decisions.md`
# MAGIC - Project Design → `02_schema_design.md`
# MAGIC - Project Design → `03_style_guides.md`
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create schema gold_summary
# MAGIC %sql -- SCHEMA: naf_catalog.gold_summary
# MAGIC CREATE SCHEMA IF NOT EXISTS naf_catalog.gold_summary;
# MAGIC

# COMMAND ----------

# DBTITLE 1,gold_summary.coach_game_spine_v
# MAGIC %sql
# MAGIC -- VIEW: naf_catalog.gold_summary.coach_game_spine_v
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Canonical per-coach-per-game spine used by coach results aggregates.
# MAGIC -- LAYER        : GOLD_SUMMARY
# MAGIC -- CONTRACT TYPE: SPINE (minimal grain scaffold, no windows, no filters)
# MAGIC -- GRAIN        : 1 row per (coach_id, game_id).
# MAGIC -- PRIMARY KEY  : (coach_id, game_id)
# MAGIC -- SOURCES      : naf_catalog.gold_fact.coach_games_fact
# MAGIC -- NOTES        : - No filtering: downstream metrics apply metric-specific filters only.
# MAGIC --               - Uniqueness and NOT NULL key constraints must hold in the fact source.
# MAGIC --               - SILVER is responsible for deterministic keys, canonicalisation, and
# MAGIC --                 data hygiene that supports stable facts.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_summary.coach_game_spine_v AS
# MAGIC SELECT
# MAGIC   game_id,
# MAGIC   tournament_id,
# MAGIC
# MAGIC   date_id,
# MAGIC   event_timestamp,
# MAGIC
# MAGIC   coach_id,
# MAGIC   race_id,
# MAGIC
# MAGIC   opponent_coach_id,
# MAGIC   opponent_race_id,
# MAGIC
# MAGIC   result_numeric,
# MAGIC   td_for,
# MAGIC   td_against
# MAGIC FROM naf_catalog.gold_fact.coach_games_fact;
# MAGIC

# COMMAND ----------

# DBTITLE 1,gold_summary.coach_rating_history_spine_v
# MAGIC %sql
# MAGIC -- VIEW: naf_catalog.gold_summary.coach_rating_history_spine_v
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Canonical coach rating-history spine used by rating aggregates and timelines.
# MAGIC -- LAYER        : GOLD_SUMMARY
# MAGIC -- CONTRACT TYPE: SPINE (minimal grain scaffold, no windows, no filters)
# MAGIC -- GRAIN        : 1 row per (scope, rating_system, coach_id, race_id, game_id)
# MAGIC -- PRIMARY KEY  : (scope, rating_system, coach_id, race_id, game_id)
# MAGIC -- SOURCES      : naf_catalog.gold_fact.rating_history_fact
# MAGIC -- NOTES        : - No filtering: timelines must not be gated by validity/burn-in flags.
# MAGIC --               - Any burn-in / "post-threshold" logic must be applied only inside the
# MAGIC --                 specific metric outputs that require it.
# MAGIC --               - Uniqueness and NOT NULL key constraints must hold in gold_fact.rating_history_fact.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_summary.coach_rating_history_spine_v AS
# MAGIC SELECT
# MAGIC   scope,
# MAGIC   rating_system,
# MAGIC
# MAGIC   coach_id,
# MAGIC   race_id,
# MAGIC   opponent_coach_id,
# MAGIC   opponent_race_id,
# MAGIC
# MAGIC   game_id,
# MAGIC   game_index,
# MAGIC
# MAGIC   date_id,
# MAGIC   event_timestamp,
# MAGIC
# MAGIC   result_numeric,
# MAGIC
# MAGIC   rating_before,
# MAGIC   rating_after,
# MAGIC   rating_delta
# MAGIC FROM naf_catalog.gold_fact.rating_history_fact;
# MAGIC

# COMMAND ----------

# DBTITLE 1,gold_summary.coach_race_elo_rating_history_v
# MAGIC %sql
# MAGIC -- VIEW: naf_catalog.gold_summary.coach_race_elo_rating_history_v
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : RACE-scope Elo rating history projection for timelines and downstream analytics.
# MAGIC -- LAYER        : GOLD_SUMMARY
# MAGIC -- CONTRACT TYPE: PROJECTION (filtered slice of the rating-history spine)
# MAGIC -- GRAIN        : 1 row per (rating_system, coach_id, race_id, game_id) for RACE scope
# MAGIC -- PRIMARY KEY  : (rating_system, coach_id, race_id, game_id)
# MAGIC -- SOURCES      : naf_catalog.gold_summary.coach_rating_history_spine_v
# MAGIC -- NOTES        : - No filtering on burn-in/threshold flags. Timelines must remain complete.
# MAGIC --               - Thresholds apply only inside *_post_threshold metrics, never to timelines.
# MAGIC --               - race_id is the coach's race for the given game; opponent_* are event attributes.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_summary.coach_race_elo_rating_history_v AS
# MAGIC SELECT
# MAGIC   rating_system,
# MAGIC
# MAGIC   coach_id,
# MAGIC   race_id,
# MAGIC   opponent_coach_id,
# MAGIC   opponent_race_id,
# MAGIC
# MAGIC   game_id,
# MAGIC   game_index,
# MAGIC
# MAGIC   date_id,
# MAGIC   event_timestamp,
# MAGIC
# MAGIC   result_numeric,
# MAGIC
# MAGIC   rating_before,
# MAGIC   rating_after,
# MAGIC   rating_delta
# MAGIC FROM naf_catalog.gold_summary.coach_rating_history_spine_v
# MAGIC WHERE scope = 'RACE'
# MAGIC   AND rating_system = 'NAF_ELO';
# MAGIC

# COMMAND ----------

# DBTITLE 1,gold_summary.coach_global_elo_rating_history_v
# MAGIC %sql
# MAGIC -- VIEW: naf_catalog.gold_summary.coach_global_elo_rating_history_v
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : GLOBAL-scope Elo rating history projection for timelines and downstream analytics.
# MAGIC -- LAYER        : GOLD_SUMMARY
# MAGIC -- CONTRACT TYPE: PROJECTION (filtered slice of the rating-history spine)
# MAGIC -- GRAIN        : 1 row per (rating_system, coach_id, game_id) for GLOBAL scope
# MAGIC -- PRIMARY KEY  : (rating_system, coach_id, game_id)
# MAGIC -- SOURCES      : naf_catalog.gold_summary.coach_rating_history_spine_v
# MAGIC -- NOTES        : - No filtering on burn-in/threshold flags. Timelines must remain complete.
# MAGIC --               - Thresholds apply only inside *_post_threshold metrics, never to timelines.
# MAGIC --               - opponent_coach_id is an event attribute, not part of the key.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_summary.coach_global_elo_rating_history_v AS
# MAGIC SELECT
# MAGIC   rating_system,
# MAGIC
# MAGIC   coach_id,
# MAGIC   opponent_coach_id,
# MAGIC
# MAGIC   game_id,
# MAGIC   game_index,
# MAGIC
# MAGIC   date_id,
# MAGIC   event_timestamp,
# MAGIC
# MAGIC   result_numeric,
# MAGIC
# MAGIC   rating_before,
# MAGIC   rating_after,
# MAGIC   rating_delta
# MAGIC FROM naf_catalog.gold_summary.coach_rating_history_spine_v
# MAGIC WHERE scope = 'GLOBAL'
# MAGIC   AND rating_system = 'NAF_ELO';
# MAGIC

# COMMAND ----------

# DBTITLE 1,gold_summary.coach_global_elo_game_spine_v
# MAGIC %sql
# MAGIC -- VIEW: naf_catalog.gold_summary.coach_global_elo_game_spine_v
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : GLOBAL Elo rating-event KPI spine for downstream windows and aggregates
# MAGIC --               (adds outcome flags and retains rating deltas).
# MAGIC -- LAYER        : GOLD_SUMMARY
# MAGIC -- CONTRACT TYPE: KPI SPINE (enriched event scaffold, no filtering)
# MAGIC -- GRAIN        : 1 row per (rating_system, coach_id, game_id) for GLOBAL rating events.
# MAGIC -- PRIMARY KEY  : (rating_system, coach_id, game_id)
# MAGIC -- SOURCES      : naf_catalog.gold_summary.coach_global_elo_rating_history_v
# MAGIC -- NOTES        : - scope_race_id is the scope race context and is always 0 for GLOBAL.
# MAGIC --               - No burn-in/threshold filtering here; thresholds apply only inside *_post_threshold metrics.
# MAGIC --               - Downstream windowing should primarily order by game_index (rating sequence).
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_summary.coach_global_elo_game_spine_v AS
# MAGIC SELECT
# MAGIC   rating_system,
# MAGIC
# MAGIC   coach_id,
# MAGIC   opponent_coach_id,
# MAGIC   CAST(0 AS INT) AS scope_race_id,
# MAGIC
# MAGIC   result_numeric,
# MAGIC
# MAGIC   game_index,
# MAGIC   game_id,
# MAGIC   date_id,
# MAGIC   event_timestamp,
# MAGIC
# MAGIC   rating_before,
# MAGIC   rating_after,
# MAGIC   rating_delta,
# MAGIC
# MAGIC   CAST(CASE WHEN result_numeric = 1.0 THEN 1 ELSE 0 END AS INT) AS is_win,
# MAGIC   CAST(CASE WHEN result_numeric = 0.5 THEN 1 ELSE 0 END AS INT) AS is_draw,
# MAGIC   CAST(CASE WHEN result_numeric = 0.0 THEN 1 ELSE 0 END AS INT) AS is_loss
# MAGIC FROM naf_catalog.gold_summary.coach_global_elo_rating_history_v;
# MAGIC

# COMMAND ----------

# DBTITLE 1,gold_summary.coach_results_cumulative_series_v
# MAGIC %sql
# MAGIC -- VIEW: naf_catalog.gold_summary.coach_results_cumulative_series_v
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Canonical cumulative W/D/L series for trend metrics across GLOBAL and RACE scopes.
# MAGIC --               Includes end-of-day cumulative values per date for plotting (cum_*_latest).
# MAGIC -- LAYER        : GOLD_SUMMARY
# MAGIC -- CONTRACT TYPE: SERIES (time series with windows/cumulative)
# MAGIC -- GRAIN        : 1 row per (scope, coach_id, scope_race_id, game_id)
# MAGIC -- PRIMARY KEY  : (scope, coach_id, scope_race_id, game_id)
# MAGIC -- SOURCES      : naf_catalog.gold_summary.coach_game_spine_v
# MAGIC -- NOTES        : - Results-universe (not rating-system-scoped).
# MAGIC --               - No dedup + no cleansing filters here; Silver enforces uniqueness/validity.
# MAGIC --               - Ordering uses (date_id, event_timestamp, game_id) for deterministic cumulative windows.
# MAGIC --               - scope_race_id is the scope race context: 0 for GLOBAL, game race_id for RACE.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_summary.coach_results_cumulative_series_v AS
# MAGIC WITH base AS (
# MAGIC   SELECT
# MAGIC     game_id,
# MAGIC     date_id,
# MAGIC     event_timestamp,
# MAGIC     race_id AS game_race_id,
# MAGIC     coach_id,
# MAGIC     result_numeric
# MAGIC   FROM naf_catalog.gold_summary.coach_game_spine_v
# MAGIC ),
# MAGIC expanded AS (
# MAGIC   SELECT
# MAGIC     'GLOBAL'       AS scope,
# MAGIC     coach_id,
# MAGIC     CAST(0 AS INT) AS scope_race_id,
# MAGIC     game_id,
# MAGIC     date_id,
# MAGIC     event_timestamp,
# MAGIC     result_numeric
# MAGIC   FROM base
# MAGIC
# MAGIC   UNION ALL
# MAGIC
# MAGIC   SELECT
# MAGIC     'RACE'                    AS scope,
# MAGIC     coach_id,
# MAGIC     CAST(game_race_id AS INT) AS scope_race_id,
# MAGIC     game_id,
# MAGIC     date_id,
# MAGIC     event_timestamp,
# MAGIC     result_numeric
# MAGIC   FROM base
# MAGIC ),
# MAGIC cum AS (
# MAGIC   SELECT
# MAGIC     scope,
# MAGIC     coach_id,
# MAGIC     scope_race_id,
# MAGIC     game_id,
# MAGIC     date_id,
# MAGIC     event_timestamp,
# MAGIC     result_numeric,
# MAGIC
# MAGIC     CAST(
# MAGIC       SUM(CASE WHEN result_numeric = 1.0 THEN 1 ELSE 0 END)
# MAGIC         OVER (
# MAGIC           PARTITION BY scope, coach_id, scope_race_id
# MAGIC           ORDER BY date_id ASC, event_timestamp ASC NULLS LAST, game_id ASC
# MAGIC           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC         )
# MAGIC       AS INT
# MAGIC     ) AS cum_win,
# MAGIC
# MAGIC     CAST(
# MAGIC       SUM(CASE WHEN result_numeric = 0.5 THEN 1 ELSE 0 END)
# MAGIC         OVER (
# MAGIC           PARTITION BY scope, coach_id, scope_race_id
# MAGIC           ORDER BY date_id ASC, event_timestamp ASC NULLS LAST, game_id ASC
# MAGIC           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC         )
# MAGIC       AS INT
# MAGIC     ) AS cum_draw,
# MAGIC
# MAGIC     CAST(
# MAGIC       SUM(CASE WHEN result_numeric = 0.0 THEN 1 ELSE 0 END)
# MAGIC         OVER (
# MAGIC           PARTITION BY scope, coach_id, scope_race_id
# MAGIC           ORDER BY date_id ASC, event_timestamp ASC NULLS LAST, game_id ASC
# MAGIC           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC         )
# MAGIC       AS INT
# MAGIC     ) AS cum_loss
# MAGIC   FROM expanded
# MAGIC ),
# MAGIC eod AS (
# MAGIC   SELECT
# MAGIC     c.*,
# MAGIC
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY scope, coach_id, scope_race_id, date_id
# MAGIC       ORDER BY event_timestamp DESC NULLS LAST, game_id DESC
# MAGIC     ) AS rn_eod,
# MAGIC
# MAGIC     FIRST_VALUE(cum_win) OVER (
# MAGIC       PARTITION BY scope, coach_id, scope_race_id, date_id
# MAGIC       ORDER BY event_timestamp DESC NULLS LAST, game_id DESC
# MAGIC     ) AS cum_win_latest,
# MAGIC
# MAGIC     FIRST_VALUE(cum_draw) OVER (
# MAGIC       PARTITION BY scope, coach_id, scope_race_id, date_id
# MAGIC       ORDER BY event_timestamp DESC NULLS LAST, game_id DESC
# MAGIC     ) AS cum_draw_latest,
# MAGIC
# MAGIC     FIRST_VALUE(cum_loss) OVER (
# MAGIC       PARTITION BY scope, coach_id, scope_race_id, date_id
# MAGIC       ORDER BY event_timestamp DESC NULLS LAST, game_id DESC
# MAGIC     ) AS cum_loss_latest
# MAGIC   FROM cum c
# MAGIC )
# MAGIC SELECT
# MAGIC   scope,
# MAGIC   coach_id,
# MAGIC   scope_race_id,
# MAGIC   game_id,
# MAGIC   date_id,
# MAGIC   event_timestamp,
# MAGIC
# MAGIC   cum_win,
# MAGIC   cum_draw,
# MAGIC   cum_loss,
# MAGIC   (cum_win + cum_draw + cum_loss) AS cum_games,
# MAGIC
# MAGIC   cum_win_latest,
# MAGIC   cum_draw_latest,
# MAGIC   cum_loss_latest,
# MAGIC   (cum_win_latest + cum_draw_latest + cum_loss_latest) AS cum_games_latest,
# MAGIC
# MAGIC   (rn_eod = 1) AS is_end_of_day_row
# MAGIC FROM eod;
# MAGIC

# COMMAND ----------

# DBTITLE 1,gold_summary.coach_rating_race_summary
# MAGIC %sql
# MAGIC -- TABLE: naf_catalog.gold_summary.coach_rating_race_summary
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : RACE-scope Elo rating snapshots + distribution metrics per (rating_system, coach, race),
# MAGIC --               with phase-specific metrics (all / post-threshold / last-50).
# MAGIC -- LAYER        : GOLD_SUMMARY
# MAGIC -- CONTRACT TYPE: SUMMARY (materialised KPIs at declared grain)
# MAGIC -- GRAIN        : 1 row per (rating_system, coach_id, race_id)
# MAGIC -- PRIMARY KEY  : (rating_system, coach_id, race_id)
# MAGIC -- SOURCES      : naf_catalog.gold_summary.coach_race_elo_rating_history_v
# MAGIC -- NOTES        : - Keeps all rows (no filtering out small samples).
# MAGIC --               - Post-threshold metrics use games strictly AFTER the first N games (burn-in):
# MAGIC --                 game_number_asc > threshold_games (Elo burn-in = 25).
# MAGIC --               - Last-50 metrics cover the most recent min(50, games_with_race) games.
# MAGIC --                 Always populated (never NULL) — when games < window, equals all-games metrics.
# MAGIC --               - Prefer counts (games_with_race) + known thresholds/windows over validity flags.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_summary.coach_rating_race_summary
# MAGIC USING DELTA AS
# MAGIC
# MAGIC WITH params AS (
# MAGIC   SELECT
# MAGIC     threshold_race_elo   AS threshold_games,
# MAGIC     last_n_games_window  AS last_n_games
# MAGIC   FROM naf_catalog.gold_dim.analytical_config
# MAGIC ),
# MAGIC
# MAGIC hist AS (
# MAGIC   SELECT
# MAGIC     rating_system,
# MAGIC     coach_id,
# MAGIC     race_id,
# MAGIC     game_id,
# MAGIC     game_index,
# MAGIC     date_id,
# MAGIC     event_timestamp,
# MAGIC     rating_after
# MAGIC   FROM naf_catalog.gold_summary.coach_race_elo_rating_history_v
# MAGIC ),
# MAGIC
# MAGIC ordered AS (
# MAGIC   SELECT
# MAGIC     h.*,
# MAGIC
# MAGIC     COUNT(*) OVER (
# MAGIC       PARTITION BY rating_system, coach_id, race_id
# MAGIC     ) AS games_with_race,
# MAGIC
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY rating_system, coach_id, race_id
# MAGIC       ORDER BY game_index ASC NULLS LAST, game_id ASC
# MAGIC     ) AS game_number_asc,
# MAGIC
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY rating_system, coach_id, race_id
# MAGIC       ORDER BY game_index DESC NULLS LAST, game_id DESC
# MAGIC     ) AS game_number_desc
# MAGIC
# MAGIC   FROM hist h
# MAGIC ),
# MAGIC
# MAGIC current_per_pair AS (
# MAGIC   SELECT
# MAGIC     rating_system,
# MAGIC     coach_id,
# MAGIC     race_id,
# MAGIC     MAX(games_with_race) AS games_with_race,
# MAGIC
# MAGIC     MAX_BY(
# MAGIC       NAMED_STRUCT(
# MAGIC         'elo_current',                 rating_after,
# MAGIC         'elo_current_game_index',      game_index,
# MAGIC         'elo_current_date_id',         date_id,
# MAGIC         'elo_current_event_timestamp', event_timestamp,
# MAGIC         'elo_current_game_id',         game_id
# MAGIC       ),
# MAGIC       NAMED_STRUCT(
# MAGIC         'game_index', COALESCE(game_index, -1),
# MAGIC         'game_id',    game_id
# MAGIC       )
# MAGIC     ) AS cur
# MAGIC   FROM ordered
# MAGIC   GROUP BY rating_system, coach_id, race_id
# MAGIC ),
# MAGIC
# MAGIC -- 1) All games metrics
# MAGIC all_agg AS (
# MAGIC   SELECT
# MAGIC     rating_system,
# MAGIC     coach_id,
# MAGIC     race_id,
# MAGIC     MAX(rating_after)                    AS elo_peak_all,
# MAGIC     AVG(rating_after)                    AS elo_mean_all,
# MAGIC     PERCENTILE_APPROX(rating_after, 0.5) AS elo_median_all
# MAGIC   FROM ordered
# MAGIC   GROUP BY rating_system, coach_id, race_id
# MAGIC ),
# MAGIC
# MAGIC all_peak_meta AS (
# MAGIC   SELECT
# MAGIC     rating_system,
# MAGIC     coach_id,
# MAGIC     race_id,
# MAGIC     MAX_BY(
# MAGIC       NAMED_STRUCT(
# MAGIC         'elo_peak_all_game_index',      game_index,
# MAGIC         'elo_peak_all_date_id',         date_id,
# MAGIC         'elo_peak_all_event_timestamp', event_timestamp,
# MAGIC         'elo_peak_all_game_id',         game_id
# MAGIC       ),
# MAGIC       NAMED_STRUCT(
# MAGIC         'rating_after', rating_after,
# MAGIC         'game_index',   COALESCE(game_index, -1),
# MAGIC         'game_id',      game_id
# MAGIC       )
# MAGIC     ) AS peak_all
# MAGIC   FROM ordered
# MAGIC   GROUP BY rating_system, coach_id, race_id
# MAGIC ),
# MAGIC
# MAGIC -- 2) Post-threshold metrics (strictly after burn-in)
# MAGIC post_threshold AS (
# MAGIC   SELECT
# MAGIC     o.*
# MAGIC   FROM ordered o
# MAGIC   CROSS JOIN params p
# MAGIC   WHERE o.game_number_asc > p.threshold_games
# MAGIC ),
# MAGIC
# MAGIC post_agg AS (
# MAGIC   SELECT
# MAGIC     rating_system,
# MAGIC     coach_id,
# MAGIC     race_id,
# MAGIC     MAX(rating_after)                    AS elo_peak_post_threshold,
# MAGIC     AVG(rating_after)                    AS elo_mean_post_threshold,
# MAGIC     PERCENTILE_APPROX(rating_after, 0.5) AS elo_median_post_threshold
# MAGIC   FROM post_threshold
# MAGIC   GROUP BY rating_system, coach_id, race_id
# MAGIC ),
# MAGIC
# MAGIC post_peak_meta AS (
# MAGIC   SELECT
# MAGIC     rating_system,
# MAGIC     coach_id,
# MAGIC     race_id,
# MAGIC     MAX_BY(
# MAGIC       NAMED_STRUCT(
# MAGIC         'elo_peak_post_threshold_game_index',      game_index,
# MAGIC         'elo_peak_post_threshold_date_id',         date_id,
# MAGIC         'elo_peak_post_threshold_event_timestamp', event_timestamp,
# MAGIC         'elo_peak_post_threshold_game_id',         game_id
# MAGIC       ),
# MAGIC       NAMED_STRUCT(
# MAGIC         'rating_after', rating_after,
# MAGIC         'game_index',   COALESCE(game_index, -1),
# MAGIC         'game_id',      game_id
# MAGIC       )
# MAGIC     ) AS peak_post
# MAGIC   FROM post_threshold
# MAGIC   GROUP BY rating_system, coach_id, race_id
# MAGIC ),
# MAGIC
# MAGIC -- 3) Last-50 metrics (independent of threshold)
# MAGIC last_50 AS (
# MAGIC   SELECT
# MAGIC     o.*
# MAGIC   FROM ordered o
# MAGIC   CROSS JOIN params p
# MAGIC   WHERE o.game_number_desc <= p.last_n_games
# MAGIC ),
# MAGIC
# MAGIC last_50_agg AS (
# MAGIC   SELECT
# MAGIC     rating_system,
# MAGIC     coach_id,
# MAGIC     race_id,
# MAGIC     MAX(rating_after)                    AS elo_peak_last_50,
# MAGIC     AVG(rating_after)                    AS elo_mean_last_50,
# MAGIC     PERCENTILE_APPROX(rating_after, 0.5) AS elo_median_last_50
# MAGIC   FROM last_50
# MAGIC   GROUP BY rating_system, coach_id, race_id
# MAGIC ),
# MAGIC
# MAGIC last_50_peak_meta AS (
# MAGIC   SELECT
# MAGIC     rating_system,
# MAGIC     coach_id,
# MAGIC     race_id,
# MAGIC     MAX_BY(
# MAGIC       NAMED_STRUCT(
# MAGIC         'elo_peak_last_50_game_index',      game_index,
# MAGIC         'elo_peak_last_50_date_id',         date_id,
# MAGIC         'elo_peak_last_50_event_timestamp', event_timestamp,
# MAGIC         'elo_peak_last_50_game_id',         game_id
# MAGIC       ),
# MAGIC       NAMED_STRUCT(
# MAGIC         'rating_after', rating_after,
# MAGIC         'game_index',   COALESCE(game_index, -1),
# MAGIC         'game_id',      game_id
# MAGIC       )
# MAGIC     ) AS peak_last_50
# MAGIC   FROM last_50
# MAGIC   GROUP BY rating_system, coach_id, race_id
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   c.rating_system,
# MAGIC   c.coach_id,
# MAGIC   c.race_id,
# MAGIC
# MAGIC   -- Current
# MAGIC   c.cur.elo_current,
# MAGIC   c.cur.elo_current_game_index,
# MAGIC   c.cur.elo_current_date_id,
# MAGIC   c.cur.elo_current_event_timestamp,
# MAGIC   c.cur.elo_current_game_id,
# MAGIC
# MAGIC   -- All games
# MAGIC   a.elo_peak_all,
# MAGIC   ap.peak_all.elo_peak_all_game_index,
# MAGIC   ap.peak_all.elo_peak_all_date_id,
# MAGIC   ap.peak_all.elo_peak_all_event_timestamp,
# MAGIC   ap.peak_all.elo_peak_all_game_id,
# MAGIC   a.elo_mean_all,
# MAGIC   a.elo_median_all,
# MAGIC
# MAGIC   -- Post-threshold
# MAGIC   pa.elo_peak_post_threshold,
# MAGIC   pp.peak_post.elo_peak_post_threshold_game_index,
# MAGIC   pp.peak_post.elo_peak_post_threshold_date_id,
# MAGIC   pp.peak_post.elo_peak_post_threshold_event_timestamp,
# MAGIC   pp.peak_post.elo_peak_post_threshold_game_id,
# MAGIC   pa.elo_mean_post_threshold,
# MAGIC   pa.elo_median_post_threshold,
# MAGIC
# MAGIC   -- Last 50
# MAGIC   l50.elo_peak_last_50,
# MAGIC   lp.peak_last_50.elo_peak_last_50_game_index,
# MAGIC   lp.peak_last_50.elo_peak_last_50_date_id,
# MAGIC   lp.peak_last_50.elo_peak_last_50_event_timestamp,
# MAGIC   lp.peak_last_50.elo_peak_last_50_game_id,
# MAGIC   l50.elo_mean_last_50,
# MAGIC   l50.elo_median_last_50,
# MAGIC
# MAGIC   -- Counts + parameters
# MAGIC   CAST(c.games_with_race AS INT) AS games_with_race,
# MAGIC   p.threshold_games,
# MAGIC   p.last_n_games,
# MAGIC
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM current_per_pair c
# MAGIC CROSS JOIN params p
# MAGIC LEFT JOIN all_agg a
# MAGIC   ON c.rating_system = a.rating_system
# MAGIC  AND c.coach_id      = a.coach_id
# MAGIC  AND c.race_id       = a.race_id
# MAGIC LEFT JOIN all_peak_meta ap
# MAGIC   ON c.rating_system = ap.rating_system
# MAGIC  AND c.coach_id      = ap.coach_id
# MAGIC  AND c.race_id       = ap.race_id
# MAGIC LEFT JOIN post_agg pa
# MAGIC   ON c.rating_system = pa.rating_system
# MAGIC  AND c.coach_id      = pa.coach_id
# MAGIC  AND c.race_id       = pa.race_id
# MAGIC LEFT JOIN post_peak_meta pp
# MAGIC   ON c.rating_system = pp.rating_system
# MAGIC  AND c.coach_id      = pp.coach_id
# MAGIC  AND c.race_id       = pp.race_id
# MAGIC LEFT JOIN last_50_agg l50
# MAGIC   ON c.rating_system = l50.rating_system
# MAGIC  AND c.coach_id      = l50.coach_id
# MAGIC  AND c.race_id       = l50.race_id
# MAGIC LEFT JOIN last_50_peak_meta lp
# MAGIC   ON c.rating_system = lp.rating_system
# MAGIC  AND c.coach_id      = lp.coach_id
# MAGIC  AND c.race_id       = lp.race_id;
# MAGIC

# COMMAND ----------

# DBTITLE 1,gold_summary.coach_streak_segments
# MAGIC %sql
# MAGIC -- TABLE: naf_catalog.gold_summary.coach_streak_segments
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Compute streak segments (WIN / UNBEATEN) for GLOBAL and RACE scopes.
# MAGIC -- LAYER        : GOLD_SUMMARY
# MAGIC -- CONTRACT TYPE: DERIVED (segment table computed from the results spine)
# MAGIC -- GRAIN        : 1 row per (scope, coach_id, scope_race_id, streak_type, group_id).
# MAGIC -- PRIMARY KEY  : (scope, coach_id, scope_race_id, streak_type, group_id)
# MAGIC -- SOURCES      : naf_catalog.gold_summary.coach_game_spine_v
# MAGIC -- NOTES        : - GLOBAL rows use scope_race_id = 0 (scope race context).
# MAGIC --               - RACE rows use scope_race_id = race_id (playable races only).
# MAGIC --               - Determinism relies on game_order_timestamp then game_id for ordering.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_summary.coach_streak_segments
# MAGIC USING DELTA AS
# MAGIC WITH base AS (
# MAGIC   SELECT
# MAGIC     coach_id,
# MAGIC     race_id,
# MAGIC     tournament_id,
# MAGIC     game_id,
# MAGIC     date_id,
# MAGIC     event_timestamp,
# MAGIC     result_numeric,
# MAGIC     opponent_coach_id,
# MAGIC     COALESCE(
# MAGIC       event_timestamp,
# MAGIC       TO_TIMESTAMP(CAST(date_id AS STRING), 'yyyyMMdd')
# MAGIC     ) AS game_order_timestamp
# MAGIC   FROM naf_catalog.gold_summary.coach_game_spine_v
# MAGIC ),
# MAGIC scoped AS (
# MAGIC   SELECT
# MAGIC     'GLOBAL'       AS scope,
# MAGIC     coach_id,
# MAGIC     CAST(0 AS INT) AS scope_race_id,
# MAGIC     tournament_id,
# MAGIC     game_id,
# MAGIC     date_id,
# MAGIC     event_timestamp,
# MAGIC     result_numeric,
# MAGIC     opponent_coach_id,
# MAGIC     game_order_timestamp
# MAGIC   FROM base
# MAGIC
# MAGIC   UNION ALL
# MAGIC
# MAGIC   SELECT
# MAGIC     'RACE'               AS scope,
# MAGIC     coach_id,
# MAGIC     CAST(race_id AS INT) AS scope_race_id,
# MAGIC     tournament_id,
# MAGIC     game_id,
# MAGIC     date_id,
# MAGIC     event_timestamp,
# MAGIC     result_numeric,
# MAGIC     opponent_coach_id,
# MAGIC     game_order_timestamp
# MAGIC   FROM base
# MAGIC   WHERE race_id IS NOT NULL
# MAGIC     AND race_id <> 0
# MAGIC ),
# MAGIC ordered AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY scope, coach_id, scope_race_id
# MAGIC       ORDER BY game_order_timestamp ASC, game_id ASC
# MAGIC     ) AS scope_game_order
# MAGIC   FROM scoped
# MAGIC ),
# MAGIC flags AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     CAST(CASE WHEN result_numeric = 1.0 THEN 1 ELSE 0 END AS INT) AS is_win,
# MAGIC     CAST(CASE WHEN result_numeric = 0.0 THEN 1 ELSE 0 END AS INT) AS is_loss,
# MAGIC     CAST(CASE WHEN result_numeric = 1.0 THEN 0 ELSE 1 END AS INT) AS is_not_win
# MAGIC   FROM ordered
# MAGIC ),
# MAGIC seq AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     -- WIN streak: breaks on any non-win (draw or loss). result_numeric is never NULL.
# MAGIC     SUM(is_not_win) OVER (
# MAGIC       PARTITION BY scope, coach_id, scope_race_id
# MAGIC       ORDER BY scope_game_order
# MAGIC       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC     ) AS grp_win,
# MAGIC
# MAGIC     -- UNBEATEN streak: breaks on a loss
# MAGIC     SUM(is_loss) OVER (
# MAGIC       PARTITION BY scope, coach_id, scope_race_id
# MAGIC       ORDER BY scope_game_order
# MAGIC       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC     ) AS grp_unbeaten,
# MAGIC
# MAGIC     LEAD(result_numeric) OVER (
# MAGIC       PARTITION BY scope, coach_id, scope_race_id
# MAGIC       ORDER BY scope_game_order
# MAGIC     ) AS next_result_numeric,
# MAGIC
# MAGIC     LEAD(opponent_coach_id) OVER (
# MAGIC       PARTITION BY scope, coach_id, scope_race_id
# MAGIC       ORDER BY scope_game_order
# MAGIC     ) AS next_opponent_coach_id,
# MAGIC
# MAGIC     LEAD(tournament_id) OVER (
# MAGIC       PARTITION BY scope, coach_id, scope_race_id
# MAGIC       ORDER BY scope_game_order
# MAGIC     ) AS next_tournament_id
# MAGIC   FROM flags
# MAGIC ),
# MAGIC win_segments AS (
# MAGIC   SELECT
# MAGIC     scope,
# MAGIC     coach_id,
# MAGIC     scope_race_id,
# MAGIC     CAST(grp_win AS INT) AS group_id,
# MAGIC
# MAGIC     MIN(scope_game_order) AS first_game_order,
# MAGIC     MAX(scope_game_order) AS last_game_order,
# MAGIC     MIN(date_id)          AS start_date_id,
# MAGIC     MAX(date_id)          AS end_date_id,
# MAGIC     CAST(COUNT(*) AS INT) AS length,
# MAGIC
# MAGIC     MAX_BY(next_result_numeric, scope_game_order)    AS end_next_result_numeric,
# MAGIC     MAX_BY(next_opponent_coach_id, scope_game_order) AS ended_by_opponent_coach_id,
# MAGIC     MAX_BY(next_tournament_id, scope_game_order)     AS ended_by_tournament_id
# MAGIC   FROM seq
# MAGIC   WHERE is_win = 1
# MAGIC   GROUP BY scope, coach_id, scope_race_id, grp_win
# MAGIC ),
# MAGIC unbeaten_segments AS (
# MAGIC   SELECT
# MAGIC     scope,
# MAGIC     coach_id,
# MAGIC     scope_race_id,
# MAGIC     CAST(grp_unbeaten AS INT) AS group_id,
# MAGIC
# MAGIC     MIN(scope_game_order) AS first_game_order,
# MAGIC     MAX(scope_game_order) AS last_game_order,
# MAGIC     MIN(date_id)          AS start_date_id,
# MAGIC     MAX(date_id)          AS end_date_id,
# MAGIC     CAST(COUNT(*) AS INT) AS length,
# MAGIC
# MAGIC     MAX_BY(next_result_numeric, scope_game_order)    AS end_next_result_numeric,
# MAGIC     MAX_BY(next_opponent_coach_id, scope_game_order) AS ended_by_opponent_coach_id,
# MAGIC     MAX_BY(next_tournament_id, scope_game_order)     AS ended_by_tournament_id
# MAGIC   FROM seq
# MAGIC   WHERE is_loss = 0
# MAGIC   GROUP BY scope, coach_id, scope_race_id, grp_unbeaten
# MAGIC )
# MAGIC SELECT
# MAGIC   scope,
# MAGIC   coach_id,
# MAGIC   scope_race_id,
# MAGIC   'WIN' AS streak_type,
# MAGIC   group_id,
# MAGIC   first_game_order,
# MAGIC   last_game_order,
# MAGIC   start_date_id,
# MAGIC   end_date_id,
# MAGIC   length,
# MAGIC   CASE
# MAGIC     WHEN end_next_result_numeric IS NULL THEN 'ACTIVE'
# MAGIC     WHEN end_next_result_numeric = 0.0  THEN 'LOSS'
# MAGIC     WHEN end_next_result_numeric = 0.5  THEN 'DRAW'
# MAGIC     ELSE 'UNKNOWN'
# MAGIC   END AS ended_by_result,
# MAGIC   ended_by_opponent_coach_id,
# MAGIC   ended_by_tournament_id,
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM win_segments
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   scope,
# MAGIC   coach_id,
# MAGIC   scope_race_id,
# MAGIC   'UNBEATEN' AS streak_type,
# MAGIC   group_id,
# MAGIC   first_game_order,
# MAGIC   last_game_order,
# MAGIC   start_date_id,
# MAGIC   end_date_id,
# MAGIC   length,
# MAGIC   CASE
# MAGIC     WHEN end_next_result_numeric IS NULL THEN 'ACTIVE'
# MAGIC     WHEN end_next_result_numeric = 0.0  THEN 'LOSS'
# MAGIC     ELSE 'UNKNOWN'
# MAGIC   END AS ended_by_result,
# MAGIC   ended_by_opponent_coach_id,
# MAGIC   ended_by_tournament_id,
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM unbeaten_segments;
# MAGIC

# COMMAND ----------

# DBTITLE 1,gold_summary.coach_streak_summary
# MAGIC %sql
# MAGIC -- TABLE: naf_catalog.gold_summary.coach_streak_summary
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Streak rollup per scope (current and max lengths).
# MAGIC -- LAYER        : GOLD_SUMMARY
# MAGIC -- CONTRACT TYPE: SUMMARY (materialised KPIs at declared grain)
# MAGIC -- GRAIN        : 1 row per (scope, coach_id, scope_race_id).
# MAGIC -- PRIMARY KEY  : (scope, coach_id, scope_race_id)
# MAGIC -- SOURCES      : naf_catalog.gold_summary.coach_streak_segments,
# MAGIC --               naf_catalog.gold_dim.coach_dim (GLOBAL coach universe)
# MAGIC -- NOTES        : - GLOBAL uses scope_race_id = 0; RACE uses playable scope_race_id <> 0.
# MAGIC --               - Adds has_streak_data flag for safe dashboard rendering.
# MAGIC --               - Streak KPI logic must only come from coach_streak_segments (single source of truth).
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_summary.coach_streak_summary
# MAGIC USING DELTA AS
# MAGIC
# MAGIC WITH spine AS (
# MAGIC   -- GLOBAL: all coaches (even if they have no streak segments yet)
# MAGIC   SELECT
# MAGIC     'GLOBAL' AS scope,
# MAGIC     coach_id,
# MAGIC     CAST(0 AS INT) AS scope_race_id
# MAGIC   FROM (
# MAGIC     SELECT DISTINCT coach_id
# MAGIC     FROM naf_catalog.gold_dim.coach_dim
# MAGIC     WHERE coach_id IS NOT NULL
# MAGIC       AND coach_id <> 0
# MAGIC   ) AS d
# MAGIC
# MAGIC   UNION ALL
# MAGIC
# MAGIC   -- RACE: only coach/race pairs that actually appear in games (prevents explosion)
# MAGIC   SELECT
# MAGIC     'RACE' AS scope,
# MAGIC     coach_id,
# MAGIC     CAST(race_id AS INT) AS scope_race_id
# MAGIC   FROM (
# MAGIC     SELECT DISTINCT coach_id, race_id
# MAGIC     FROM naf_catalog.gold_summary.coach_game_spine_v
# MAGIC     WHERE coach_id IS NOT NULL
# MAGIC       AND race_id IS NOT NULL
# MAGIC       AND race_id <> 0
# MAGIC   ) g
# MAGIC ),
# MAGIC agg AS (
# MAGIC   SELECT
# MAGIC     scope,
# MAGIC     coach_id,
# MAGIC     scope_race_id,
# MAGIC
# MAGIC     CAST(COUNT(*) AS INT) AS seg_cnt,
# MAGIC
# MAGIC     MAX(CASE WHEN streak_type = 'WIN'      THEN length END) AS best_win_streak,
# MAGIC     MAX(CASE WHEN streak_type = 'UNBEATEN' THEN length END) AS best_unbeaten_streak,
# MAGIC
# MAGIC     MAX(CASE WHEN streak_type = 'WIN'
# MAGIC               AND ended_by_result = 'ACTIVE' THEN length END) AS current_win_streak,
# MAGIC     MAX(CASE WHEN streak_type = 'UNBEATEN'
# MAGIC               AND ended_by_result = 'ACTIVE' THEN length END) AS current_unbeaten_streak
# MAGIC   FROM naf_catalog.gold_summary.coach_streak_segments
# MAGIC   WHERE streak_type IN ('WIN', 'UNBEATEN')
# MAGIC   GROUP BY scope, coach_id, scope_race_id
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   s.scope,
# MAGIC   s.coach_id,
# MAGIC   s.scope_race_id,
# MAGIC
# MAGIC   CAST(COALESCE(a.seg_cnt, 0) > 0 AS BOOLEAN) AS has_streak_data,
# MAGIC
# MAGIC   CAST(COALESCE(a.best_win_streak, 0) AS INT) AS best_win_streak,
# MAGIC   CAST(COALESCE(a.best_unbeaten_streak, 0) AS INT) AS best_unbeaten_streak,
# MAGIC
# MAGIC   CAST(COALESCE(a.current_win_streak, 0) AS INT) AS current_win_streak,
# MAGIC   CAST(COALESCE(a.current_unbeaten_streak, 0) AS INT) AS current_unbeaten_streak,
# MAGIC
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM spine s
# MAGIC LEFT JOIN agg a
# MAGIC   ON s.scope = a.scope
# MAGIC  AND s.coach_id = a.coach_id
# MAGIC  AND s.scope_race_id = a.scope_race_id;
# MAGIC

# COMMAND ----------

# DBTITLE 1,gold_summary.coach_rating_global_elo_summary
# MAGIC %sql
# MAGIC -- *TABLE*: naf_catalog.gold_summary.coach_rating_global_elo_summary
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE
# MAGIC --   GLOBAL Elo rating snapshot + distribution metrics per (rating_system, coach),
# MAGIC --   with phase-specific metrics (all / post-threshold / last-50).
# MAGIC --
# MAGIC -- LAYER        : GOLD_SUMMARY
# MAGIC -- CONTRACT TYPE: SUMMARY (materialised KPIs at declared grain)
# MAGIC -- GRAIN        : 1 row per (rating_system, coach_id)
# MAGIC -- PRIMARY KEY  : (rating_system, coach_id)
# MAGIC -- SOURCES      : naf_catalog.gold_summary.coach_global_elo_rating_history_v
# MAGIC -- NOTES        : - No rows are filtered out for small samples
# MAGIC --               - Post-threshold uses games strictly AFTER the first N games (burn-in):
# MAGIC --                 game_number_asc > threshold_games
# MAGIC --               - Last-50 covers the most recent min(50, global_elo_games) games.
# MAGIC --                 Always populated (never NULL) — when games < window, equals all-games metrics.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_summary.coach_rating_global_elo_summary
# MAGIC USING DELTA AS
# MAGIC
# MAGIC WITH params AS (
# MAGIC   SELECT
# MAGIC     threshold_global_elo AS threshold_games,
# MAGIC     last_n_games_window                       -- rolling window
# MAGIC   FROM naf_catalog.gold_dim.analytical_config
# MAGIC ),
# MAGIC
# MAGIC hist AS (
# MAGIC   SELECT
# MAGIC     rating_system,
# MAGIC     coach_id,
# MAGIC     game_id,
# MAGIC     game_index,
# MAGIC     date_id,
# MAGIC     event_timestamp,
# MAGIC     rating_after
# MAGIC   FROM naf_catalog.gold_summary.coach_global_elo_rating_history_v
# MAGIC   WHERE coach_id IS NOT NULL
# MAGIC     AND game_id  IS NOT NULL
# MAGIC     AND rating_after IS NOT NULL
# MAGIC ),
# MAGIC
# MAGIC ordered AS (
# MAGIC   SELECT
# MAGIC     h.*,
# MAGIC
# MAGIC     COUNT(*) OVER (
# MAGIC       PARTITION BY rating_system, coach_id
# MAGIC     ) AS global_elo_games,
# MAGIC
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY rating_system, coach_id
# MAGIC       ORDER BY game_index ASC NULLS LAST, game_id ASC
# MAGIC     ) AS game_number_asc,
# MAGIC
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY rating_system, coach_id
# MAGIC       ORDER BY game_index DESC NULLS LAST, game_id DESC
# MAGIC     ) AS game_number_desc
# MAGIC   FROM hist h
# MAGIC ),
# MAGIC
# MAGIC current_per_coach AS (
# MAGIC   SELECT
# MAGIC     rating_system,
# MAGIC     coach_id,
# MAGIC     MAX(global_elo_games) AS global_elo_games,
# MAGIC
# MAGIC     MAX_BY(
# MAGIC       NAMED_STRUCT(
# MAGIC         'global_elo_current',                 rating_after,
# MAGIC         'global_elo_current_game_index',      game_index,
# MAGIC         'global_elo_current_date_id',         date_id,
# MAGIC         'global_elo_current_event_timestamp', event_timestamp,
# MAGIC         'global_elo_current_game_id',         game_id
# MAGIC       ),
# MAGIC       NAMED_STRUCT(
# MAGIC         'game_index', COALESCE(game_index, -1),
# MAGIC         'game_id',    game_id
# MAGIC       )
# MAGIC     ) AS cur
# MAGIC   FROM ordered
# MAGIC   GROUP BY rating_system, coach_id
# MAGIC ),
# MAGIC
# MAGIC -- 1) All games metrics
# MAGIC all_agg AS (
# MAGIC   SELECT
# MAGIC     rating_system,
# MAGIC     coach_id,
# MAGIC     MAX(rating_after)                    AS global_elo_peak_all,
# MAGIC     AVG(rating_after)                    AS global_elo_mean_all,
# MAGIC     PERCENTILE_APPROX(rating_after, 0.5) AS global_elo_median_all
# MAGIC   FROM ordered
# MAGIC   GROUP BY rating_system, coach_id
# MAGIC ),
# MAGIC
# MAGIC all_peak_meta AS (
# MAGIC   SELECT
# MAGIC     rating_system,
# MAGIC     coach_id,
# MAGIC     MAX_BY(
# MAGIC       NAMED_STRUCT(
# MAGIC         'global_elo_peak_all_game_index',      game_index,
# MAGIC         'global_elo_peak_all_date_id',         date_id,
# MAGIC         'global_elo_peak_all_event_timestamp', event_timestamp,
# MAGIC         'global_elo_peak_all_game_id',         game_id
# MAGIC       ),
# MAGIC       NAMED_STRUCT(
# MAGIC         'rating_after', rating_after,
# MAGIC         'game_index',   COALESCE(game_index, -1),
# MAGIC         'game_id',      game_id
# MAGIC       )
# MAGIC     ) AS peak_all
# MAGIC   FROM ordered
# MAGIC   GROUP BY rating_system, coach_id
# MAGIC ),
# MAGIC
# MAGIC -- 2) Post-threshold metrics (strictly after burn-in)
# MAGIC post_threshold AS (
# MAGIC   SELECT o.*
# MAGIC   FROM ordered o
# MAGIC   CROSS JOIN params p
# MAGIC   WHERE o.game_number_asc > p.threshold_games
# MAGIC ),
# MAGIC
# MAGIC post_agg AS (
# MAGIC   SELECT
# MAGIC     rating_system,
# MAGIC     coach_id,
# MAGIC     MAX(rating_after)                    AS global_elo_peak_post_threshold,
# MAGIC     AVG(rating_after)                    AS global_elo_mean_post_threshold,
# MAGIC     PERCENTILE_APPROX(rating_after, 0.5) AS global_elo_median_post_threshold
# MAGIC   FROM post_threshold
# MAGIC   GROUP BY rating_system, coach_id
# MAGIC ),
# MAGIC
# MAGIC post_peak_meta AS (
# MAGIC   SELECT
# MAGIC     rating_system,
# MAGIC     coach_id,
# MAGIC     MAX_BY(
# MAGIC       NAMED_STRUCT(
# MAGIC         'global_elo_peak_post_threshold_game_index',      game_index,
# MAGIC         'global_elo_peak_post_threshold_date_id',         date_id,
# MAGIC         'global_elo_peak_post_threshold_event_timestamp', event_timestamp,
# MAGIC         'global_elo_peak_post_threshold_game_id',         game_id
# MAGIC       ),
# MAGIC       NAMED_STRUCT(
# MAGIC         'rating_after', rating_after,
# MAGIC         'game_index',   COALESCE(game_index, -1),
# MAGIC         'game_id',      game_id
# MAGIC       )
# MAGIC     ) AS peak_post_threshold
# MAGIC   FROM post_threshold
# MAGIC   GROUP BY rating_system, coach_id
# MAGIC ),
# MAGIC
# MAGIC -- 3) Last-50 metrics (most recent min(window, games) games)
# MAGIC last_50 AS (
# MAGIC   SELECT o.*
# MAGIC   FROM ordered o
# MAGIC   CROSS JOIN params p
# MAGIC   WHERE o.game_number_desc <= p.last_n_games_window
# MAGIC ),
# MAGIC
# MAGIC last_50_agg AS (
# MAGIC   SELECT
# MAGIC     rating_system,
# MAGIC     coach_id,
# MAGIC     MAX(rating_after)                    AS global_elo_peak_last_50,
# MAGIC     AVG(rating_after)                    AS global_elo_mean_last_50,
# MAGIC     PERCENTILE_APPROX(rating_after, 0.5) AS global_elo_median_last_50
# MAGIC   FROM last_50
# MAGIC   GROUP BY rating_system, coach_id
# MAGIC ),
# MAGIC
# MAGIC last_50_peak_meta AS (
# MAGIC   SELECT
# MAGIC     rating_system,
# MAGIC     coach_id,
# MAGIC     MAX_BY(
# MAGIC       NAMED_STRUCT(
# MAGIC         'global_elo_peak_last_50_game_index',      game_index,
# MAGIC         'global_elo_peak_last_50_date_id',         date_id,
# MAGIC         'global_elo_peak_last_50_event_timestamp', event_timestamp,
# MAGIC         'global_elo_peak_last_50_game_id',         game_id
# MAGIC       ),
# MAGIC       NAMED_STRUCT(
# MAGIC         'rating_after', rating_after,
# MAGIC         'game_index',   COALESCE(game_index, -1),
# MAGIC         'game_id',      game_id
# MAGIC       )
# MAGIC     ) AS peak_last_50
# MAGIC   FROM last_50
# MAGIC   GROUP BY rating_system, coach_id
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   c.rating_system,
# MAGIC   c.coach_id,
# MAGIC
# MAGIC   CAST(c.global_elo_games AS INT) AS global_elo_games,
# MAGIC
# MAGIC   -- Current
# MAGIC   CAST(c.cur.global_elo_current AS DOUBLE)             AS global_elo_current,
# MAGIC   CAST(c.cur.global_elo_current_game_index AS INT)     AS global_elo_current_game_index,
# MAGIC   CAST(c.cur.global_elo_current_date_id AS INT)        AS global_elo_current_date_id,
# MAGIC   c.cur.global_elo_current_event_timestamp             AS global_elo_current_event_timestamp,
# MAGIC   CAST(c.cur.global_elo_current_game_id AS BIGINT)     AS global_elo_current_game_id,
# MAGIC
# MAGIC   -- All games
# MAGIC   CAST(a.global_elo_peak_all AS DOUBLE)                AS global_elo_peak_all,
# MAGIC   CAST(ap.peak_all.global_elo_peak_all_game_index AS INT) AS global_elo_peak_all_game_index,
# MAGIC   CAST(ap.peak_all.global_elo_peak_all_date_id AS INT)    AS global_elo_peak_all_date_id,
# MAGIC   ap.peak_all.global_elo_peak_all_event_timestamp      AS global_elo_peak_all_event_timestamp,
# MAGIC   CAST(ap.peak_all.global_elo_peak_all_game_id AS BIGINT) AS global_elo_peak_all_game_id,
# MAGIC   CAST(a.global_elo_mean_all AS DOUBLE)                AS global_elo_mean_all,
# MAGIC   CAST(a.global_elo_median_all AS DOUBLE)              AS global_elo_median_all,
# MAGIC
# MAGIC   -- Post-threshold
# MAGIC   CAST(pa.global_elo_peak_post_threshold AS DOUBLE)    AS global_elo_peak_post_threshold,
# MAGIC   CAST(pp.peak_post_threshold.global_elo_peak_post_threshold_game_index AS INT) AS global_elo_peak_post_threshold_game_index,
# MAGIC   CAST(pp.peak_post_threshold.global_elo_peak_post_threshold_date_id AS INT)    AS global_elo_peak_post_threshold_date_id,
# MAGIC   pp.peak_post_threshold.global_elo_peak_post_threshold_event_timestamp         AS global_elo_peak_post_threshold_event_timestamp,
# MAGIC   CAST(pp.peak_post_threshold.global_elo_peak_post_threshold_game_id AS BIGINT) AS global_elo_peak_post_threshold_game_id,
# MAGIC   CAST(pa.global_elo_mean_post_threshold AS DOUBLE)    AS global_elo_mean_post_threshold,
# MAGIC   CAST(pa.global_elo_median_post_threshold AS DOUBLE)  AS global_elo_median_post_threshold,
# MAGIC
# MAGIC   -- Last 50 (definition: last min(window, games) games; if games < window, equals all-games)
# MAGIC   CAST(
# MAGIC     CASE
# MAGIC       WHEN c.global_elo_games < p.last_n_games_window THEN a.global_elo_peak_all
# MAGIC       ELSE l50.global_elo_peak_last_50
# MAGIC     END
# MAGIC   AS DOUBLE) AS global_elo_peak_last_50,
# MAGIC
# MAGIC   CAST(
# MAGIC     CASE
# MAGIC       WHEN c.global_elo_games < p.last_n_games_window THEN ap.peak_all.global_elo_peak_all_game_index
# MAGIC       ELSE lp.peak_last_50.global_elo_peak_last_50_game_index
# MAGIC     END
# MAGIC   AS INT) AS global_elo_peak_last_50_game_index,
# MAGIC
# MAGIC   CAST(
# MAGIC     CASE
# MAGIC       WHEN c.global_elo_games < p.last_n_games_window THEN ap.peak_all.global_elo_peak_all_date_id
# MAGIC       ELSE lp.peak_last_50.global_elo_peak_last_50_date_id
# MAGIC     END
# MAGIC   AS INT) AS global_elo_peak_last_50_date_id,
# MAGIC
# MAGIC   CASE
# MAGIC     WHEN c.global_elo_games < p.last_n_games_window THEN ap.peak_all.global_elo_peak_all_event_timestamp
# MAGIC     ELSE lp.peak_last_50.global_elo_peak_last_50_event_timestamp
# MAGIC   END AS global_elo_peak_last_50_event_timestamp,
# MAGIC
# MAGIC   CAST(
# MAGIC     CASE
# MAGIC       WHEN c.global_elo_games < p.last_n_games_window THEN ap.peak_all.global_elo_peak_all_game_id
# MAGIC       ELSE lp.peak_last_50.global_elo_peak_last_50_game_id
# MAGIC     END
# MAGIC   AS BIGINT) AS global_elo_peak_last_50_game_id,
# MAGIC
# MAGIC   CAST(
# MAGIC     CASE
# MAGIC       WHEN c.global_elo_games < p.last_n_games_window THEN a.global_elo_mean_all
# MAGIC       ELSE l50.global_elo_mean_last_50
# MAGIC     END
# MAGIC   AS DOUBLE) AS global_elo_mean_last_50,
# MAGIC
# MAGIC   CAST(
# MAGIC     CASE
# MAGIC       WHEN c.global_elo_games < p.last_n_games_window THEN a.global_elo_median_all
# MAGIC       ELSE l50.global_elo_median_last_50
# MAGIC     END
# MAGIC   AS DOUBLE) AS global_elo_median_last_50,
# MAGIC
# MAGIC   -- Parameters (no redundant validity flags in summary)
# MAGIC   p.threshold_games,
# MAGIC   p.last_n_games_window,
# MAGIC
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM current_per_coach c
# MAGIC CROSS JOIN params p
# MAGIC LEFT JOIN all_agg a
# MAGIC   ON c.rating_system = a.rating_system
# MAGIC  AND c.coach_id      = a.coach_id
# MAGIC LEFT JOIN all_peak_meta ap
# MAGIC   ON c.rating_system = ap.rating_system
# MAGIC  AND c.coach_id      = ap.coach_id
# MAGIC LEFT JOIN post_agg pa
# MAGIC   ON c.rating_system = pa.rating_system
# MAGIC  AND c.coach_id      = pa.coach_id
# MAGIC LEFT JOIN post_peak_meta pp
# MAGIC   ON c.rating_system = pp.rating_system
# MAGIC  AND c.coach_id      = pp.coach_id
# MAGIC LEFT JOIN last_50_agg l50
# MAGIC   ON c.rating_system = l50.rating_system
# MAGIC  AND c.coach_id      = l50.coach_id
# MAGIC LEFT JOIN last_50_peak_meta lp
# MAGIC   ON c.rating_system = lp.rating_system
# MAGIC  AND c.coach_id      = lp.coach_id
# MAGIC ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,gold_summary.coach_performance_summary
# MAGIC %sql
# MAGIC -- TABLE: naf_catalog.gold_summary.coach_performance_summary
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Coach-level performance totals, rates, and opponent strength context.
# MAGIC -- LAYER        : GOLD_SUMMARY
# MAGIC -- CONTRACT TYPE: SUMMARY (materialised KPIs at declared grain)
# MAGIC -- GRAIN        : 1 row per coach_id.
# MAGIC -- PRIMARY KEY  : (coach_id)
# MAGIC -- SOURCES      : naf_catalog.gold_summary.coach_game_spine_v,
# MAGIC --               naf_catalog.gold_dim.tournament_dim (nation_id only),
# MAGIC --               naf_catalog.gold_summary.coach_rating_global_elo_summary (opponent peaks)
# MAGIC -- NOTES        : - Uses date_id (keys) instead of display dates.
# MAGIC --               - No validity flags here; interpret using games_played + known thresholds.
# MAGIC --               - Uniqueness and NOT NULL key constraints must hold in the underlying fact/spine.
# MAGIC --               - Placed after coach_rating_global_elo_summary so opponent peaks are available.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_summary.coach_performance_summary
# MAGIC USING DELTA AS
# MAGIC WITH params AS (
# MAGIC   SELECT last_n_games_window
# MAGIC   FROM naf_catalog.gold_dim.analytical_config
# MAGIC ),
# MAGIC games AS (
# MAGIC   SELECT
# MAGIC     g.coach_id,
# MAGIC     g.game_id,
# MAGIC     g.tournament_id,
# MAGIC     g.opponent_coach_id,
# MAGIC     td.nation_id AS tournament_nation_id,
# MAGIC     g.date_id,
# MAGIC     g.event_timestamp,
# MAGIC     COALESCE(g.event_timestamp, TO_TIMESTAMP(CAST(g.date_id AS STRING), 'yyyyMMdd')) AS event_ts_coalesced,
# MAGIC     g.result_numeric,
# MAGIC     g.td_for,
# MAGIC     g.td_against,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY g.coach_id
# MAGIC       ORDER BY g.date_id DESC,
# MAGIC                COALESCE(g.event_timestamp, TO_TIMESTAMP(CAST(g.date_id AS STRING), 'yyyyMMdd')) DESC NULLS LAST,
# MAGIC                g.game_id DESC
# MAGIC     ) AS game_recency_rank
# MAGIC   FROM naf_catalog.gold_summary.coach_game_spine_v g
# MAGIC   LEFT JOIN naf_catalog.gold_dim.tournament_dim td
# MAGIC     ON g.tournament_id = td.tournament_id
# MAGIC ),
# MAGIC -- Opponent GLO peaks from the freshly-built rating summary.
# MAGIC opponent_peaks AS (
# MAGIC   SELECT coach_id, global_elo_peak_all AS opponent_glo_peak
# MAGIC   FROM naf_catalog.gold_summary.coach_rating_global_elo_summary
# MAGIC   WHERE rating_system = 'NAF_ELO'
# MAGIC ),
# MAGIC games_with_opp AS (
# MAGIC   SELECT
# MAGIC     g.*,
# MAGIC     op.opponent_glo_peak
# MAGIC   FROM games g
# MAGIC   LEFT JOIN opponent_peaks op
# MAGIC     ON g.opponent_coach_id = op.coach_id
# MAGIC ),
# MAGIC agg AS (
# MAGIC   SELECT
# MAGIC     coach_id,
# MAGIC
# MAGIC     CAST(COUNT(*) AS INT) AS games_played,
# MAGIC
# MAGIC     CAST(SUM(CASE WHEN result_numeric = 1.0 THEN 1 ELSE 0 END) AS INT) AS wins,
# MAGIC     CAST(SUM(CASE WHEN result_numeric = 0.5 THEN 1 ELSE 0 END) AS INT) AS draws,
# MAGIC     CAST(SUM(CASE WHEN result_numeric = 0.0 THEN 1 ELSE 0 END) AS INT) AS losses,
# MAGIC
# MAGIC     CAST(SUM(result_numeric) AS DECIMAL(12,2)) AS points_total,
# MAGIC     AVG(result_numeric)                        AS points_per_game,
# MAGIC
# MAGIC     CAST(SUM(td_for) AS INT)              AS td_for,
# MAGIC     CAST(SUM(td_against) AS INT)          AS td_against,
# MAGIC     CAST(SUM(td_for - td_against) AS INT) AS td_diff,
# MAGIC
# MAGIC     AVG(td_for)              AS td_for_per_game,
# MAGIC     AVG(td_against)          AS td_against_per_game,
# MAGIC     AVG(td_for - td_against) AS td_diff_per_game,
# MAGIC
# MAGIC     MIN(date_id) AS date_first_game_id,
# MAGIC     MAX(date_id) AS date_last_game_id,
# MAGIC
# MAGIC     max_by(
# MAGIC       event_ts_coalesced,
# MAGIC       struct(date_id, event_ts_coalesced, game_id)
# MAGIC     ) AS last_game_event_timestamp,
# MAGIC
# MAGIC     CAST(COUNT(DISTINCT tournament_id) AS INT)        AS tournaments_played,
# MAGIC     CAST(COUNT(DISTINCT tournament_nation_id) AS INT) AS nations_played,
# MAGIC
# MAGIC     -- Opponent strength: all games
# MAGIC     AVG(opponent_glo_peak)                            AS avg_opponent_glo_peak,
# MAGIC
# MAGIC     -- Opponent strength: last N games
# MAGIC     AVG(CASE WHEN game_recency_rank <= p.last_n_games_window
# MAGIC              THEN opponent_glo_peak END)              AS avg_opponent_glo_peak_last_50
# MAGIC
# MAGIC   FROM games_with_opp
# MAGIC   CROSS JOIN params p
# MAGIC   GROUP BY coach_id
# MAGIC )
# MAGIC SELECT
# MAGIC   coach_id,
# MAGIC
# MAGIC   games_played,
# MAGIC   wins,
# MAGIC   draws,
# MAGIC   losses,
# MAGIC
# MAGIC   points_total,
# MAGIC   points_per_game,
# MAGIC
# MAGIC   wins   / CAST(NULLIF(games_played, 0) AS DOUBLE) AS win_frac,
# MAGIC   draws  / CAST(NULLIF(games_played, 0) AS DOUBLE) AS draw_frac,
# MAGIC   losses / CAST(NULLIF(games_played, 0) AS DOUBLE) AS loss_frac,
# MAGIC
# MAGIC   td_for,
# MAGIC   td_against,
# MAGIC   td_diff,
# MAGIC   td_for_per_game,
# MAGIC   td_against_per_game,
# MAGIC   td_diff_per_game,
# MAGIC
# MAGIC   date_first_game_id,
# MAGIC   date_last_game_id,
# MAGIC   last_game_event_timestamp,
# MAGIC
# MAGIC   tournaments_played,
# MAGIC   COALESCE(nations_played, 0) AS nations_played,
# MAGIC
# MAGIC   avg_opponent_glo_peak,
# MAGIC   avg_opponent_glo_peak_last_50,
# MAGIC
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM agg;
# MAGIC

# COMMAND ----------

# DBTITLE 1,gold_summary.coach_form_summary
# MAGIC %sql
# MAGIC -- TABLE: naf_catalog.gold_summary.coach_form_summary
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Form score — measures how much a coach outperforms (or underperforms)
# MAGIC --               Elo expectations over their most recent games (GLOBAL scope).
# MAGIC -- LAYER        : GOLD_SUMMARY
# MAGIC -- CONTRACT TYPE: SUMMARY (materialised KPIs at declared grain)
# MAGIC -- GRAIN        : 1 row per coach_id.
# MAGIC -- PRIMARY KEY  : (coach_id)
# MAGIC -- SOURCES      : naf_catalog.gold_fact.rating_history_fact (GLOBAL scope),
# MAGIC --               naf_catalog.gold_dim.analytical_config
# MAGIC -- NOTES        : - form_score = SUM(actual_result - expected_result) over last N GLOBAL games.
# MAGIC --               - form_pctl = percentile rank among coaches with >= min_games in window.
# MAGIC --               - Coaches below the minimum threshold get NULL pctl/label (not excluded).
# MAGIC --               - Label thresholds: Strong (>=90), Good (>=70), Neutral (>=30), Poor (>=10), Weak (<10).
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_summary.coach_form_summary
# MAGIC USING DELTA AS
# MAGIC WITH params AS (
# MAGIC   SELECT
# MAGIC     form_window_games,
# MAGIC     form_min_games_for_pctl
# MAGIC   FROM naf_catalog.gold_dim.analytical_config
# MAGIC ),
# MAGIC -- All GLOBAL NAF_ELO games with recency rank per coach.
# MAGIC global_games AS (
# MAGIC   SELECT
# MAGIC     coach_id,
# MAGIC     game_index,
# MAGIC     result_numeric,
# MAGIC     score_expected,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY coach_id
# MAGIC       ORDER BY game_index DESC
# MAGIC     ) AS game_rn_desc
# MAGIC   FROM naf_catalog.gold_fact.rating_history_fact
# MAGIC   WHERE scope = 'GLOBAL'
# MAGIC     AND rating_system = 'NAF_ELO'
# MAGIC     AND COALESCE(race_id, 0) = 0
# MAGIC     AND coach_id IS NOT NULL
# MAGIC ),
# MAGIC -- Form score: sum of outperformance over last N games.
# MAGIC form_agg AS (
# MAGIC   SELECT
# MAGIC     g.coach_id,
# MAGIC     CAST(SUM(g.result_numeric - g.score_expected) AS DOUBLE) AS form_score,
# MAGIC     CAST(COUNT(*) AS INT)                                    AS form_games_in_window
# MAGIC   FROM global_games g
# MAGIC   CROSS JOIN params p
# MAGIC   WHERE g.game_rn_desc <= p.form_window_games
# MAGIC   GROUP BY g.coach_id
# MAGIC ),
# MAGIC -- Percentile rank among coaches with enough games in the window.
# MAGIC pctl_rank AS (
# MAGIC   SELECT
# MAGIC     fa.coach_id,
# MAGIC     fa.form_score,
# MAGIC     fa.form_games_in_window,
# MAGIC     PERCENT_RANK() OVER (ORDER BY fa.form_score ASC) * 100.0 AS form_pctl
# MAGIC   FROM form_agg fa
# MAGIC   CROSS JOIN params p
# MAGIC   WHERE fa.form_games_in_window >= p.form_min_games_for_pctl
# MAGIC )
# MAGIC SELECT
# MAGIC   fa.coach_id,
# MAGIC   fa.form_score,
# MAGIC   fa.form_games_in_window,
# MAGIC
# MAGIC   pr.form_pctl,
# MAGIC
# MAGIC   CASE
# MAGIC     WHEN pr.form_pctl IS NULL     THEN NULL
# MAGIC     WHEN pr.form_pctl >= 90.0     THEN 'Strong Form'
# MAGIC     WHEN pr.form_pctl >= 70.0     THEN 'Good Form'
# MAGIC     WHEN pr.form_pctl >= 30.0     THEN 'Neutral'
# MAGIC     WHEN pr.form_pctl >= 10.0     THEN 'Poor Form'
# MAGIC     ELSE                               'Weak Form'
# MAGIC   END AS form_label,
# MAGIC
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM form_agg fa
# MAGIC LEFT JOIN pctl_rank pr
# MAGIC   ON fa.coach_id = pr.coach_id;
# MAGIC

# COMMAND ----------

# DBTITLE 1,gold_summary.coach_opponent_summary
# MAGIC %sql
# MAGIC -- TABLE: naf_catalog.gold_summary.coach_opponent_summary
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : GLOBAL Elo matchup totals per opponent (W/D/L, points, rating exchange, last played).
# MAGIC -- LAYER        : GOLD_SUMMARY
# MAGIC -- GRAIN        : 1 row per (rating_system, coach_id, opponent_coach_id).
# MAGIC -- PRIMARY KEY  : (rating_system, coach_id, opponent_coach_id)
# MAGIC -- SOURCES      : naf_catalog.gold_summary.coach_global_elo_game_spine_v
# MAGIC -- NOTES        : Keeps all descriptive attributes out (presentation joins dims).
# MAGIC --               Uses date_id (keys) rather than game_date (display).
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_summary.coach_opponent_summary
# MAGIC USING DELTA AS
# MAGIC
# MAGIC WITH base AS (
# MAGIC   SELECT
# MAGIC     rating_system,
# MAGIC     coach_id,
# MAGIC     opponent_coach_id,
# MAGIC     game_index,
# MAGIC     game_id,
# MAGIC     date_id,
# MAGIC     event_timestamp,
# MAGIC     COALESCE(
# MAGIC       event_timestamp,
# MAGIC       TO_TIMESTAMP(CAST(date_id AS STRING), 'yyyyMMdd')
# MAGIC     ) AS event_ts_coalesced,
# MAGIC     result_numeric,
# MAGIC     rating_delta,
# MAGIC     is_win,
# MAGIC     is_draw,
# MAGIC     is_loss
# MAGIC   FROM naf_catalog.gold_summary.coach_global_elo_game_spine_v
# MAGIC   WHERE opponent_coach_id IS NOT NULL
# MAGIC ),
# MAGIC
# MAGIC agg AS (
# MAGIC   SELECT
# MAGIC     rating_system,
# MAGIC     coach_id,
# MAGIC     opponent_coach_id,
# MAGIC
# MAGIC     CAST(COUNT(*) AS INT) AS games_played,
# MAGIC     CAST(SUM(is_win)  AS INT) AS wins,
# MAGIC     CAST(SUM(is_draw) AS INT) AS draws,
# MAGIC     CAST(SUM(is_loss) AS INT) AS losses,
# MAGIC
# MAGIC     CAST(SUM(result_numeric) AS DECIMAL(12,2)) AS points_total,
# MAGIC     AVG(result_numeric) AS points_per_game,
# MAGIC
# MAGIC     CASE WHEN COUNT(*) > 0 THEN SUM(is_win)  / CAST(COUNT(*) AS DOUBLE) END AS win_frac,
# MAGIC     CASE WHEN COUNT(*) > 0 THEN SUM(is_draw) / CAST(COUNT(*) AS DOUBLE) END AS draw_frac,
# MAGIC     CASE WHEN COUNT(*) > 0 THEN SUM(is_loss) / CAST(COUNT(*) AS DOUBLE) END AS loss_frac,
# MAGIC
# MAGIC     MAX_BY(
# MAGIC       NAMED_STRUCT(
# MAGIC         'last_game_index',            game_index,
# MAGIC         'last_game_id',               game_id,
# MAGIC         'last_game_date_id',          date_id,
# MAGIC         'last_game_event_timestamp',  event_ts_coalesced
# MAGIC       ),
# MAGIC       NAMED_STRUCT(
# MAGIC         'event_timestamp', COALESCE(event_ts_coalesced, TIMESTAMP('1900-01-01')),
# MAGIC         'date_id',         COALESCE(date_id, -1),
# MAGIC         'game_index',      COALESCE(game_index, -1),
# MAGIC         'game_id',         game_id
# MAGIC       )
# MAGIC     ) AS last_game,
# MAGIC
# MAGIC     SUM(rating_delta) AS global_elo_exchange_total,
# MAGIC     AVG(rating_delta) AS global_elo_exchange_mean
# MAGIC
# MAGIC   FROM base
# MAGIC   GROUP BY rating_system, coach_id, opponent_coach_id
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   rating_system,
# MAGIC   coach_id,
# MAGIC   opponent_coach_id,
# MAGIC
# MAGIC   games_played,
# MAGIC   wins,
# MAGIC   draws,
# MAGIC   losses,
# MAGIC
# MAGIC   points_total,
# MAGIC   points_per_game,
# MAGIC
# MAGIC   win_frac,
# MAGIC   draw_frac,
# MAGIC   loss_frac,
# MAGIC
# MAGIC   last_game.last_game_index           AS last_game_index,
# MAGIC   last_game.last_game_id              AS last_game_id,
# MAGIC   last_game.last_game_date_id         AS last_game_date_id,
# MAGIC   last_game.last_game_event_timestamp AS last_game_event_timestamp,
# MAGIC
# MAGIC   global_elo_exchange_total,
# MAGIC   global_elo_exchange_mean,
# MAGIC
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM agg;
# MAGIC

# COMMAND ----------

# DBTITLE 1,gold_summary.coach_opponent_global_elo_enriched_v
# MAGIC %sql
# MAGIC -- VIEW: naf_catalog.gold_summary.coach_opponent_global_elo_enriched_v
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Opponent-enriched GLOBAL Elo matchup summary for opponent analytics and binning.
# MAGIC -- LAYER        : GOLD_SUMMARY
# MAGIC -- GRAIN        : 1 row per (rating_system, coach_id, opponent_coach_id).
# MAGIC -- PRIMARY KEY  : (rating_system, coach_id, opponent_coach_id)
# MAGIC -- SOURCES      : naf_catalog.gold_summary.coach_opponent_summary,
# MAGIC --                naf_catalog.gold_summary.coach_rating_global_elo_summary
# MAGIC -- NOTES        : opponent_global_elo_for_binning uses global_elo_peak_all (fallback global_elo_current).
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_summary.coach_opponent_global_elo_enriched_v AS
# MAGIC SELECT
# MAGIC   s.rating_system,
# MAGIC   s.coach_id,
# MAGIC   s.opponent_coach_id,
# MAGIC
# MAGIC   s.games_played,
# MAGIC   s.wins,
# MAGIC   s.draws,
# MAGIC   s.losses,
# MAGIC   (s.wins * 1.0D + s.draws * 0.5D) AS win_points,
# MAGIC
# MAGIC   s.global_elo_exchange_total,
# MAGIC   s.global_elo_exchange_mean,
# MAGIC
# MAGIC   rg.global_elo_current    AS opponent_global_elo_current,
# MAGIC   rg.global_elo_peak_all   AS opponent_global_elo_peak_all,
# MAGIC   rg.global_elo_median_all AS opponent_global_elo_median_all,
# MAGIC
# MAGIC   COALESCE(rg.global_elo_peak_all, rg.global_elo_current) AS opponent_global_elo_for_binning,
# MAGIC   COALESCE(rg.global_elo_median_all, rg.global_elo_current) AS opponent_global_elo_for_binning_median,
# MAGIC
# MAGIC   rg.global_elo_games       AS opponent_global_elo_games,
# MAGIC   rg.threshold_games        AS opponent_threshold_games,
# MAGIC   rg.last_n_games_window    AS opponent_last_n_games_window,
# MAGIC
# MAGIC   s.load_timestamp
# MAGIC FROM naf_catalog.gold_summary.coach_opponent_summary s
# MAGIC LEFT JOIN naf_catalog.gold_summary.coach_rating_global_elo_summary rg
# MAGIC   ON s.rating_system = rg.rating_system
# MAGIC  AND s.opponent_coach_id = rg.coach_id
# MAGIC ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,gold_summary.coach_opponent_median_glo_bin_summary
# MAGIC %sql
# MAGIC -- TABLE: naf_catalog.gold_summary.coach_opponent_median_glo_bin_summary
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Coach-level W/D/L by opponent MEDIAN GLO bins (fixed bins).
# MAGIC --                Same pattern as nation_opponent_elo_bin_wdl (332).
# MAGIC --                Fixed 4-bin scheme: 0-150, 150-200, 200-250, 250+
# MAGIC -- LAYER        : GOLD_SUMMARY
# MAGIC -- GRAIN        : 1 row per (coach_id, bin_index)
# MAGIC -- PRIMARY KEY  : (coach_id, bin_index)
# MAGIC -- SOURCES      : naf_catalog.gold_summary.coach_opponent_global_elo_enriched_v
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_summary.coach_opponent_median_glo_bin_summary
# MAGIC USING DELTA AS
# MAGIC
# MAGIC WITH bin_def AS (
# MAGIC   SELECT * FROM (VALUES
# MAGIC     (1, 0.0,    150.0, '0–150'),
# MAGIC     (2, 150.0,  200.0, '150–200'),
# MAGIC     (3, 200.0,  250.0, '200–250'),
# MAGIC     (4, 250.0, 9999.0, '250+')
# MAGIC   ) AS t(bin_index, bin_min, bin_max, bin_label)
# MAGIC ),
# MAGIC
# MAGIC coach_spine AS (
# MAGIC   SELECT DISTINCT coach_id
# MAGIC   FROM naf_catalog.gold_summary.coach_opponent_global_elo_enriched_v
# MAGIC   WHERE rating_system = 'NAF_ELO'
# MAGIC ),
# MAGIC
# MAGIC full_spine AS (
# MAGIC   SELECT cs.coach_id, bd.bin_index, bd.bin_min, bd.bin_max, bd.bin_label
# MAGIC   FROM coach_spine cs
# MAGIC   CROSS JOIN bin_def bd
# MAGIC ),
# MAGIC
# MAGIC game_data AS (
# MAGIC   SELECT
# MAGIC     e.coach_id,
# MAGIC     e.opponent_coach_id,
# MAGIC     e.games_played,
# MAGIC     e.wins,
# MAGIC     e.draws,
# MAGIC     e.losses,
# MAGIC     e.win_points,
# MAGIC     e.opponent_global_elo_for_binning_median AS opp_median_glo
# MAGIC   FROM naf_catalog.gold_summary.coach_opponent_global_elo_enriched_v e
# MAGIC   WHERE e.rating_system = 'NAF_ELO'
# MAGIC     AND e.opponent_global_elo_for_binning_median IS NOT NULL
# MAGIC ),
# MAGIC
# MAGIC binned AS (
# MAGIC   SELECT
# MAGIC     g.coach_id,
# MAGIC     bd.bin_index,
# MAGIC     CAST(COUNT(DISTINCT g.opponent_coach_id) AS INT) AS opponents_count,
# MAGIC     CAST(SUM(g.games_played) AS INT) AS games_played,
# MAGIC     CAST(SUM(g.wins) AS INT)         AS wins,
# MAGIC     CAST(SUM(g.draws) AS INT)        AS draws,
# MAGIC     CAST(SUM(g.losses) AS INT)       AS losses,
# MAGIC     CAST(SUM(g.win_points) AS DECIMAL(12,2)) AS win_points
# MAGIC   FROM game_data g
# MAGIC   INNER JOIN bin_def bd
# MAGIC     ON g.opp_median_glo >= bd.bin_min
# MAGIC    AND g.opp_median_glo <  bd.bin_max
# MAGIC   GROUP BY g.coach_id, bd.bin_index
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   sp.coach_id,
# MAGIC   sp.bin_index,
# MAGIC   sp.bin_min,
# MAGIC   sp.bin_max,
# MAGIC   sp.bin_label,
# MAGIC   COALESCE(b.opponents_count, 0) AS opponents_count,
# MAGIC   COALESCE(b.games_played, 0)    AS games_played,
# MAGIC   COALESCE(b.wins, 0)            AS wins,
# MAGIC   COALESCE(b.draws, 0)           AS draws,
# MAGIC   COALESCE(b.losses, 0)          AS losses,
# MAGIC   COALESCE(b.win_points, 0.0)    AS win_points,
# MAGIC   CASE WHEN COALESCE(b.games_played, 0) > 0
# MAGIC     THEN b.wins / CAST(b.games_played AS DOUBLE)
# MAGIC     ELSE 0.0 END AS win_frac,
# MAGIC   CASE WHEN COALESCE(b.games_played, 0) > 0
# MAGIC     THEN b.draws / CAST(b.games_played AS DOUBLE)
# MAGIC     ELSE 0.0 END AS draw_frac,
# MAGIC   CASE WHEN COALESCE(b.games_played, 0) > 0
# MAGIC     THEN b.losses / CAST(b.games_played AS DOUBLE)
# MAGIC     ELSE 0.0 END AS loss_frac,
# MAGIC   CASE WHEN COALESCE(b.games_played, 0) > 0
# MAGIC     THEN b.win_points / CAST(b.games_played AS DOUBLE)
# MAGIC     ELSE 0.0 END AS win_points_per_game,
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM full_spine sp
# MAGIC LEFT JOIN binned b
# MAGIC   ON sp.coach_id = b.coach_id
# MAGIC  AND sp.bin_index = b.bin_index;
# MAGIC

# COMMAND ----------

# DBTITLE 1,gold_summary.coach_opponent_global_elo_all_opponents_summary_v
# MAGIC %sql
# MAGIC -- VIEW: naf_catalog.gold_summary.coach_opponent_global_elo_all_opponents_summary_v
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : All-opponents matchup rollup (GLOBAL Elo) for coach-level dashboard KPIs.
# MAGIC -- LAYER        : GOLD_SUMMARY
# MAGIC -- GRAIN        : 1 row per (rating_system, coach_id).
# MAGIC -- PRIMARY KEY  : (rating_system, coach_id)
# MAGIC -- SOURCES      : naf_catalog.gold_summary.coach_opponent_global_elo_enriched_v
# MAGIC -- NOTES        : Aggregates opponent-level totals into coach-level totals across ALL opponents.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_summary.coach_opponent_global_elo_all_opponents_summary_v AS
# MAGIC SELECT
# MAGIC   rating_system,
# MAGIC   coach_id,
# MAGIC
# MAGIC   CAST(COUNT(DISTINCT opponent_coach_id) AS INT) AS opponents_count,
# MAGIC
# MAGIC   CAST(SUM(games_played) AS INT) AS games_played,
# MAGIC   CAST(SUM(wins)        AS INT) AS wins,
# MAGIC   CAST(SUM(draws)       AS INT) AS draws,
# MAGIC   CAST(SUM(losses)      AS INT) AS losses,
# MAGIC
# MAGIC   CAST(SUM(win_points) AS DECIMAL(12,2)) AS win_points,
# MAGIC
# MAGIC   CASE WHEN SUM(games_played) > 0
# MAGIC     THEN SUM(wins) / CAST(SUM(games_played) AS DOUBLE)
# MAGIC   END AS win_frac,
# MAGIC
# MAGIC   CASE WHEN SUM(games_played) > 0
# MAGIC     THEN SUM(win_points) / CAST(SUM(games_played) AS DOUBLE)
# MAGIC   END AS win_points_per_game,
# MAGIC
# MAGIC   MAX(load_timestamp) AS load_timestamp
# MAGIC FROM naf_catalog.gold_summary.coach_opponent_global_elo_enriched_v
# MAGIC GROUP BY
# MAGIC   rating_system,
# MAGIC   coach_id
# MAGIC ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,gold_summary.coach_race_summary
# MAGIC %sql
# MAGIC -- TABLE: naf_catalog.gold_summary.coach_race_summary
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Combined per-race performance + rating rollups per coach.
# MAGIC -- LAYER        : GOLD_SUMMARY
# MAGIC -- GRAIN        : 1 row per (rating_system, coach_id, race_id).
# MAGIC -- PRIMARY KEY  : (rating_system, coach_id, race_id)
# MAGIC -- SOURCES      : naf_catalog.gold_summary.coach_game_spine_v,
# MAGIC --                naf_catalog.gold_summary.coach_rating_race_summary
# MAGIC -- NOTES        : Uses date_id (keys) rather than game_date (display).
# MAGIC --               This is the single source for presentation’s coach_race_performance contract.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_summary.coach_race_summary
# MAGIC USING DELTA AS
# MAGIC
# MAGIC WITH cfg AS (
# MAGIC   SELECT threshold_race_elo, last_n_games_window, min_games_race_performance
# MAGIC   FROM naf_catalog.gold_dim.analytical_config
# MAGIC ),
# MAGIC
# MAGIC games AS (
# MAGIC   SELECT
# MAGIC     coach_id,
# MAGIC     race_id,
# MAGIC     tournament_id,
# MAGIC     game_id,
# MAGIC     date_id,
# MAGIC     COALESCE(
# MAGIC       event_timestamp,
# MAGIC       TO_TIMESTAMP(CAST(date_id AS STRING), 'yyyyMMdd')
# MAGIC     ) AS event_timestamp,
# MAGIC     result_numeric,
# MAGIC     COALESCE(td_for, 0)     AS td_for,
# MAGIC     COALESCE(td_against, 0) AS td_against
# MAGIC   FROM naf_catalog.gold_summary.coach_game_spine_v
# MAGIC ),
# MAGIC
# MAGIC base AS (
# MAGIC   SELECT
# MAGIC     coach_id,
# MAGIC     race_id,
# MAGIC
# MAGIC     CAST(COUNT(*) AS INT) AS games_played,
# MAGIC
# MAGIC     CAST(SUM(CASE WHEN result_numeric = 1.0 THEN 1 ELSE 0 END) AS INT) AS wins,
# MAGIC     CAST(SUM(CASE WHEN result_numeric = 0.5 THEN 1 ELSE 0 END) AS INT) AS draws,
# MAGIC     CAST(SUM(CASE WHEN result_numeric = 0.0 THEN 1 ELSE 0 END) AS INT) AS losses,
# MAGIC
# MAGIC     CAST(SUM(result_numeric) AS DECIMAL(12,2)) AS points_total,
# MAGIC     CASE WHEN COUNT(*) > 0 THEN SUM(result_numeric) / CAST(COUNT(*) AS DOUBLE) END AS points_per_game,
# MAGIC
# MAGIC     CASE WHEN COUNT(*) > 0 THEN
# MAGIC       SUM(CASE WHEN result_numeric = 1.0 THEN 1 ELSE 0 END) / CAST(COUNT(*) AS DOUBLE)
# MAGIC     END AS win_frac,
# MAGIC     CASE WHEN COUNT(*) > 0 THEN
# MAGIC       SUM(CASE WHEN result_numeric = 0.5 THEN 1 ELSE 0 END) / CAST(COUNT(*) AS DOUBLE)
# MAGIC     END AS draw_frac,
# MAGIC     CASE WHEN COUNT(*) > 0 THEN
# MAGIC       SUM(CASE WHEN result_numeric = 0.0 THEN 1 ELSE 0 END) / CAST(COUNT(*) AS DOUBLE)
# MAGIC     END AS loss_frac,
# MAGIC
# MAGIC     CAST(SUM(td_for) AS INT)              AS td_for,
# MAGIC     CAST(SUM(td_against) AS INT)          AS td_against,
# MAGIC     CAST(SUM(td_for - td_against) AS INT) AS td_diff,
# MAGIC
# MAGIC     AVG(td_for)              AS td_for_per_game,
# MAGIC     AVG(td_against)          AS td_against_per_game,
# MAGIC     AVG(td_for - td_against) AS td_diff_per_game,
# MAGIC
# MAGIC     MIN(date_id)         AS date_first_game_id,
# MAGIC     MAX(date_id)         AS date_last_game_id,
# MAGIC     MAX(event_timestamp) AS last_game_event_timestamp,
# MAGIC
# MAGIC     CAST(COUNT(DISTINCT tournament_id) AS INT) AS tournaments_played
# MAGIC   FROM games
# MAGIC   GROUP BY
# MAGIC     coach_id,
# MAGIC     race_id
# MAGIC ),
# MAGIC
# MAGIC rating_systems AS (
# MAGIC   SELECT DISTINCT
# MAGIC     rating_system
# MAGIC   FROM naf_catalog.gold_summary.coach_rating_race_summary
# MAGIC ),
# MAGIC
# MAGIC base_with_rating_system AS (
# MAGIC   SELECT
# MAGIC     rs.rating_system,
# MAGIC     b.coach_id,
# MAGIC     b.race_id,
# MAGIC
# MAGIC     b.games_played,
# MAGIC     b.wins,
# MAGIC     b.draws,
# MAGIC     b.losses,
# MAGIC
# MAGIC     b.points_total,
# MAGIC     b.points_per_game,
# MAGIC
# MAGIC     b.win_frac,
# MAGIC     b.draw_frac,
# MAGIC     b.loss_frac,
# MAGIC
# MAGIC     b.td_for,
# MAGIC     b.td_against,
# MAGIC     b.td_diff,
# MAGIC     b.td_for_per_game,
# MAGIC     b.td_against_per_game,
# MAGIC     b.td_diff_per_game,
# MAGIC
# MAGIC     b.date_first_game_id,
# MAGIC     b.date_last_game_id,
# MAGIC     b.last_game_event_timestamp,
# MAGIC
# MAGIC     b.tournaments_played
# MAGIC   FROM base b
# MAGIC   CROSS JOIN rating_systems rs
# MAGIC ),
# MAGIC
# MAGIC with_rating AS (
# MAGIC   SELECT
# MAGIC     b.*,
# MAGIC
# MAGIC     COALESCE(r.games_with_race, 0)                        AS elo_games_with_race,
# MAGIC     COALESCE(r.threshold_games, cfg.threshold_race_elo)   AS min_games_threshold_race_elo,
# MAGIC     COALESCE(r.last_n_games, cfg.last_n_games_window)     AS last_n_games_window,
# MAGIC
# MAGIC     (COALESCE(r.games_with_race, 0) >= COALESCE(r.threshold_games, cfg.threshold_race_elo))   AS is_valid_min_games_race_elo,
# MAGIC     (COALESCE(r.games_with_race, 0) >  COALESCE(r.threshold_games, cfg.threshold_race_elo))   AS is_valid_post_threshold_race_elo,
# MAGIC     (COALESCE(r.games_with_race, 0) >= COALESCE(r.last_n_games, cfg.last_n_games_window))     AS is_valid_last_50_race_elo,
# MAGIC
# MAGIC     -- Current
# MAGIC     r.elo_current,
# MAGIC     r.elo_current_game_index,
# MAGIC     r.elo_current_date_id,
# MAGIC     r.elo_current_event_timestamp,
# MAGIC     r.elo_current_game_id,
# MAGIC
# MAGIC     -- All games
# MAGIC     r.elo_peak_all,
# MAGIC     r.elo_peak_all_game_index,
# MAGIC     r.elo_peak_all_date_id,
# MAGIC     r.elo_peak_all_event_timestamp,
# MAGIC     r.elo_peak_all_game_id,
# MAGIC     r.elo_mean_all,
# MAGIC     r.elo_median_all,
# MAGIC
# MAGIC     -- Post-threshold (NULL if not reached)
# MAGIC     r.elo_peak_post_threshold,
# MAGIC     r.elo_peak_post_threshold_game_index,
# MAGIC     r.elo_peak_post_threshold_date_id,
# MAGIC     r.elo_peak_post_threshold_event_timestamp,
# MAGIC     r.elo_peak_post_threshold_game_id,
# MAGIC     r.elo_mean_post_threshold,
# MAGIC     r.elo_median_post_threshold,
# MAGIC
# MAGIC     -- Last 50 (NULL if not reached)
# MAGIC     r.elo_peak_last_50,
# MAGIC     r.elo_peak_last_50_game_index,
# MAGIC     r.elo_peak_last_50_date_id,
# MAGIC     r.elo_peak_last_50_event_timestamp,
# MAGIC     r.elo_peak_last_50_game_id,
# MAGIC     r.elo_mean_last_50,
# MAGIC     r.elo_median_last_50
# MAGIC
# MAGIC   FROM base_with_rating_system b
# MAGIC   CROSS JOIN cfg
# MAGIC   LEFT JOIN naf_catalog.gold_summary.coach_rating_race_summary r
# MAGIC     ON b.rating_system = r.rating_system
# MAGIC    AND b.coach_id      = r.coach_id
# MAGIC    AND b.race_id       = r.race_id
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   rating_system,
# MAGIC   coach_id,
# MAGIC   race_id,
# MAGIC
# MAGIC   games_played,
# MAGIC   wins,
# MAGIC   draws,
# MAGIC   losses,
# MAGIC
# MAGIC   points_total,
# MAGIC   points_per_game,
# MAGIC
# MAGIC   win_frac,
# MAGIC   draw_frac,
# MAGIC   loss_frac,
# MAGIC
# MAGIC   td_for,
# MAGIC   td_against,
# MAGIC   td_diff,
# MAGIC   td_for_per_game,
# MAGIC   td_against_per_game,
# MAGIC   td_diff_per_game,
# MAGIC
# MAGIC   date_first_game_id,
# MAGIC   date_last_game_id,
# MAGIC   last_game_event_timestamp,
# MAGIC
# MAGIC   tournaments_played,
# MAGIC
# MAGIC   cfg.min_games_race_performance AS min_games_threshold_race_performance,
# MAGIC   (games_played >= cfg.min_games_race_performance) AS is_valid_min_games_race_performance,
# MAGIC
# MAGIC   elo_games_with_race,
# MAGIC   min_games_threshold_race_elo,
# MAGIC   with_rating.last_n_games_window,
# MAGIC   is_valid_min_games_race_elo,
# MAGIC   is_valid_post_threshold_race_elo,
# MAGIC   is_valid_last_50_race_elo,
# MAGIC
# MAGIC   elo_current,
# MAGIC   elo_current_game_index,
# MAGIC   elo_current_date_id,
# MAGIC   elo_current_event_timestamp,
# MAGIC   elo_current_game_id,
# MAGIC
# MAGIC   elo_peak_all,
# MAGIC   elo_peak_all_game_index,
# MAGIC   elo_peak_all_date_id,
# MAGIC   elo_peak_all_event_timestamp,
# MAGIC   elo_peak_all_game_id,
# MAGIC   elo_mean_all,
# MAGIC   elo_median_all,
# MAGIC
# MAGIC   elo_peak_post_threshold,
# MAGIC   elo_peak_post_threshold_game_index,
# MAGIC   elo_peak_post_threshold_date_id,
# MAGIC   elo_peak_post_threshold_event_timestamp,
# MAGIC   elo_peak_post_threshold_game_id,
# MAGIC   elo_mean_post_threshold,
# MAGIC   elo_median_post_threshold,
# MAGIC
# MAGIC   elo_peak_last_50,
# MAGIC   elo_peak_last_50_game_index,
# MAGIC   elo_peak_last_50_date_id,
# MAGIC   elo_peak_last_50_event_timestamp,
# MAGIC   elo_peak_last_50_game_id,
# MAGIC   elo_mean_last_50,
# MAGIC   elo_median_last_50,
# MAGIC
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM with_rating
# MAGIC CROSS JOIN cfg;
# MAGIC

# COMMAND ----------

# DBTITLE 1,gold_summary.coach_opponent_global_elo_mean_summary_v
# MAGIC %sql
# MAGIC -- VIEW: naf_catalog.gold_summary.coach_opponent_global_elo_mean_summary_v
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Aggregate opponent GLOBAL Elo distribution stats (weighted mean of median + coverage) per coach.
# MAGIC -- LAYER        : GOLD_SUMMARY
# MAGIC -- GRAIN        : 1 row per (rating_system, coach_id).
# MAGIC -- PRIMARY KEY  : (rating_system, coach_id)
# MAGIC -- SOURCES      : naf_catalog.gold_summary.coach_opponent_global_elo_enriched_v,
# MAGIC --                naf_catalog.gold_summary.coach_rating_global_elo_summary
# MAGIC -- NOTES        : Uses each opponent's MEDIAN GLO (all games) as the strength measure,
# MAGIC --                weighted by games_played against that opponent.
# MAGIC --                Missing opponent ratings are imputed to elo_initial_rating (150.0).
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_summary.coach_opponent_global_elo_mean_summary_v AS
# MAGIC WITH cfg AS (
# MAGIC   SELECT elo_initial_rating FROM naf_catalog.gold_dim.analytical_config
# MAGIC ),
# MAGIC opp AS (
# MAGIC   SELECT
# MAGIC     rating_system,
# MAGIC     coach_id,
# MAGIC     opponent_coach_id,
# MAGIC     games_played
# MAGIC   FROM naf_catalog.gold_summary.coach_opponent_global_elo_enriched_v
# MAGIC   WHERE opponent_coach_id IS NOT NULL
# MAGIC     AND games_played > 0
# MAGIC ),
# MAGIC
# MAGIC opp_global_elo AS (
# MAGIC   SELECT
# MAGIC     rating_system,
# MAGIC     coach_id AS opponent_coach_id,
# MAGIC     CAST(global_elo_median_all AS DOUBLE) AS opponent_global_elo_median_all,
# MAGIC     CAST(global_elo_current    AS DOUBLE) AS opponent_global_elo_current
# MAGIC   FROM naf_catalog.gold_summary.coach_rating_global_elo_summary
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   o.rating_system,
# MAGIC   o.coach_id,
# MAGIC
# MAGIC   -- weighted mean of opponent median GLO, imputing missing to initial rating
# MAGIC   SUM(o.games_played * COALESCE(g.opponent_global_elo_median_all, g.opponent_global_elo_current, cfg.elo_initial_rating))
# MAGIC     / NULLIF(SUM(o.games_played), 0) AS opponent_global_elo_median_weighted,
# MAGIC
# MAGIC   CAST(COUNT(DISTINCT o.opponent_coach_id) AS INT) AS opponents_count,
# MAGIC
# MAGIC   -- denominator of the weighted mean (total games vs opponents with IDs)
# MAGIC   CAST(SUM(o.games_played) AS INT) AS opponent_games_weight,
# MAGIC
# MAGIC   -- coverage diagnostics (how many weighted games had a real opponent global_elo_median_all)
# MAGIC   CAST(SUM(CASE WHEN g.opponent_global_elo_median_all IS NOT NULL THEN o.games_played ELSE 0 END) AS INT) AS opponent_games_rated,
# MAGIC   CAST(SUM(CASE WHEN g.opponent_global_elo_median_all IS     NULL THEN o.games_played ELSE 0 END) AS INT) AS opponent_games_imputed,
# MAGIC
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM opp o
# MAGIC CROSS JOIN cfg
# MAGIC LEFT JOIN opp_global_elo g
# MAGIC   ON o.rating_system      = g.rating_system
# MAGIC  AND o.opponent_coach_id  = g.opponent_coach_id
# MAGIC GROUP BY
# MAGIC   o.rating_system,
# MAGIC   o.coach_id
# MAGIC ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,gold_summary.coach_tournament_performance_summary
# MAGIC %sql
# MAGIC -- TABLE: naf_catalog.gold_summary.coach_tournament_performance_summary
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Coach performance rollups by tournament (including GLOBAL Elo exchange metrics when available).
# MAGIC -- LAYER        : GOLD_SUMMARY
# MAGIC -- GRAIN        : 1 row per (rating_system, coach_id, tournament_id).
# MAGIC -- PRIMARY KEY  : (rating_system, coach_id, tournament_id)
# MAGIC -- SOURCES      : naf_catalog.gold_summary.coach_game_spine_v,
# MAGIC --                naf_catalog.gold_summary.coach_global_elo_rating_history_v
# MAGIC -- NOTES        : Presentation owns tournament labels/flags from tournament_dim.
# MAGIC --               Uses date_id (keys) rather than game_date (display).
# MAGIC --               Keeps all rows; adds has_* flags for rating availability.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_summary.coach_tournament_performance_summary
# MAGIC USING DELTA AS
# MAGIC
# MAGIC WITH games AS (
# MAGIC   SELECT
# MAGIC     coach_id,
# MAGIC     tournament_id,
# MAGIC     game_id,
# MAGIC     date_id,
# MAGIC     event_timestamp,
# MAGIC     COALESCE(
# MAGIC       event_timestamp,
# MAGIC       TO_TIMESTAMP(CAST(date_id AS STRING), 'yyyyMMdd')
# MAGIC     ) AS event_ts_coalesced,
# MAGIC     result_numeric,
# MAGIC     COALESCE(td_for, 0)     AS td_for,
# MAGIC     COALESCE(td_against, 0) AS td_against
# MAGIC   FROM naf_catalog.gold_summary.coach_game_spine_v
# MAGIC   WHERE tournament_id IS NOT NULL
# MAGIC ),
# MAGIC
# MAGIC rating_systems AS (
# MAGIC   SELECT DISTINCT
# MAGIC     rating_system
# MAGIC   FROM naf_catalog.gold_summary.coach_global_elo_rating_history_v
# MAGIC ),
# MAGIC
# MAGIC games_with_rating_system AS (
# MAGIC   SELECT
# MAGIC     rating_systems.rating_system,
# MAGIC     games.coach_id,
# MAGIC     games.tournament_id,
# MAGIC     games.game_id,
# MAGIC     games.date_id,
# MAGIC     games.event_timestamp,
# MAGIC     games.event_ts_coalesced,
# MAGIC     games.result_numeric,
# MAGIC     games.td_for,
# MAGIC     games.td_against
# MAGIC   FROM games
# MAGIC   CROSS JOIN rating_systems
# MAGIC ),
# MAGIC
# MAGIC global_elo AS (
# MAGIC   SELECT
# MAGIC     rating_system,
# MAGIC     coach_id,
# MAGIC     game_id,
# MAGIC     rating_after,
# MAGIC     rating_delta
# MAGIC   FROM naf_catalog.gold_summary.coach_global_elo_rating_history_v
# MAGIC ),
# MAGIC
# MAGIC joined AS (
# MAGIC   SELECT
# MAGIC     games_with_rating_system.rating_system,
# MAGIC     games_with_rating_system.coach_id,
# MAGIC     games_with_rating_system.tournament_id,
# MAGIC     games_with_rating_system.game_id,
# MAGIC     games_with_rating_system.date_id,
# MAGIC     games_with_rating_system.event_timestamp,
# MAGIC     games_with_rating_system.event_ts_coalesced,
# MAGIC     games_with_rating_system.result_numeric,
# MAGIC     games_with_rating_system.td_for,
# MAGIC     games_with_rating_system.td_against,
# MAGIC     global_elo.rating_after,
# MAGIC     global_elo.rating_delta
# MAGIC   FROM games_with_rating_system
# MAGIC   LEFT JOIN global_elo
# MAGIC     ON games_with_rating_system.rating_system = global_elo.rating_system
# MAGIC    AND games_with_rating_system.coach_id      = global_elo.coach_id
# MAGIC    AND games_with_rating_system.game_id       = global_elo.game_id
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   rating_system,
# MAGIC   coach_id,
# MAGIC   tournament_id,
# MAGIC
# MAGIC   CAST(COUNT(*) AS INT) AS games_played,
# MAGIC   CAST(SUM(CASE WHEN result_numeric = 1.0 THEN 1 ELSE 0 END) AS INT) AS wins,
# MAGIC   CAST(SUM(CASE WHEN result_numeric = 0.5 THEN 1 ELSE 0 END) AS INT) AS draws,
# MAGIC   CAST(SUM(CASE WHEN result_numeric = 0.0 THEN 1 ELSE 0 END) AS INT) AS losses,
# MAGIC
# MAGIC   CAST(SUM(result_numeric) AS DECIMAL(12,2)) AS points_total,
# MAGIC
# MAGIC   CAST(SUM(td_for) AS INT)              AS td_for,
# MAGIC   CAST(SUM(td_against) AS INT)          AS td_against,
# MAGIC   CAST(SUM(td_for - td_against) AS INT) AS td_diff,
# MAGIC
# MAGIC   MIN(date_id) AS date_first_game_id,
# MAGIC   MAX(date_id) AS date_last_game_id,
# MAGIC
# MAGIC   -- Return the coalesced timestamp so "last game" is never NULL when games exist.
# MAGIC   MAX_BY(
# MAGIC     event_ts_coalesced,
# MAGIC     STRUCT(
# MAGIC       date_id,
# MAGIC       event_ts_coalesced,
# MAGIC       game_id
# MAGIC     )
# MAGIC   ) AS last_game_event_timestamp,
# MAGIC
# MAGIC   CAST(SUM(CASE WHEN rating_delta IS NOT NULL THEN 1 ELSE 0 END) AS INT) AS global_elo_games_rated,
# MAGIC   (SUM(CASE WHEN rating_delta IS NOT NULL THEN 1 ELSE 0 END) > 0) AS has_global_elo_exchange_data,
# MAGIC
# MAGIC   SUM(rating_delta)                    AS global_elo_exchange_total,
# MAGIC   AVG(rating_delta)                    AS global_elo_exchange_mean,
# MAGIC   PERCENTILE_APPROX(rating_after, 0.5) AS global_elo_median_rating_after,
# MAGIC
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM joined
# MAGIC GROUP BY
# MAGIC   rating_system,
# MAGIC   coach_id,
# MAGIC   tournament_id
# MAGIC ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,gold_summary.coach_biggest_upset_summary
# MAGIC %sql
# MAGIC -- TABLE: naf_catalog.gold_summary.coach_biggest_upset_summary
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Per-coach biggest upset (done and received) based on
# MAGIC --                GLOBAL NAF_ELO rating gap at the time of the game.
# MAGIC -- LAYER        : GOLD_SUMMARY
# MAGIC -- GRAIN        : 1 row per (coach_id, upset_type)
# MAGIC --                  upset_type IN ('DONE', 'RECEIVED')
# MAGIC -- PRIMARY KEY  : (coach_id, upset_type)
# MAGIC -- SOURCES      : naf_catalog.gold_fact.rating_history_fact
# MAGIC -- NOTES        : Moved from 341 presentation (was inline CTE) to respect
# MAGIC --                the layer contract: presentation reads summary, not fact.
# MAGIC --                rating_gap = opponent_rating_before - rating_before
# MAGIC --                  DONE:     coach won against higher-rated (gap > 0)
# MAGIC --                  RECEIVED: coach lost to lower-rated    (gap < 0)
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_summary.coach_biggest_upset_summary
# MAGIC USING DELTA AS
# MAGIC
# MAGIC WITH upset_base AS (
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
# MAGIC -- Biggest upset done: coach won against higher-rated opponent
# MAGIC biggest_done AS (
# MAGIC   SELECT
# MAGIC     u.coach_id,
# MAGIC     'DONE' AS upset_type,
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
# MAGIC -- Biggest upset received: coach lost to lower-rated opponent
# MAGIC biggest_received AS (
# MAGIC   SELECT
# MAGIC     u.coach_id,
# MAGIC     'RECEIVED' AS upset_type,
# MAGIC     MAX_BY(
# MAGIC       NAMED_STRUCT(
# MAGIC         'opponent_coach_id', u.opponent_coach_id,
# MAGIC         'game_id', u.game_id,
# MAGIC         'date_id', u.date_id,
# MAGIC         'coach_rating', u.rating_before,
# MAGIC         'opponent_rating', u.opponent_rating_before,
# MAGIC         'rating_gap', u.rating_gap
# MAGIC       ),
# MAGIC       -u.rating_gap
# MAGIC     ) AS s
# MAGIC   FROM upset_base u
# MAGIC   WHERE u.result_numeric = 0.0 AND u.rating_gap < 0
# MAGIC   GROUP BY u.coach_id
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   coach_id,
# MAGIC   upset_type,
# MAGIC   s.opponent_coach_id,
# MAGIC   s.game_id,
# MAGIC   s.date_id,
# MAGIC   s.coach_rating,
# MAGIC   s.opponent_rating,
# MAGIC   s.rating_gap,
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM biggest_done
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   coach_id,
# MAGIC   upset_type,
# MAGIC   s.opponent_coach_id,
# MAGIC   s.game_id,
# MAGIC   s.date_id,
# MAGIC   s.coach_rating,
# MAGIC   s.opponent_rating,
# MAGIC   s.rating_gap,
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM biggest_received;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### End of 331 pipeline objects
# MAGIC All QA/debug/smoke tests for 331 outputs live in `350_Tests_and_admining.py`.
