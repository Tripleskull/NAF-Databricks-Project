# Databricks notebook source
# MAGIC %md
# MAGIC # Gold_dim notebook contract (`naf_catalog.gold_dim`)
# MAGIC
# MAGIC **Purpose:** Build **minimal, normalized canonical entities** (SCD1 / current-state) from **Silver**.
# MAGIC **Not in scope:** metrics, aggregates, windows, rankings → `gold_summary` / `gold_presentation`.
# MAGIC
# MAGIC **Design authority (wins):**
# MAGIC - Project Design → `00_design_decisions.md`
# MAGIC - Project Design → `02_schema_design.md`
# MAGIC - Project Design → `03_style_guides.md`
# MAGIC - Dimension specs (when applicable): `10_nation_dimension.md`, `11_race_dimension.md`, `12_coach_dimension.md`, `13_tournament_dimension.md`, `14_date_dimension.md`
# MAGIC
# MAGIC ## Gold_dim rules (short)
# MAGIC - Output: one dimension table per entity (SCD1). Re-runnable / idempotent loads.
# MAGIC - Keys: `<entity>_id` (no generic `id`); `date_id` = INT `YYYYMMDD`; booleans use `is_` (prefer NOT NULL).
# MAGIC - **No duplication of attributes owned by other dims** (use FKs; e.g., nation attributes live in `nation_dim`).
# MAGIC - Always state **grain + PK uniqueness** + expected FKs in the notebook header.
# MAGIC - Enforce basic integrity: PK unique, required keys NOT NULL, and coverage for referenced IDs from Silver where relevant.
# MAGIC

# COMMAND ----------

# # Drop schemas: gold_dim, gold_fact, gold_summary, gold_presentation
# spark.sql("DROP SCHEMA IF EXISTS naf_catalog.gold_dim CASCADE")
# spark.sql("DROP SCHEMA IF EXISTS naf_catalog.gold_fact CASCADE")
# spark.sql("DROP SCHEMA IF EXISTS naf_catalog.gold_summary CASCADE")
# spark.sql("DROP SCHEMA IF EXISTS naf_catalog.gold_presentation CASCADE")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS naf_catalog.gold_dim;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TABLE: naf_catalog.gold_dim.race_dim
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Canonical race lookup for joins (minimal dimension).
# MAGIC -- LAYER        : GOLD_DIM
# MAGIC -- GRAIN        : 1 row per race_id
# MAGIC -- PRIMARY KEY  : race_id
# MAGIC -- SOURCES      : naf_catalog.silver.races_clean
# MAGIC -- NOTES        : race_id = 0 is a reserved GLOBAL (race-agnostic) scope row with race_name = 'none'.
# MAGIC --                Do not treat race_id = 0 as a playable race.
# MAGIC -- See design docs: 00_design_decisions.md, 11_race_dimension.md
# MAGIC -- =====================================================================
# MAGIC
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_dim.race_dim
# MAGIC USING DELTA AS
# MAGIC WITH base AS (
# MAGIC   SELECT
# MAGIC     race_id,
# MAGIC     race_name
# MAGIC   FROM naf_catalog.silver.races_clean
# MAGIC   WHERE race_id IS NOT NULL
# MAGIC     AND race_id <> 0
# MAGIC )
# MAGIC SELECT
# MAGIC   race_id,
# MAGIC   race_name
# MAGIC FROM base
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   0      AS race_id,
# MAGIC   'none' AS race_name;
# MAGIC

# COMMAND ----------

# MAGIC %sql -- TABLE: naf_catalog.gold_dim.tournament_stat_dim
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Canonical lookup of tournament statistic types for joins.
# MAGIC -- LAYER        : GOLD_DIM
# MAGIC -- GRAIN        : 1 row per stat_id
# MAGIC -- PRIMARY KEY  : stat_id
# MAGIC -- FOREIGN KEYS : none (used by tournament_statistics_fact.stat_id)
# MAGIC -- SOURCES      : naf_catalog.silver.tournament_statistics_list_clean
# MAGIC -- NOTES        : Defines metadata and human-readable labels for statistic IDs.
# MAGIC --                Used to enrich tournament_statistics_fact.
# MAGIC -- See design docs: 00_design_decisions.md, 02_schema_design.md
# MAGIC -- =====================================================================
# MAGIC
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_dim.tournament_stat_dim
# MAGIC USING DELTA AS
# MAGIC SELECT DISTINCT
# MAGIC   stat_id,
# MAGIC   stat_name
# MAGIC FROM naf_catalog.silver.tournament_statistics_list_clean
# MAGIC

# COMMAND ----------

# MAGIC %sql -- TABLE: naf_catalog.gold_dim.nation_dim
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Canonical nation lookup (names + codes + flag key) for joins.
# MAGIC -- LAYER        : GOLD_DIM
# MAGIC -- GRAIN        : 1 row per nation_id
# MAGIC -- PRIMARY KEY  : nation_id
# MAGIC -- FOREIGN KEYS : none (referenced by coach_dim.nation_id, tournament_dim.nation_id, etc.)
# MAGIC -- SOURCES      : naf_catalog.silver.nations_entity
# MAGIC -- NOTES        : nation_id is a deterministic surrogate (hash of canonical nation group key).
# MAGIC --                Includes an 'UNKNOWN' nation row for unmapped/blank nation inputs.
# MAGIC -- See design docs: 00_design_decisions.md, 02_schema_design.md
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_dim.nation_dim
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC   nation_id,
# MAGIC   nation_name,
# MAGIC   nation_name_display,
# MAGIC   iso2_code,
# MAGIC   iso3_code,
# MAGIC   fifa_code,
# MAGIC   ioc_code,
# MAGIC   flag_code
# MAGIC FROM naf_catalog.silver.nations_entity;

# COMMAND ----------

# MAGIC %sql -- TABLE: naf_catalog.gold_dim.coach_dim ==================================================
# MAGIC -- PURPOSE: Canonical list of active coaches and their current nation.
# MAGIC -- LAYER: GOLD_DIM
# MAGIC -- GRAIN: 1 row per coach (current state only, SCD1)
# MAGIC -- PRIMARY KEY: coach_id
# MAGIC -- FOREIGN KEYS: nation_id → gold_dim.nation_dim
# MAGIC -- SOURCES: naf_catalog.silver.coaches_clean
# MAGIC -- NOTES: Uses stable NAF coach_id. No historical records.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_dim.coach_dim
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC   c.coach_id,
# MAGIC   c.coach_name,
# MAGIC   c.nation_id
# MAGIC FROM naf_catalog.silver.coaches_clean c;
# MAGIC

# COMMAND ----------

# MAGIC %sql -- TABLE: naf_catalog.gold_dim.tournament_dim
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Canonical tournament lookup (minimal + stable attributes) for joins.
# MAGIC -- LAYER        : GOLD_DIM
# MAGIC -- GRAIN        : 1 row per tournament_id
# MAGIC -- PRIMARY KEY  : tournament_id
# MAGIC -- FOREIGN KEYS : nation_id        → naf_catalog.gold_dim.nation_dim
# MAGIC --               organizer_coach_id → naf_catalog.gold_dim.coach_dim
# MAGIC --               start_date_id     → naf_catalog.gold_dim.date_dim (date_id)
# MAGIC --               end_date_id       → naf_catalog.gold_dim.date_dim (date_id)
# MAGIC -- SOURCES      : naf_catalog.silver.tournaments_clean
# MAGIC -- NOTES        : start_date_id / end_date_id are derived in Silver as INT YYYYMMDD.
# MAGIC --               No aggregates or event outcomes here.
# MAGIC -- See design docs: 00_design_decisions.md, 02_schema_design.md
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_dim.tournament_dim
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC   t.tournament_id,
# MAGIC   t.tournament_name,
# MAGIC   t.organizer_coach_id,
# MAGIC   t.date_start,
# MAGIC   t.date_end,
# MAGIC   t.start_date_id,
# MAGIC   t.end_date_id,
# MAGIC   t.is_major_tournament,
# MAGIC   t.nation_id
# MAGIC FROM naf_catalog.silver.tournaments_clean t;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql -- TABLE: naf_catalog.gold_dim.date_dim
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Canonical calendar table (ISO weekday: Mon=1 ... Sun=7).
# MAGIC -- LAYER        : GOLD_DIM
# MAGIC -- GRAIN        : 1 row per date_id (calendar date)
# MAGIC -- PRIMARY KEY  : (date_id)
# MAGIC -- SOURCES      : Generated (no upstream tables)
# MAGIC -- NOTES        : date_id is INT in YYYYMMDD format; date is DATE.
# MAGIC --                ISO weekday convention: Mon=1 ... Sun=7.
# MAGIC --                Range is fixed here; keep stable unless a real need arises.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_dim.date_dim
# MAGIC USING DELTA AS
# MAGIC WITH dates AS (
# MAGIC   SELECT explode(
# MAGIC     sequence(
# MAGIC       to_date('2000-01-01'),
# MAGIC       to_date('2035-12-31'),
# MAGIC       interval 1 day
# MAGIC     )
# MAGIC   ) AS date_value
# MAGIC ),
# MAGIC enriched AS (
# MAGIC   SELECT
# MAGIC     date_value,
# MAGIC     CAST(date_format(date_value, 'yyyyMMdd') AS INT) AS date_id,
# MAGIC     (((dayofweek(date_value) + 5) % 7) + 1) AS day_of_week_num
# MAGIC   FROM dates
# MAGIC )
# MAGIC SELECT
# MAGIC   date_id,
# MAGIC   date_value AS date,
# MAGIC
# MAGIC   year(date_value)    AS year,
# MAGIC   quarter(date_value) AS quarter,
# MAGIC   month(date_value)   AS month,
# MAGIC   day(date_value)     AS day_of_month,
# MAGIC
# MAGIC   date_format(date_value, 'MMM')     AS month_name,
# MAGIC   date_format(date_value, 'yyyy-MM') AS year_month,
# MAGIC   date_format(date_value, 'E')       AS day_of_week_name,
# MAGIC
# MAGIC   weekofyear(date_value) AS week_of_year,
# MAGIC   day_of_week_num,
# MAGIC
# MAGIC   (day_of_week_num IN (6, 7)) AS is_weekend
# MAGIC FROM enriched;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =====================================================================
# MAGIC -- OBJECT       : naf_catalog.gold_dim.analytical_config
# MAGIC -- GRAIN        : Singleton (1 row). No PK — single-row config table.
# MAGIC -- PURPOSE      : Central store for all tuneable analytical parameters.
# MAGIC --                Consumed by: tournament_parameters (below), 320 (Elo engine),
# MAGIC --                331 (coach summary), 332 (nation summary).
# MAGIC --                See Analytical_Parameters.md for documentation.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_dim.analytical_config
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC   -- Elo engine
# MAGIC   CAST(150.0   AS DOUBLE)  AS elo_initial_rating,
# MAGIC   CAST(150.0   AS DOUBLE)  AS elo_scale,
# MAGIC
# MAGIC   -- Tournament n_eff constants
# MAGIC   CAST(60      AS INT)     AS n_eff_major,
# MAGIC   CAST(32      AS INT)     AS n_eff_minor_cap,
# MAGIC
# MAGIC   -- Coach burn-in thresholds (strict >)
# MAGIC   CAST(50      AS INT)     AS threshold_global_elo,
# MAGIC   CAST(25      AS INT)     AS threshold_race_elo,
# MAGIC
# MAGIC   -- Last-N window
# MAGIC   CAST(50      AS INT)     AS last_n_games_window,
# MAGIC
# MAGIC   -- Coach minimum-games eligibility
# MAGIC   CAST(10      AS INT)     AS min_games_race_performance,
# MAGIC
# MAGIC   -- Nation thresholds
# MAGIC   CAST(50      AS INT)     AS min_games_nation_glo,
# MAGIC   CAST(5       AS INT)     AS min_games_nation_race,
# MAGIC
# MAGIC   -- Nation rivalry scoring weights (currently unused — rivalry tables use
# MAGIC   -- simple rank-average formula; kept for future refinement)
# MAGIC   CAST(100.0   AS DOUBLE)  AS rivalry_games_cap,
# MAGIC   CAST(1.0/6.0 AS DOUBLE)  AS rivalry_w_games,
# MAGIC   CAST(2.0/3.0 AS DOUBLE)  AS rivalry_w_closeness,
# MAGIC   CAST(1.0/6.0 AS DOUBLE)  AS rivalry_w_share,
# MAGIC
# MAGIC   -- Nation elite rivalry: minimum GLO median for both coaches
# MAGIC   CAST(200.0   AS DOUBLE)  AS elite_glo_median_threshold,
# MAGIC
# MAGIC   -- Form score (Phase 1)
# MAGIC   CAST(50      AS INT)     AS form_window_games,
# MAGIC   CAST(50      AS INT)     AS form_min_games_for_pctl,
# MAGIC
# MAGIC   -- Team selector (Phase 6)
# MAGIC   -- Old 5-weight scheme removed. New 3-component selector (GLO/Race/Opponent)
# MAGIC   -- uses deterministic focus weights computed in 332 (no config needed).
# MAGIC   -- Kept for reference: previous weights were 0.30/0.25/0.15/0.15/0.15.
# MAGIC
# MAGIC   -- SSM v1 (random-walk EKF, 321)
# MAGIC   CAST(50.0    AS DOUBLE)  AS ssm1_prior_sigma,        -- prior SD for new coaches
# MAGIC   CAST(1.000   AS DOUBLE)  AS ssm1_phi,                -- AR(1) mean-reversion coeff (1.0 = none)
# MAGIC   CAST(2.0     AS DOUBLE)  AS ssm1_sigma2_process,     -- per-game process noise variance
# MAGIC   CAST(0.02    AS DOUBLE)  AS ssm1_sigma2_obs,         -- observation noise (logistic scale)
# MAGIC
# MAGIC   -- SSM v2 production (time-aware + adaptive volatility, 321)
# MAGIC   CAST(50.0    AS DOUBLE)  AS ssm2_prior_sigma,        -- prior SD for new coaches
# MAGIC   CAST(0.10    AS DOUBLE)  AS ssm2_sigma2_obs,         -- observation noise (logistic scale)
# MAGIC   CAST(2.00    AS DOUBLE)  AS ssm2_q_time,             -- variance per sqrt(day) of inactivity
# MAGIC   CAST(0.025   AS DOUBLE)  AS ssm2_q_game,             -- baseline per-game process noise
# MAGIC   CAST(180.0   AS DOUBLE)  AS ssm2_max_days,           -- cap for days-since-last-game
# MAGIC   CAST(0.25    AS DOUBLE)  AS ssm2_v_base,             -- volatility baseline
# MAGIC   CAST(24.0    AS DOUBLE)  AS ssm2_v_scale,            -- volatility scale factor
# MAGIC   CAST(0.90    AS DOUBLE)  AS ssm2_v_decay,            -- EWMA decay for shock tracker
# MAGIC   CAST(0.00    AS DOUBLE)  AS ssm2_v_min,              -- volatility floor
# MAGIC   CAST(16.0    AS DOUBLE)  AS ssm2_v_max,              -- volatility ceiling
# MAGIC
# MAGIC   CURRENT_TIMESTAMP()      AS load_timestamp
# MAGIC ;

# COMMAND ----------

# MAGIC %sql -- TABLE: naf_catalog.gold_dim.tournament_parameters
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Deterministic tournament-level parameter lookup used by rating pipelines.
# MAGIC --               Publishes *parameters only* (no KPI outputs).
# MAGIC -- LAYER        : GOLD_DIM  (EXCEPTION: helper/parameter lookup; deterministic + idempotent)
# MAGIC -- GRAIN        : 1 row per tournament_id
# MAGIC -- PRIMARY KEY  : tournament_id
# MAGIC -- FOREIGN KEYS : tournament_id → naf_catalog.gold_dim.tournament_dim
# MAGIC -- SOURCES      : naf_catalog.silver.games_clean     (participation set; internal only)
# MAGIC --               naf_catalog.gold_dim.tournament_dim (is_major_tournament)
# MAGIC -- OUTPUTS      : n_eff, k_value
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_dim.tournament_parameters
# MAGIC USING DELTA AS
# MAGIC WITH cfg AS (
# MAGIC   SELECT n_eff_major, n_eff_minor_cap
# MAGIC   FROM naf_catalog.gold_dim.analytical_config
# MAGIC ),
# MAGIC tournament_spine AS (
# MAGIC   SELECT tournament_id
# MAGIC   FROM naf_catalog.silver.games_clean
# MAGIC   GROUP BY tournament_id
# MAGIC ),
# MAGIC participant_coach_n AS (
# MAGIC   SELECT
# MAGIC     tournament_id,
# MAGIC     CAST(COUNT(DISTINCT coach_id) AS INT) AS n_participant_coaches
# MAGIC   FROM (
# MAGIC     SELECT tournament_id, home_coach_id AS coach_id
# MAGIC     FROM naf_catalog.silver.games_clean
# MAGIC     UNION ALL
# MAGIC     SELECT tournament_id, away_coach_id AS coach_id
# MAGIC     FROM naf_catalog.silver.games_clean
# MAGIC   ) x
# MAGIC   GROUP BY tournament_id
# MAGIC ),
# MAGIC enriched AS (
# MAGIC   SELECT
# MAGIC     s.tournament_id,
# MAGIC     COALESCE(td.is_major_tournament, FALSE) AS is_major_tournament,
# MAGIC     COALESCE(p.n_participant_coaches, 0)    AS n_participant_coaches
# MAGIC   FROM tournament_spine s
# MAGIC   LEFT JOIN naf_catalog.gold_dim.tournament_dim td
# MAGIC     ON td.tournament_id = s.tournament_id
# MAGIC   LEFT JOIN participant_coach_n p
# MAGIC     ON p.tournament_id = s.tournament_id
# MAGIC ),
# MAGIC scored AS (
# MAGIC   SELECT
# MAGIC     e.tournament_id,
# MAGIC     CAST(
# MAGIC       CASE
# MAGIC         WHEN e.is_major_tournament THEN cfg.n_eff_major
# MAGIC         ELSE LEAST(e.n_participant_coaches, cfg.n_eff_minor_cap)
# MAGIC       END AS INT
# MAGIC     ) AS n_eff
# MAGIC   FROM enriched e
# MAGIC   CROSS JOIN cfg
# MAGIC )
# MAGIC SELECT
# MAGIC   tournament_id,
# MAGIC   n_eff,
# MAGIC   CAST(ROUND(2 * SQRT(CAST(n_eff AS DOUBLE))) AS INT) AS k_value,
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM scored;
# MAGIC
