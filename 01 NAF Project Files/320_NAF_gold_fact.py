# Databricks notebook source
# MAGIC %md
# MAGIC # Gold_fact notebook contract (`naf_catalog.gold_fact`)
# MAGIC
# MAGIC **Purpose:** Gold facts are **event-truth** tables.
# MAGIC
# MAGIC Every fact notebook must state:
# MAGIC - **Grain** (one sentence: “1 row per …”)
# MAGIC - **Primary key** (enforced unique)
# MAGIC - **Foreign keys** to `naf_catalog.gold_dim`
# MAGIC
# MAGIC **Inputs:** built from **Silver** (and dims where needed for keys).
# MAGIC **Not in scope:** aggregates, KPIs, rolling/windows, trajectories → `naf_catalog.gold_summary`.
# MAGIC
# MAGIC ## Conventions (short)
# MAGIC - Prefer a single event key: `<entity>_id` (no generic `id`)
# MAGIC - Use `date_id` = INT `YYYYMMDD` where applicable
# MAGIC - Include `*_date` / `*_timestamp` only when they are true event attributes (not derived KPI time)
# MAGIC - Ratings/outputs must remain **at event grain** (no per-coach / per-season rollups here)
# MAGIC
# MAGIC **Design authority (wins):**
# MAGIC - Project Design → `00_design_decisions.md`
# MAGIC - Project Design → `02_schema_design.md`
# MAGIC - Project Design → `03_style_guides.md`
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Drop schema gold_fact
# MAGIC %sql
# MAGIC -- DANGER: drops EVERYTHING in naf_catalog.gold_fact(views + tables) incl. dependencies.
# MAGIC -- DROP SCHEMA IF EXISTS naf_catalog.gold_fact CASCADE;

# COMMAND ----------

# DBTITLE 1,Create schema gold_fact
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS naf_catalog.gold_fact;
# MAGIC

# COMMAND ----------

# DBTITLE 1,gold_fact.games_fact
# MAGIC %sql -- TABLE: naf_catalog.gold_fact.games_fact
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Event-truth games fact table (one record per NAF game).
# MAGIC -- LAYER        : GOLD_FACT
# MAGIC -- GRAIN        : 1 row per game_id
# MAGIC -- PRIMARY KEY  : game_id
# MAGIC -- FOREIGN KEYS : tournament_id  → naf_catalog.gold_dim.tournament_dim
# MAGIC --               home_coach_id  → naf_catalog.gold_dim.coach_dim
# MAGIC --               away_coach_id  → naf_catalog.gold_dim.coach_dim
# MAGIC --               home_race_id   → naf_catalog.gold_dim.race_dim
# MAGIC --               away_race_id   → naf_catalog.gold_dim.race_dim
# MAGIC --               date_id        → naf_catalog.gold_dim.date_dim
# MAGIC -- SOURCES      : naf_catalog.silver.games_clean
# MAGIC -- NOTES        : event_timestamp, game_date, and date_id are canonicalized in Silver.
# MAGIC --                Excludes non-supported variants and placeholder coach_id=9 per Silver rules.
# MAGIC -- See design docs: 00_design_decisions.md, 02_schema_design.md
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_fact.games_fact
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC   game_id,
# MAGIC   tournament_id,
# MAGIC   variant_id,
# MAGIC   event_timestamp,
# MAGIC   game_date,
# MAGIC   date_id,
# MAGIC   home_coach_id,
# MAGIC   away_coach_id,
# MAGIC   home_race_id,
# MAGIC   away_race_id,
# MAGIC   td_home,
# MAGIC   td_away,
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM naf_catalog.silver.games_clean;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,gold_fact.game_global_order_spine_v
# MAGIC %sql -- VIEW: naf_catalog.gold_fact.game_global_order_spine_v
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Canonical global game ordering spine (adds game_index) for consistent sequencing.
# MAGIC -- LAYER        : GOLD_FACT
# MAGIC -- GRAIN        : 1 row per game_id
# MAGIC -- PRIMARY KEY  : game_id
# MAGIC -- SOURCES      : naf_catalog.gold_fact.games_fact
# MAGIC -- NOTES        : - game_index is a derived helper (not event truth).
# MAGIC --               - Ordering is deterministic on an explicit order key + game_id.
# MAGIC --               - If late/backfilled games arrive, game_index may shift (expected).
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_fact.game_global_order_spine_v AS
# MAGIC SELECT
# MAGIC   game_id,
# MAGIC   ROW_NUMBER() OVER (
# MAGIC     ORDER BY
# MAGIC       COALESCE(
# MAGIC         event_timestamp,
# MAGIC         TO_TIMESTAMP(CAST(date_id AS STRING), 'yyyyMMdd')
# MAGIC       ) ASC,
# MAGIC       game_id ASC
# MAGIC   ) AS game_index
# MAGIC FROM naf_catalog.gold_fact.games_fact;
# MAGIC

# COMMAND ----------

# DBTITLE 1,gold_fact.coach_games_fact
# MAGIC %sql -- VIEW: naf_catalog.gold_fact.coach_games_fact
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Per-coach perspective of games (coach vs opponent), home/away normalized.
# MAGIC -- LAYER        : GOLD_FACT
# MAGIC -- GRAIN        : 1 row per (game_id, coach_id, home_away)
# MAGIC -- PRIMARY KEY  : (game_id, coach_id, home_away)
# MAGIC -- FOREIGN KEYS : game_id            → naf_catalog.gold_fact.games_fact
# MAGIC --               coach_id            → naf_catalog.gold_dim.coach_dim
# MAGIC --               opponent_coach_id   → naf_catalog.gold_dim.coach_dim
# MAGIC --               tournament_id       → naf_catalog.gold_dim.tournament_dim
# MAGIC --               race_id             → naf_catalog.gold_dim.race_dim
# MAGIC --               opponent_race_id    → naf_catalog.gold_dim.race_dim
# MAGIC --               date_id             → naf_catalog.gold_dim.date_dim
# MAGIC -- SOURCES      : naf_catalog.gold_fact.games_fact,
# MAGIC --               naf_catalog.gold_fact.game_global_order_spine_v
# MAGIC -- NOTES        : Derived view: duplicates each game into two rows (H and A) with
# MAGIC --               td_for/td_against and result fields from the coach perspective.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_fact.coach_games_fact AS
# MAGIC WITH g AS (
# MAGIC   SELECT
# MAGIC     gf.game_id,
# MAGIC     gf.tournament_id,
# MAGIC     gf.event_timestamp,
# MAGIC     gf.game_date,
# MAGIC     gf.date_id,
# MAGIC     go.game_index,
# MAGIC     gf.variant_id,
# MAGIC
# MAGIC     gf.home_coach_id,
# MAGIC     gf.away_coach_id,
# MAGIC     gf.home_race_id,
# MAGIC     gf.away_race_id,
# MAGIC     gf.td_home,
# MAGIC     gf.td_away
# MAGIC   FROM naf_catalog.gold_fact.games_fact gf
# MAGIC   INNER JOIN naf_catalog.gold_fact.game_global_order_spine_v go
# MAGIC     ON gf.game_id = go.game_id
# MAGIC ),
# MAGIC base AS (
# MAGIC   -- Home coach perspective
# MAGIC   SELECT
# MAGIC     game_id,
# MAGIC     tournament_id,
# MAGIC     event_timestamp,
# MAGIC     game_date,
# MAGIC     date_id,
# MAGIC     game_index,
# MAGIC     variant_id,
# MAGIC
# MAGIC     'H'             AS home_away,
# MAGIC     home_coach_id   AS coach_id,
# MAGIC     away_coach_id   AS opponent_coach_id,
# MAGIC     home_race_id    AS race_id,
# MAGIC     away_race_id    AS opponent_race_id,
# MAGIC     td_home         AS td_for,
# MAGIC     td_away         AS td_against
# MAGIC   FROM g
# MAGIC
# MAGIC   UNION ALL
# MAGIC
# MAGIC   -- Away coach perspective
# MAGIC   SELECT
# MAGIC     game_id,
# MAGIC     tournament_id,
# MAGIC     event_timestamp,
# MAGIC     game_date,
# MAGIC     date_id,
# MAGIC     game_index,
# MAGIC     variant_id,
# MAGIC
# MAGIC     'A'             AS home_away,
# MAGIC     away_coach_id   AS coach_id,
# MAGIC     home_coach_id   AS opponent_coach_id,
# MAGIC     away_race_id    AS race_id,
# MAGIC     home_race_id    AS opponent_race_id,
# MAGIC     td_away         AS td_for,
# MAGIC     td_home         AS td_against
# MAGIC   FROM g
# MAGIC )
# MAGIC SELECT
# MAGIC   game_id,
# MAGIC   tournament_id,
# MAGIC   event_timestamp,
# MAGIC   game_date,
# MAGIC   date_id,
# MAGIC   game_index,
# MAGIC   variant_id,
# MAGIC
# MAGIC   home_away,
# MAGIC   coach_id,
# MAGIC   opponent_coach_id,
# MAGIC   race_id,
# MAGIC   opponent_race_id,
# MAGIC
# MAGIC   td_for,
# MAGIC   td_against,
# MAGIC   td_for - td_against AS td_diff,
# MAGIC
# MAGIC   CASE
# MAGIC     WHEN td_for > td_against THEN 'W'
# MAGIC     WHEN td_for = td_against THEN 'D'
# MAGIC     ELSE 'L'
# MAGIC   END AS result_str,
# MAGIC
# MAGIC   CASE
# MAGIC     WHEN td_for > td_against THEN 1.0
# MAGIC     WHEN td_for = td_against THEN 0.5
# MAGIC     ELSE 0.0
# MAGIC   END AS result_numeric
# MAGIC FROM base;
# MAGIC

# COMMAND ----------

# DBTITLE 1,gold_fact.game_feed_for_ratings_fact
# MAGIC %sql -- VIEW: naf_catalog.gold_fact.game_feed_for_ratings_fact
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Ordered game feed for rating computation (adds k_value/n_eff/is_major_tournament).
# MAGIC -- LAYER        : GOLD_FACT
# MAGIC -- GRAIN        : 1 row per game_id
# MAGIC -- PRIMARY KEY  : (game_id)
# MAGIC -- FOREIGN KEYS : game_id        → naf_catalog.gold_fact.games_fact
# MAGIC --               tournament_id  → naf_catalog.gold_dim.tournament_dim
# MAGIC --               home_coach_id  → naf_catalog.gold_dim.coach_dim
# MAGIC --               away_coach_id  → naf_catalog.gold_dim.coach_dim
# MAGIC --               home_race_id   → naf_catalog.gold_dim.race_dim
# MAGIC --               away_race_id   → naf_catalog.gold_dim.race_dim
# MAGIC --               date_id        → naf_catalog.gold_dim.date_dim
# MAGIC --               (params) tournament_id → naf_catalog.gold_dim.tournament_parameters
# MAGIC -- SOURCES      : naf_catalog.gold_fact.games_fact
# MAGIC --               naf_catalog.gold_fact.game_global_order_spine_v
# MAGIC --               naf_catalog.gold_dim.tournament_parameters
# MAGIC --               naf_catalog.gold_dim.tournament_dim
# MAGIC -- NOTES        : Uses centralized deterministic ordering via game_index from game_global_order_spine_v.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_fact.game_feed_for_ratings_fact AS
# MAGIC SELECT
# MAGIC   g.game_id,
# MAGIC   go.game_index,
# MAGIC
# MAGIC   g.event_timestamp,
# MAGIC   g.game_date,
# MAGIC   g.date_id,
# MAGIC
# MAGIC   g.tournament_id,
# MAGIC   g.variant_id,
# MAGIC
# MAGIC   g.home_coach_id,
# MAGIC   g.away_coach_id,
# MAGIC   g.home_race_id,
# MAGIC   g.away_race_id,
# MAGIC
# MAGIC   g.td_home,
# MAGIC   g.td_away,
# MAGIC
# MAGIC   CASE
# MAGIC     WHEN g.td_home IS NULL OR g.td_away IS NULL THEN NULL
# MAGIC     WHEN g.td_home > g.td_away THEN 1.0
# MAGIC     WHEN g.td_home = g.td_away THEN 0.5
# MAGIC     ELSE 0.0
# MAGIC   END AS result_home,
# MAGIC
# MAGIC   CASE
# MAGIC     WHEN g.td_home IS NULL OR g.td_away IS NULL THEN NULL
# MAGIC     WHEN g.td_away > g.td_home THEN 1.0
# MAGIC     WHEN g.td_home = g.td_away THEN 0.5
# MAGIC     ELSE 0.0
# MAGIC   END AS result_away,
# MAGIC
# MAGIC   tp.k_value AS k_value,
# MAGIC   tp.n_eff   AS n_eff,
# MAGIC   t.is_major_tournament AS is_major_tournament
# MAGIC
# MAGIC FROM naf_catalog.gold_fact.games_fact g
# MAGIC INNER JOIN naf_catalog.gold_fact.game_global_order_spine_v go
# MAGIC   ON g.game_id = go.game_id
# MAGIC INNER JOIN naf_catalog.gold_dim.tournament_parameters tp
# MAGIC   ON tp.tournament_id = g.tournament_id
# MAGIC INNER JOIN naf_catalog.gold_dim.tournament_dim t
# MAGIC   ON t.tournament_id = g.tournament_id;
# MAGIC

# COMMAND ----------

# DBTITLE 1,gold_fact.rating_history_fact
# MAGIC %sql -- TABLE: naf_catalog.gold_fact.rating_history_fact
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Rating change history per game and scope (GLOBAL / RACE), for analysis and downstream summaries.
# MAGIC -- LAYER        : GOLD_FACT
# MAGIC -- GRAIN        : 1 row per (scope, rating_system, game_id, coach_id, race_id)
# MAGIC -- PRIMARY KEY  : (scope, rating_system, game_id, coach_id, race_id)
# MAGIC -- FOREIGN KEYS : game_id            → naf_catalog.gold_fact.games_fact
# MAGIC --               coach_id            → naf_catalog.gold_dim.coach_dim
# MAGIC --               opponent_coach_id   → naf_catalog.gold_dim.coach_dim
# MAGIC --               tournament_id       → naf_catalog.gold_dim.tournament_dim
# MAGIC --               date_id             → naf_catalog.gold_dim.date_dim
# MAGIC --               race_id             → naf_catalog.gold_dim.race_dim        (scope='RACE')
# MAGIC --               opponent_race_id    → naf_catalog.gold_dim.race_dim        (scope='RACE')
# MAGIC -- SOURCES      : naf_catalog.gold_fact.game_feed_for_ratings_fact + rating computation logic
# MAGIC -- NOTES        : - GLOBAL scope uses race_id=0 and opponent_race_id=0.
# MAGIC --               - date_id and game_index are carried from the rating feed (canonical ordering inputs).
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_fact.rating_history_fact (
# MAGIC   scope                    STRING,
# MAGIC   rating_system            STRING,
# MAGIC
# MAGIC   game_id                  INT,
# MAGIC   coach_id                 INT,
# MAGIC   race_id                  INT,        -- 0 allowed for GLOBAL
# MAGIC
# MAGIC   event_timestamp          TIMESTAMP,
# MAGIC   game_date                DATE,
# MAGIC   date_id                  INT,
# MAGIC   game_index               INT,
# MAGIC
# MAGIC   rating_before            DOUBLE,
# MAGIC   opponent_rating_before   DOUBLE,
# MAGIC   score_expected           DOUBLE,
# MAGIC   result_numeric           DOUBLE,
# MAGIC   rating_after             DOUBLE,
# MAGIC   rating_delta             DOUBLE,
# MAGIC
# MAGIC   opponent_coach_id        INT,
# MAGIC   opponent_race_id         INT,        -- 0 allowed for GLOBAL
# MAGIC
# MAGIC   tournament_id            INT,
# MAGIC   variant_id               INT,
# MAGIC
# MAGIC   is_major_tournament_used BOOLEAN,
# MAGIC   n_eff_used               INT,
# MAGIC   k_value_used             INT,
# MAGIC
# MAGIC   load_timestamp           TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (scope);
# MAGIC

# COMMAND ----------

# DBTITLE 1,ELO Rating Engine (PySpark)
# COMPONENT: ELO Rating Engine (PySpark)
# =============================================================================
# PURPOSE      : Compute Elo-style rating history for NAF coaches in two scopes:
#                - GLOBAL: one rating per coach across all races (race_id = 0)
#                - RACE  : one rating per (coach_id, race_id)
# LAYER        : GOLD_FACT
# INPUT        : naf_catalog.gold_fact.game_feed_for_ratings_fact
# OUTPUT       : naf_catalog.gold_fact.rating_history_fact
# GRAIN / PK   : 1 row per (scope, rating_system, game_id, coach_id, race_id)
# ORDERING     : Uses deterministic game_index from the feed.
# NOTES        : Burn-in thresholds are applied only in GOLD_SUMMARY post-threshold metrics (not here).
# =============================================================================

from pyspark.sql import functions as F, types as T

# ---------------------------------------------------------------------------
# Load analytical parameters from central config table
# ---------------------------------------------------------------------------
_cfg = spark.table("naf_catalog.gold_dim.analytical_config").first()
INITIAL_RATING = float(_cfg["elo_initial_rating"])
ELO_SCALE      = float(_cfg["elo_scale"])
SYSTEM_NAME    = "NAF_ELO"

# -----------------------------------------------------------------------------
# 1) Load the feed in correct order
# -----------------------------------------------------------------------------
feed_df = (
    spark.table("naf_catalog.gold_fact.game_feed_for_ratings_fact")
    .withColumn("game_index", F.col("game_index").cast("int"))
    .orderBy(F.col("game_index").asc(), F.col("game_id").asc())
    .select(
        "game_id",
        "game_index",
        "event_timestamp",
        "game_date",
        "date_id",
        "tournament_id",
        "variant_id",
        "home_coach_id",
        "away_coach_id",
        "home_race_id",
        "away_race_id",
        "result_home",
        "result_away",
        "k_value",
        "n_eff",
        "is_major_tournament",
    )
)

# -----------------------------------------------------------------------------
# 2) Fail-fast validation (align to Silver/Fact contracts)
# -----------------------------------------------------------------------------
bad = feed_df.filter(
    F.col("game_id").isNull()
    | F.col("game_index").isNull()
    | F.col("date_id").isNull()
    | F.col("home_coach_id").isNull()
    | F.col("away_coach_id").isNull()
    | (F.col("home_coach_id") == F.col("away_coach_id"))
    | F.col("home_race_id").isNull()
    | F.col("away_race_id").isNull()
    | (F.col("home_race_id") == 0)
    | (F.col("away_race_id") == 0)
    | F.col("k_value").isNull()
    | ~F.col("result_home").isin([0.0, 0.5, 1.0])
    | ~F.col("result_away").isin([0.0, 0.5, 1.0])
)

if len(bad.take(1)) > 0:
    raise ValueError("Invalid rows detected in rating feed (nulls / bad results / same coach / race_id=0).")

null_ts_count = feed_df.filter(F.col("event_timestamp").isNull()).count()
if null_ts_count > 0:
    print(f"WARNING: {null_ts_count} rows have NULL event_timestamp. Ordering still uses game_index.")

# Materialize ordered rows on driver for strictly sequential state updates
feed_rows = feed_df.collect()

# -----------------------------------------------------------------------------
# 3) Rating state containers
# -----------------------------------------------------------------------------
global_ratings = {}  # key: coach_id -> rating
race_ratings = {}    # key: (coach_id, race_id) -> rating

def get_rating(store, key):
    return store.get(key, INITIAL_RATING)

def set_rating(store, key, value):
    store[key] = value

def win_probability(r_you, r_opp):
    return 1.0 / (1.0 + (10.0 ** ((r_opp - r_you) / ELO_SCALE)))

# -----------------------------------------------------------------------------
# 4) Iterate over games and compute rating updates
# -----------------------------------------------------------------------------
rows = []

for row in feed_rows:
    game_id = int(row["game_id"])
    game_index = int(row["game_index"])

    event_ts = row["event_timestamp"]   # TIMESTAMP (nullable)
    game_date = row["game_date"]        # DATE (nullable)
    date_id = int(row["date_id"])       # required

    k_value = int(row["k_value"])       # required
    n_eff = int(row["n_eff"]) if row["n_eff"] is not None else None
    is_major = bool(row["is_major_tournament"]) if row["is_major_tournament"] is not None else False

    coach_home = int(row["home_coach_id"])
    coach_away = int(row["away_coach_id"])

    race_home = int(row["home_race_id"])
    race_away = int(row["away_race_id"])

    result_home = float(row["result_home"])
    result_away = float(row["result_away"])

    tournament_id = int(row["tournament_id"]) if row["tournament_id"] is not None else None
    variant_id = int(row["variant_id"]) if row["variant_id"] is not None else None

    # -------------------------
    # GLOBAL scope (race_id = 0)
    # -------------------------
    r_h = get_rating(global_ratings, coach_home)
    r_a = get_rating(global_ratings, coach_away)

    p_h = win_probability(r_h, r_a)
    p_a = win_probability(r_a, r_h)

    r_h_new = r_h + float(k_value) * (result_home - p_h)
    r_a_new = r_a + float(k_value) * (result_away - p_a)

    rows += [
        (
            "GLOBAL", SYSTEM_NAME,
            game_id, coach_home, 0,
            event_ts, game_date, date_id, game_index,
            r_h, r_a, p_h, result_home, r_h_new, (r_h_new - r_h),
            coach_away, 0,
            tournament_id, variant_id,
            is_major, n_eff, k_value,
        ),
        (
            "GLOBAL", SYSTEM_NAME,
            game_id, coach_away, 0,
            event_ts, game_date, date_id, game_index,
            r_a, r_h, p_a, result_away, r_a_new, (r_a_new - r_a),
            coach_home, 0,
            tournament_id, variant_id,
            is_major, n_eff, k_value,
        ),
    ]

    set_rating(global_ratings, coach_home, r_h_new)
    set_rating(global_ratings, coach_away, r_a_new)

    # ---------------
    # RACE scope
    # ---------------
    r_hr = get_rating(race_ratings, (coach_home, race_home))
    r_ar = get_rating(race_ratings, (coach_away, race_away))

    p_hr = win_probability(r_hr, r_ar)
    p_ar = win_probability(r_ar, r_hr)

    r_hr_new = r_hr + float(k_value) * (result_home - p_hr)
    r_ar_new = r_ar + float(k_value) * (result_away - p_ar)

    rows += [
        (
            "RACE", SYSTEM_NAME,
            game_id, coach_home, race_home,
            event_ts, game_date, date_id, game_index,
            r_hr, r_ar, p_hr, result_home, r_hr_new, (r_hr_new - r_hr),
            coach_away, race_away,
            tournament_id, variant_id,
            is_major, n_eff, k_value,
        ),
        (
            "RACE", SYSTEM_NAME,
            game_id, coach_away, race_away,
            event_ts, game_date, date_id, game_index,
            r_ar, r_hr, p_ar, result_away, r_ar_new, (r_ar_new - r_ar),
            coach_home, race_home,
            tournament_id, variant_id,
            is_major, n_eff, k_value,
        ),
    ]

    set_rating(race_ratings, (coach_home, race_home), r_hr_new)
    set_rating(race_ratings, (coach_away, race_away), r_ar_new)

# -----------------------------------------------------------------------------
# 5) Spark schema definition (aligned to gold_fact.rating_history_fact; no load_timestamp here)
# -----------------------------------------------------------------------------
schema = T.StructType([
    T.StructField("scope", T.StringType(), False),
    T.StructField("rating_system", T.StringType(), False),

    T.StructField("game_id", T.IntegerType(), False),
    T.StructField("coach_id", T.IntegerType(), False),
    T.StructField("race_id", T.IntegerType(), False),

    T.StructField("event_timestamp", T.TimestampType(), True),
    T.StructField("game_date", T.DateType(), True),
    T.StructField("date_id", T.IntegerType(), False),
    T.StructField("game_index", T.IntegerType(), False),

    T.StructField("rating_before", T.DoubleType(), False),
    T.StructField("opponent_rating_before", T.DoubleType(), False),
    T.StructField("score_expected", T.DoubleType(), False),
    T.StructField("result_numeric", T.DoubleType(), False),
    T.StructField("rating_after", T.DoubleType(), False),
    T.StructField("rating_delta", T.DoubleType(), False),

    T.StructField("opponent_coach_id", T.IntegerType(), False),
    T.StructField("opponent_race_id", T.IntegerType(), False),

    T.StructField("tournament_id", T.IntegerType(), True),
    T.StructField("variant_id", T.IntegerType(), True),

    T.StructField("is_major_tournament_used", T.BooleanType(), True),
    T.StructField("n_eff_used", T.IntegerType(), True),
    T.StructField("k_value_used", T.IntegerType(), True),
])

hist_df = (
    spark.createDataFrame(rows, schema)
    .withColumn("load_timestamp", F.current_timestamp())
)

# -----------------------------------------------------------------------------
# 6) Write (overwrite full table)
# -----------------------------------------------------------------------------
cols = [
    "scope","rating_system",
    "game_id","coach_id","race_id",
    "event_timestamp","game_date","date_id","game_index",
    "rating_before","opponent_rating_before","score_expected","result_numeric","rating_after","rating_delta",
    "opponent_coach_id","opponent_race_id",
    "tournament_id","variant_id",
    "is_major_tournament_used","n_eff_used","k_value_used",
    "load_timestamp",
]

target = "naf_catalog.gold_fact.rating_history_fact"

(
    hist_df.select(*cols)
    .write
    .format("delta")
    .mode("overwrite")          # explicit full overwrite
    .saveAsTable(target)
)


# COMMAND ----------

# DBTITLE 1,game feed for Elo engine
from pyspark.sql import functions as F

feed_df = (
    spark.table("naf_catalog.gold_fact.game_feed_for_ratings_fact")
    .withColumn("game_index", F.col("game_index").cast("int"))
    .select(
        "game_id","game_index","date_id","event_timestamp",
        "tournament_id","variant_id",
        "home_coach_id","away_coach_id",
        "home_race_id","away_race_id",
        "result_home","result_away",
        "k_value","n_eff","is_major_tournament",
    )
)

# Build a single "reason" column so we can see what triggered the fail-fast.
with_reason = (
    feed_df
    .withColumn(
        "bad_reason",
        F.when(F.col("game_id").isNull(), F.lit("game_id_null"))
         .when(F.col("game_index").isNull(), F.lit("game_index_null"))
         .when(F.col("date_id").isNull(), F.lit("date_id_null"))
         .when(F.col("home_coach_id").isNull(), F.lit("home_coach_id_null"))
         .when(F.col("away_coach_id").isNull(), F.lit("away_coach_id_null"))
         .when(F.col("home_coach_id") == F.col("away_coach_id"), F.lit("same_coach_home_away"))
         .when(F.col("k_value").isNull(), F.lit("k_value_null"))
         .when(~F.col("result_home").isin([0.0, 0.5, 1.0]), F.lit("result_home_invalid"))
         .when(~F.col("result_away").isin([0.0, 0.5, 1.0]), F.lit("result_away_invalid"))
         # Race checks (likely the cause)
         .when(F.col("home_race_id").isNull(), F.lit("home_race_id_null"))
         .when(F.col("away_race_id").isNull(), F.lit("away_race_id_null"))
         .when(F.col("home_race_id") == 0, F.lit("home_race_id_zero"))
         .when(F.col("away_race_id") == 0, F.lit("away_race_id_zero"))
         .otherwise(F.lit(None))
    )
)

bad = with_reason.filter(F.col("bad_reason").isNotNull())

print("Bad rows by reason:")
bad.groupBy("bad_reason").count().orderBy(F.col("count").desc()).show(50, truncate=False)

print("Sample bad rows:")
bad.select(
    "bad_reason",
    "game_id","game_index","date_id","event_timestamp",
    "home_coach_id","away_coach_id",
    "home_race_id","away_race_id",
    "result_home","result_away",
    "k_value","tournament_id"
).orderBy("bad_reason","game_index","game_id").show(50, truncate=False)

print("Quick coverage stats:")
feed_df.select(
    F.count("*").alias("rows"),
    F.sum(F.col("event_timestamp").isNull().cast("int")).alias("event_timestamp_nulls"),
    F.sum(F.col("date_id").isNull().cast("int")).alias("date_id_nulls"),
    F.sum((F.col("home_race_id").isNull() | (F.col("home_race_id") == 0)).cast("int")).alias("home_race_missing_or_zero"),
    F.sum((F.col("away_race_id").isNull() | (F.col("away_race_id") == 0)).cast("int")).alias("away_race_missing_or_zero"),
).show(truncate=False)


# COMMAND ----------

# DBTITLE 1,gold_fact.tournament_statistics_fact
# MAGIC %sql -- TABLE: naf_catalog.gold_fact.tournament_statistics_fact
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Tournament stat event rows for analysis and joins.
# MAGIC -- LAYER        : GOLD_FACT
# MAGIC -- GRAIN        : 1 row per (tournament_id, coach_id, race_id, stat_id, stat_date)
# MAGIC -- PRIMARY KEY  : (tournament_id, coach_id, race_id, stat_id, stat_date)
# MAGIC -- FOREIGN KEYS : tournament_id → naf_catalog.gold_dim.tournament_dim
# MAGIC --               coach_id      → naf_catalog.gold_dim.coach_dim
# MAGIC --               race_id       → naf_catalog.gold_dim.race_dim
# MAGIC --               stat_id       → naf_catalog.gold_dim.tournament_stat_dim
# MAGIC --               date_id       → naf_catalog.gold_dim.date_dim
# MAGIC -- SOURCES      : naf_catalog.silver.tournament_statistics_group_clean
# MAGIC -- NOTES        : - Per policy: NO dedup and NO validity filtering here; Silver enforces uniqueness/NOT NULL keys.
# MAGIC --               - stat_date is retained as the source event date (DATE); date_id is the canonical YYYYMMDD key.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_fact.tournament_statistics_fact
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   tournament_id,
# MAGIC   coach_id,
# MAGIC   race_id,
# MAGIC   stat_id,
# MAGIC   CAST(date_event AS DATE) AS stat_date,
# MAGIC   date_id,
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM naf_catalog.silver.tournament_statistics_group_clean;
# MAGIC
