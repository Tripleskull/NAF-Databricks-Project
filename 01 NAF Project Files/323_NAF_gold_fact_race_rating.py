# Databricks notebook source
# MAGIC %md
# MAGIC # 323 — Race-Aware Rating Engine
# MAGIC
# MAGIC **Layer:** GOLD_FACT &nbsp;|&nbsp; **Status:** Production
# MAGIC **Pipeline position:** Runs after 310 (config) and 320 (game feed)
# MAGIC
# MAGIC ## Model
# MAGIC
# MAGIC Each coach's strength with race `r` is decomposed as:
# MAGIC ```
# MAGIC θ_{i,r,t} = g_{i,t} + d_{i,r,t}
# MAGIC ```
# MAGIC where `g` is global skill and `d` is a race-specific deviation (prior mean = 0).
# MAGIC
# MAGIC **Stage 1 (this version):** Independent race deviations — no cross-race
# MAGIC covariance yet. Each game updates `g` and the played race's `d` via a
# MAGIC joint 2-dimensional EKF step.
# MAGIC
# MAGIC **Observation model:** Same logistic link as Elo/SSM:
# MAGIC ```
# MAGIC P(win) = 1 / (1 + 10^((θ_opp - θ_self) / S))
# MAGIC ```
# MAGIC where `θ_self = g_self + d_self_race` and `θ_opp = g_opp + d_opp_race`.
# MAGIC
# MAGIC ## Output
# MAGIC
# MAGIC - `naf_catalog.gold_fact.race_rating_history_fact` — 1 row per (game_id, coach_id)
# MAGIC
# MAGIC ## Design reference
# MAGIC
# MAGIC See `00 NAF Project Design/race_rating_model_plan.md` for full spec.

# COMMAND ----------

# DBTITLE 1,Load Analytical Config


# COMMAND ----------

# DBTITLE 1,Load Analytical Config (Stage 1 + Stage 2)
# =============================================================================
# COMPONENT: Load analytical parameters and shared constants
# =============================================================================

import math
import datetime as dt
from pyspark.sql import functions as F, types as T

_cfg = spark.table("naf_catalog.gold_dim.analytical_config").first()

# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------
INITIAL_RATING = float(_cfg["elo_initial_rating"])     # 150.0
ELO_SCALE      = float(_cfg["elo_scale"])              # 150.0
LN10_OVER_SCALE = math.log(10.0) / ELO_SCALE

# ---------------------------------------------------------------------------
# Stage 1: independent race deviations
# ---------------------------------------------------------------------------
RR_PRIOR_SIGMA_G = float(_cfg["rr_prior_sigma_g"])     # global prior SD
RR_PRIOR_SIGMA_D = float(_cfg["rr_prior_sigma_d"])     # race-deviation prior SD
RR_SIGMA2_OBS    = float(_cfg["rr_sigma2_obs"])        # observation noise
RR_Q_GLOBAL      = float(_cfg["rr_q_global"])          # per-game process noise for g
RR_Q_RACE        = float(_cfg["rr_q_race"])            # per-game process noise for d

# ---------------------------------------------------------------------------
# Stage 2: correlated race deviations
# ---------------------------------------------------------------------------
RR2_PRIOR_SIGMA_G = float(_cfg["rr2_prior_sigma_g"])   # global prior SD
RR2_PRIOR_SIGMA_D = float(_cfg["rr2_prior_sigma_d"])   # race-deviation prior SD
RR2_SIGMA2_OBS    = float(_cfg["rr2_sigma2_obs"])      # observation noise
RR2_Q_GLOBAL      = float(_cfg["rr2_q_global"])        # per-game process noise for g
RR2_Q_RACE        = float(_cfg["rr2_q_race"])          # per-game process noise for race vector

# Covariance-estimation / covariance-use controls
RR2_COV_SHRINKAGE_LAMBDA   = float(_cfg["rr2_cov_shrinkage_lambda"])
RR2_COV_MIN_GAMES_PER_RACE = int(_cfg["rr2_cov_min_games_per_race"])
RR2_COV_MIN_OVERLAP_COACHES = int(_cfg["rr2_cov_min_overlap_coaches"])
RR2_COV_EIGEN_FLOOR        = float(_cfg["rr2_cov_eigen_floor"])

# ---------------------------------------------------------------------------
# Output targets
# ---------------------------------------------------------------------------
RR_STAGE1_TARGET = "naf_catalog.gold_fact.race_rating_history_fact"
RR_STAGE2_TARGET = "naf_catalog.gold_fact.race_rating_corr_history_fact"
RR_STAGE2_COV_TABLE = "naf_catalog.gold_fact.race_correlation_matrix_fact"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
print(
    "Config loaded — Race rating Stage 1: "
    f"prior_σ_g={RR_PRIOR_SIGMA_G}  "
    f"prior_σ_d={RR_PRIOR_SIGMA_D}  "
    f"σ²_obs={RR_SIGMA2_OBS}  "
    f"q_global={RR_Q_GLOBAL}  "
    f"q_race={RR_Q_RACE}"
)

print(
    "Config loaded — Race rating Stage 2: "
    f"prior_σ_g={RR2_PRIOR_SIGMA_G}  "
    f"prior_σ_d={RR2_PRIOR_SIGMA_D}  "
    f"σ²_obs={RR2_SIGMA2_OBS}  "
    f"q_global={RR2_Q_GLOBAL}  "
    f"q_race={RR2_Q_RACE}  "
    f"λ_shrink={RR2_COV_SHRINKAGE_LAMBDA}  "
    f"min_games={RR2_COV_MIN_GAMES_PER_RACE}  "
    f"min_overlap={RR2_COV_MIN_OVERLAP_COACHES}  "
    f"eigen_floor={RR2_COV_EIGEN_FLOOR}"
)

# COMMAND ----------

# DBTITLE 1,Table DDL
# MAGIC %sql
# MAGIC -- =====================================================================
# MAGIC -- TABLE: naf_catalog.gold_fact.race_rating_history_fact
# MAGIC -- GRAIN        : 1 row per (game_id, coach_id)
# MAGIC -- PURPOSE      : Race-aware rating history per game.
# MAGIC --                Each row records pre-game and post-game state for
# MAGIC --                global skill (g) and played-race deviation (d).
# MAGIC --                Full race rating = g + d.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS naf_catalog.gold_fact.race_rating_history_fact (
# MAGIC   game_id                  INT          NOT NULL,
# MAGIC   coach_id                 INT          NOT NULL,
# MAGIC   coach_game_number        INT          NOT NULL,
# MAGIC
# MAGIC   event_timestamp          TIMESTAMP,
# MAGIC   game_date                DATE,
# MAGIC   date_id                  INT          NOT NULL,
# MAGIC   game_index               INT          NOT NULL,
# MAGIC
# MAGIC   race_id                  INT          NOT NULL,
# MAGIC
# MAGIC   -- Global skill component (g)
# MAGIC   g_before                 DOUBLE       NOT NULL,
# MAGIC   g_sigma_before           DOUBLE       NOT NULL,
# MAGIC   g_after                  DOUBLE       NOT NULL,
# MAGIC   g_sigma_after            DOUBLE       NOT NULL,
# MAGIC
# MAGIC   -- Race-deviation component (d) for the played race
# MAGIC   d_before                 DOUBLE       NOT NULL,
# MAGIC   d_sigma_before           DOUBLE       NOT NULL,
# MAGIC   d_after                  DOUBLE       NOT NULL,
# MAGIC   d_sigma_after            DOUBLE       NOT NULL,
# MAGIC
# MAGIC   -- Combined race rating (g + d)
# MAGIC   theta_before             DOUBLE       NOT NULL,
# MAGIC   theta_after              DOUBLE       NOT NULL,
# MAGIC
# MAGIC   -- Opponent state
# MAGIC   opponent_coach_id        INT          NOT NULL,
# MAGIC   opponent_race_id         INT          NOT NULL,
# MAGIC   opponent_theta_before    DOUBLE       NOT NULL,
# MAGIC
# MAGIC   -- Observation
# MAGIC   score_expected           DOUBLE       NOT NULL,
# MAGIC   result_numeric           DOUBLE       NOT NULL,
# MAGIC   innovation               DOUBLE       NOT NULL,
# MAGIC
# MAGIC   -- Context
# MAGIC   tournament_id            INT,
# MAGIC   variant_id               INT,
# MAGIC
# MAGIC   load_timestamp           TIMESTAMP    NOT NULL
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# DBTITLE 1,Race Rating Engine (Joint EKF)
# =============================================================================
# COMPONENT: Race-Aware Rating Engine
# =============================================================================
# PURPOSE      : Compute race-aware ratings for all coaches using a joint EKF
#                that updates global skill (g) and race deviation (d) together.
# INPUT        : naf_catalog.gold_fact.game_feed_for_ratings_fact
# CONFIG       : naf_catalog.gold_dim.analytical_config
# OUTPUT       : naf_catalog.gold_fact.race_rating_history_fact
# GRAIN / PK   : 1 row per (game_id, coach_id)
# =============================================================================
#
# STATE VECTOR per coach:
#   g         = global skill (scalar, initial = INITIAL_RATING)
#   d[race_r] = deviation for race r (scalar per race, initial = 0)
#   P_g       = variance of g
#   P_d[r]    = variance of d[r]
#   (Stage 1: g and d are independent — no cross-covariance P_gd)
#
# PREDICTION STEP (per game):
#   g_pred    = g_prev                    (no mean reversion)
#   d_pred[r] = d_prev[r]                 (no mean reversion)
#   P_g_pred  = P_g_prev + Q_GLOBAL
#   P_d_pred  = P_d_prev + Q_RACE         (for played race only)
#
# OBSERVATION MODEL:
#   θ_self = g_self + d_self_race
#   θ_opp  = g_opp  + d_opp_race
#   p = 1 / (1 + 10^((θ_opp - θ_self) / S))
#
# UPDATE STEP (joint EKF on [g, d_played_race]):
#   H = [h, h] where h = ln(10)/S × p × (1-p)    (Jacobian wrt g and d)
#   S_innov = h² × (P_g_pred + P_d_pred) + σ²_obs
#   K_g = h × P_g_pred / S_innov
#   K_d = h × P_d_pred / S_innov
#   innovation = result - p
#   g_post = g_pred + K_g × innovation
#   d_post = d_pred + K_d × innovation
#   P_g_post = (1 - K_g × h) × P_g_pred
#   P_d_post = (1 - K_d × h) × P_d_pred
#
# The information from one game is split between g and d proportionally to
# their uncertainties. When d has large variance (few games with that race),
# most of the update goes to d. When d is well-estimated, most goes to g.
# =============================================================================

# ---------------------------------------------------------------------------
# 1) Load feed
# ---------------------------------------------------------------------------
feed_df = (
    spark.table("naf_catalog.gold_fact.game_feed_for_ratings_fact")
    .withColumn("game_index", F.col("game_index").cast("int"))
    .orderBy(F.col("game_index").asc(), F.col("game_id").asc())
    .select(
        "game_id", "game_index", "event_timestamp", "game_date", "date_id",
        "tournament_id", "variant_id",
        "home_coach_id", "away_coach_id",
        "home_race_id", "away_race_id",
        "result_home", "result_away",
    )
)

# Fail-fast validation
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
    | ~F.col("result_home").isin([0.0, 0.5, 1.0])
    | ~F.col("result_away").isin([0.0, 0.5, 1.0])
)
if len(bad.take(1)) > 0:
    raise ValueError("Race rating: invalid rows detected in rating feed.")

feed_rows = feed_df.collect()
print(f"Feed loaded: {len(feed_rows)} games")

# ---------------------------------------------------------------------------
# 2) State containers
# ---------------------------------------------------------------------------
# Per-coach state: g, P_g
# Per-(coach, race) state: d, P_d
g_state = {}      # coach_id -> [g, P_g]
d_state = {}      # (coach_id, race_id) -> [d, P_d]
game_counts = {}  # coach_id -> count

PRIOR_P_G = RR_PRIOR_SIGMA_G ** 2
PRIOR_P_D = RR_PRIOR_SIGMA_D ** 2

def get_g(coach_id):
    """Get global skill state, initialising if needed."""
    if coach_id not in g_state:
        g_state[coach_id] = [INITIAL_RATING, PRIOR_P_G]
    return g_state[coach_id]

def get_d(coach_id, race_id):
    """Get race-deviation state, initialising if needed."""
    key = (coach_id, race_id)
    if key not in d_state:
        d_state[key] = [0.0, PRIOR_P_D]
    return d_state[key]

def win_prob(theta_self, theta_opp):
    """Logistic win probability on the Elo scale."""
    return 1.0 / (1.0 + 10.0 ** ((theta_opp - theta_self) / ELO_SCALE))

# ---------------------------------------------------------------------------
# 3) Main engine loop
# ---------------------------------------------------------------------------
rows = []

for row in feed_rows:
    game_id    = int(row["game_id"])
    game_index = int(row["game_index"])
    event_ts   = row["event_timestamp"]
    game_date  = row["game_date"]
    date_id    = int(row["date_id"])
    tourn_id   = int(row["tournament_id"]) if row["tournament_id"] is not None else None
    var_id     = int(row["variant_id"]) if row["variant_id"] is not None else None

    c_h = int(row["home_coach_id"])
    c_a = int(row["away_coach_id"])
    r_h = int(row["home_race_id"])
    r_a = int(row["away_race_id"])
    res_h = float(row["result_home"])
    res_a = float(row["result_away"])

    # --- Get pre-game state ---
    gs_h = get_g(c_h)
    gs_a = get_g(c_a)
    ds_h = get_d(c_h, r_h)
    ds_a = get_d(c_a, r_a)

    g_h, Pg_h = gs_h[0], gs_h[1]
    g_a, Pg_a = gs_a[0], gs_a[1]
    d_h, Pd_h = ds_h[0], ds_h[1]
    d_a, Pd_a = ds_a[0], ds_a[1]

    # --- Prediction step (add process noise) ---
    Pg_h_pred = Pg_h + RR_Q_GLOBAL
    Pg_a_pred = Pg_a + RR_Q_RACE  # typo guard: this is the away global
    Pd_h_pred = Pd_h + RR_Q_RACE
    Pd_a_pred = Pd_a + RR_Q_RACE

    # Fix: both coaches get Q_GLOBAL on their global component
    Pg_a_pred = Pg_a + RR_Q_GLOBAL

    # --- Combined ratings ---
    theta_h = g_h + d_h
    theta_a = g_a + d_a

    # --- Win probability ---
    p_h = win_prob(theta_h, theta_a)
    p_a = 1.0 - p_h

    # --- Jacobian ---
    # h = dp/dθ = ln(10)/S × p × (1-p)
    # For home coach: dθ/dg = 1, dθ/dd = 1
    # So H_home = [h, h] (wrt [g_h, d_h])
    h_val = LN10_OVER_SCALE * p_h * (1.0 - p_h)

    # --- Innovation ---
    innov_h = res_h - p_h
    innov_a = res_a - p_a

    # --- Home coach EKF update ---
    # S = h² × (Pg + Pd) + σ²_obs
    S_h = h_val * h_val * (Pg_h_pred + Pd_h_pred) + RR_SIGMA2_OBS
    Kg_h = h_val * Pg_h_pred / S_h
    Kd_h = h_val * Pd_h_pred / S_h

    g_h_post  = g_h + Kg_h * innov_h
    d_h_post  = d_h + Kd_h * innov_h
    Pg_h_post = max(1e-6, (1.0 - Kg_h * h_val) * Pg_h_pred)
    Pd_h_post = max(1e-6, (1.0 - Kd_h * h_val) * Pd_h_pred)

    # --- Away coach EKF update ---
    # Away coach's Jacobian uses p_a, but since p_a = 1 - p_h,
    # dp_a/dθ_a = ln(10)/S × p_a × (1 - p_a) = same h_val
    S_a = h_val * h_val * (Pg_a_pred + Pd_a_pred) + RR_SIGMA2_OBS
    Kg_a = h_val * Pg_a_pred / S_a
    Kd_a = h_val * Pd_a_pred / S_a

    g_a_post  = g_a + Kg_a * innov_a
    d_a_post  = d_a + Kd_a * innov_a
    Pg_a_post = max(1e-6, (1.0 - Kg_a * h_val) * Pg_a_pred)
    Pd_a_post = max(1e-6, (1.0 - Kd_a * h_val) * Pd_a_pred)

    # --- Update state ---
    gs_h[0], gs_h[1] = g_h_post, Pg_h_post
    gs_a[0], gs_a[1] = g_a_post, Pg_a_post
    ds_h[0], ds_h[1] = d_h_post, Pd_h_post
    ds_a[0], ds_a[1] = d_a_post, Pd_a_post

    # --- Game counts ---
    game_counts[c_h] = game_counts.get(c_h, 0) + 1
    game_counts[c_a] = game_counts.get(c_a, 0) + 1

    # --- Emit rows ---
    theta_h_post = g_h_post + d_h_post
    theta_a_post = g_a_post + d_a_post

    rows.append((
        game_id, c_h, game_counts[c_h],
        event_ts, game_date, date_id, game_index,
        r_h,
        g_h, math.sqrt(Pg_h_pred), g_h_post, math.sqrt(Pg_h_post),
        d_h, math.sqrt(Pd_h_pred), d_h_post, math.sqrt(Pd_h_post),
        theta_h, theta_h_post,
        c_a, r_a, theta_a,
        p_h, res_h, innov_h,
        tourn_id, var_id,
    ))
    rows.append((
        game_id, c_a, game_counts[c_a],
        event_ts, game_date, date_id, game_index,
        r_a,
        g_a, math.sqrt(Pg_a_pred), g_a_post, math.sqrt(Pg_a_post),
        d_a, math.sqrt(Pd_a_pred), d_a_post, math.sqrt(Pd_a_post),
        theta_a, theta_a_post,
        c_h, r_h, theta_h,
        p_a, res_a, innov_a,
        tourn_id, var_id,
    ))

print(f"Engine complete: {len(rows)} rows, {len(g_state)} coaches, "
      f"{len(d_state)} (coach, race) pairs")

# ---------------------------------------------------------------------------
# 4) Write to Delta
# ---------------------------------------------------------------------------
schema = T.StructType([
    T.StructField("game_id", T.IntegerType(), False),
    T.StructField("coach_id", T.IntegerType(), False),
    T.StructField("coach_game_number", T.IntegerType(), False),
    T.StructField("event_timestamp", T.TimestampType(), True),
    T.StructField("game_date", T.DateType(), True),
    T.StructField("date_id", T.IntegerType(), False),
    T.StructField("game_index", T.IntegerType(), False),
    T.StructField("race_id", T.IntegerType(), False),
    T.StructField("g_before", T.DoubleType(), False),
    T.StructField("g_sigma_before", T.DoubleType(), False),
    T.StructField("g_after", T.DoubleType(), False),
    T.StructField("g_sigma_after", T.DoubleType(), False),
    T.StructField("d_before", T.DoubleType(), False),
    T.StructField("d_sigma_before", T.DoubleType(), False),
    T.StructField("d_after", T.DoubleType(), False),
    T.StructField("d_sigma_after", T.DoubleType(), False),
    T.StructField("theta_before", T.DoubleType(), False),
    T.StructField("theta_after", T.DoubleType(), False),
    T.StructField("opponent_coach_id", T.IntegerType(), False),
    T.StructField("opponent_race_id", T.IntegerType(), False),
    T.StructField("opponent_theta_before", T.DoubleType(), False),
    T.StructField("score_expected", T.DoubleType(), False),
    T.StructField("result_numeric", T.DoubleType(), False),
    T.StructField("innovation", T.DoubleType(), False),
    T.StructField("tournament_id", T.IntegerType(), True),
    T.StructField("variant_id", T.IntegerType(), True),
])

hist_df = (
    spark.createDataFrame(rows, schema)
    .withColumn("load_timestamp", F.current_timestamp())
)

target = "naf_catalog.gold_fact.race_rating_history_fact"
hist_df.select(
    "game_id", "coach_id", "coach_game_number",
    "event_timestamp", "game_date", "date_id", "game_index",
    "race_id",
    "g_before", "g_sigma_before", "g_after", "g_sigma_after",
    "d_before", "d_sigma_before", "d_after", "d_sigma_after",
    "theta_before", "theta_after",
    "opponent_coach_id", "opponent_race_id", "opponent_theta_before",
    "score_expected", "result_numeric", "innovation",
    "tournament_id", "variant_id",
    "load_timestamp",
).write.mode("overwrite").saveAsTable(target)

n_written = spark.table(target).count()
print(f"✓ Wrote {n_written:,} rows to {target}")

# COMMAND ----------

# DBTITLE 1,Quick Diagnostics
# Quick sanity checks after engine run

import numpy as np

# Sample a few coaches to verify g + d = theta
check_df = spark.sql("""
    SELECT game_id, coach_id, race_id,
           g_before, d_before, theta_before,
           g_after, d_after, theta_after,
           score_expected, result_numeric
    FROM naf_catalog.gold_fact.race_rating_history_fact
    ORDER BY game_index DESC
    LIMIT 20
""").toPandas()

# Verify theta = g + d
check_df["theta_check_before"] = check_df["g_before"] + check_df["d_before"]
check_df["theta_check_after"] = check_df["g_after"] + check_df["d_after"]

max_err_before = abs(check_df["theta_before"] - check_df["theta_check_before"]).max()
max_err_after = abs(check_df["theta_after"] - check_df["theta_check_after"]).max()
print(f"θ = g + d consistency check:  max error before={max_err_before:.2e}  "
      f"after={max_err_after:.2e}")

# Coach/race summary
summary = spark.sql("""
    SELECT COUNT(DISTINCT coach_id) AS coaches,
           COUNT(DISTINCT race_id) AS races,
           COUNT(DISTINCT CONCAT(coach_id, '_', race_id)) AS coach_race_pairs,
           COUNT(*) AS total_rows,
           MIN(date_id) AS min_date,
           MAX(date_id) AS max_date
    FROM naf_catalog.gold_fact.race_rating_history_fact
""").first()

print(f"\nSummary: {summary['coaches']} coaches, {summary['races']} races, "
      f"{summary['coach_race_pairs']} (coach,race) pairs")
print(f"  {summary['total_rows']:,} total rows, "
      f"dates {summary['min_date']} to {summary['max_date']}")

# Global rating distribution (latest per coach)
latest_g = spark.sql("""
    WITH latest AS (
        SELECT coach_id, g_after,
               ROW_NUMBER() OVER (PARTITION BY coach_id ORDER BY game_index DESC) AS rn
        FROM naf_catalog.gold_fact.race_rating_history_fact
    )
    SELECT ROUND(AVG(g_after), 1) AS mean_g,
           ROUND(STDDEV(g_after), 1) AS sd_g,
           ROUND(MIN(g_after), 1) AS min_g,
           ROUND(MAX(g_after), 1) AS max_g
    FROM latest WHERE rn = 1
""").first()

print(f"  Latest g distribution: mean={latest_g['mean_g']}  "
      f"sd={latest_g['sd_g']}  range=[{latest_g['min_g']}, {latest_g['max_g']}]")

# COMMAND ----------

# DBTITLE 1,Coach Race Rating Plot
# ---------------------------------------------------------------------------
# Coach Race Rating Plot — paste into 323 or run standalone
# ---------------------------------------------------------------------------
import matplotlib.pyplot as plt
import numpy as np

COACH_ID = 9524  # change to any coach_id

plot_df = spark.sql(f"""
    SELECT rr.game_index, rr.date_id, rr.race_id, rd.race_name,
           rr.g_before, rr.g_after, rr.g_sigma_before,
           rr.d_before, rr.d_after, rr.d_sigma_before,
           rr.theta_before, rr.theta_after,
           rr.score_expected, rr.result_numeric,
           e.rating_after AS elo_global
    FROM naf_catalog.gold_fact.race_rating_history_fact rr
    LEFT JOIN naf_catalog.gold_dim.race_dim rd ON rd.race_id = rr.race_id
    LEFT JOIN naf_catalog.gold_fact.rating_history_fact e
        ON e.game_id = rr.game_id AND e.coach_id = rr.coach_id
        AND e.scope = 'GLOBAL'
    WHERE rr.coach_id = {COACH_ID}
    ORDER BY rr.game_index
""").toPandas()

if len(plot_df) == 0:
    print(f"No data for coach {COACH_ID}")
else:
    races_played = plot_df["race_name"].unique()
    n_races = len(races_played)

    fig, axes = plt.subplots(2, 2, figsize=(16, 10))

    # --- Panel 1: Global skill (g) vs Elo ---
    ax = axes[0, 0]
    ax.plot(plot_df["game_index"], plot_df["g_after"], label="g (global skill)", linewidth=1.5)
    ax.fill_between(
        plot_df["game_index"],
        plot_df["g_after"] - 2 * plot_df["g_sigma_before"],
        plot_df["g_after"] + 2 * plot_df["g_sigma_before"],
        alpha=0.15,
    )
    if "elo_global" in plot_df.columns and plot_df["elo_global"].notna().any():
        ax.plot(plot_df["game_index"], plot_df["elo_global"],
                label="Elo (global)", linewidth=1, alpha=0.6, linestyle="--")
    ax.set_ylabel("Rating")
    ax.set_title(f"Coach {COACH_ID} — Global Skill (g)")
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3)

    # --- Panel 2: Race deviations (d) over time ---
    ax = axes[0, 1]
    for race_name in races_played:
        rmask = plot_df["race_name"] == race_name
        rdf = plot_df[rmask]
        ax.plot(rdf["game_index"], rdf["d_after"], label=race_name, linewidth=1.2)
    ax.axhline(0, color="k", linestyle="--", alpha=0.3)
    ax.set_ylabel("Race Deviation (d)")
    ax.set_title(f"Race Deviations ({n_races} races)")
    ax.legend(fontsize=7, ncol=2)
    ax.grid(True, alpha=0.3)

    # --- Panel 3: Combined θ per race ---
    ax = axes[1, 0]
    for race_name in races_played:
        rmask = plot_df["race_name"] == race_name
        rdf = plot_df[rmask]
        ax.plot(rdf["game_index"], rdf["theta_after"], label=race_name, linewidth=1.2)
    ax.set_xlabel("Game index")
    ax.set_ylabel("θ = g + d")
    ax.set_title("Combined Race Ratings (θ)")
    ax.legend(fontsize=7, ncol=2)
    ax.grid(True, alpha=0.3)

    # --- Panel 4: Race deviation uncertainty (σ_d) ---
    ax = axes[1, 1]
    for race_name in races_played:
        rmask = plot_df["race_name"] == race_name
        rdf = plot_df[rmask]
        ax.plot(rdf["game_index"], rdf["d_sigma_before"], label=race_name, linewidth=1.2)
    ax.set_xlabel("Game index")
    ax.set_ylabel("σ_d (uncertainty)")
    ax.set_title("Race Deviation Uncertainty")
    ax.legend(fontsize=7, ncol=2)
    ax.grid(True, alpha=0.3)

    fig.suptitle(f"Race Rating Diagnostics — Coach {COACH_ID}", fontsize=14)
    fig.tight_layout()
    plt.show()

# COMMAND ----------

# DBTITLE 1,Create table gold_fact.race_correlation_matrix_fact
# MAGIC %sql
# MAGIC -- COMMAND ----------
# MAGIC -- DBTITLE 1,Stage 2 Covariance Table DDL
# MAGIC
# MAGIC -- =====================================================================
# MAGIC -- TABLE: naf_catalog.gold_fact.race_correlation_matrix_fact
# MAGIC -- GRAIN        : 1 row per (scope_type, variant_id, race_id_1, race_id_2)
# MAGIC -- PURPOSE      : Stores the estimated race-to-race covariance/correlation
# MAGIC --                matrix used by the Stage 2 correlated race rating model.
# MAGIC -- NOTES        :
# MAGIC --   - v1 uses one global matrix across all variants:
# MAGIC --       scope_type = 'GLOBAL_ALL_VARIANTS'
# MAGIC --       variant_id = NULL
# MAGIC --   - schema is future-proofed for later single-variant covariance models
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS naf_catalog.gold_fact.race_correlation_matrix_fact (
# MAGIC   scope_type              STRING       NOT NULL,
# MAGIC   variant_id              INT,
# MAGIC
# MAGIC   race_id_1               INT          NOT NULL,
# MAGIC   race_id_2               INT          NOT NULL,
# MAGIC
# MAGIC   cov_value               DOUBLE       NOT NULL,
# MAGIC   corr_value              DOUBLE       NOT NULL,
# MAGIC
# MAGIC   n_overlap_coaches       INT,
# MAGIC   maturity_min_games      INT          NOT NULL,
# MAGIC   shrinkage_lambda        DOUBLE       NOT NULL,
# MAGIC   eigen_floor             DOUBLE       NOT NULL,
# MAGIC
# MAGIC   estimation_basis        STRING       NOT NULL,
# MAGIC   load_timestamp          TIMESTAMP    NOT NULL
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------
# DBTITLE 1,Stage 2 Covariance Estimation from Mature Stage 1 Deviations

# =============================================================================
# COMPONENT: Stage 2 covariance estimation
# =============================================================================
# PURPOSE      : Estimate one global race-to-race covariance/correlation matrix
#                from final mature Stage 1 race deviations.
# INPUT        : naf_catalog.gold_fact.race_rating_history_fact
# OUTPUT       : naf_catalog.gold_fact.race_correlation_matrix_fact
# GRAIN / PK   : 1 row per (scope_type, variant_id, race_id_1, race_id_2)
# DESIGN       :
#   - maturity basis = final d_after per (coach_id, race_id)
#   - maturity rule  = coach-race pairs with at least 10 games
#   - scope          = one global matrix across all variants
# =============================================================================

import numpy as np
import pandas as pd
from pyspark.sql import functions as F, types as T

SCOPE_TYPE = "GLOBAL_ALL_VARIANTS"
VARIANT_ID = None
MATURITY_MIN_GAMES = 10
ESTIMATION_BASIS = "FINAL_MATURE_D_AFTER"

# ---------------------------------------------------------------------------
# 1) Extract final mature Stage 1 race deviations
# ---------------------------------------------------------------------------
# We reconstruct coach-race game counts from the Stage 1 history table, then
# keep the final row per (coach_id, race_id) and require at least 10 games.

stage1_hist = spark.table(RR_STAGE1_TARGET)

coach_race_final_df = spark.sql(f"""
WITH ranked AS (
    SELECT
        game_id,
        coach_id,
        race_id,
        game_index,
        d_after,
        ROW_NUMBER() OVER (
            PARTITION BY coach_id, race_id
            ORDER BY game_index DESC, game_id DESC
        ) AS rn_last,
        COUNT(*) OVER (
            PARTITION BY coach_id, race_id
        ) AS coach_race_games
    FROM {RR_STAGE1_TARGET}
),
final_mature AS (
    SELECT
        coach_id,
        race_id,
        d_after,
        coach_race_games
    FROM ranked
    WHERE rn_last = 1
      AND coach_race_games >= {MATURITY_MIN_GAMES}
)
SELECT
    coach_id,
    race_id,
    d_after,
    coach_race_games
FROM final_mature
""")

n_mature_rows = coach_race_final_df.count()
n_mature_coaches = coach_race_final_df.select("coach_id").distinct().count()
n_mature_races = coach_race_final_df.select("race_id").distinct().count()

print(
    f"Mature Stage 1 extraction complete: {n_mature_rows:,} coach-race rows, "
    f"{n_mature_coaches:,} coaches, {n_mature_races:,} races"
)

if n_mature_rows == 0:
    raise ValueError(
        "Stage 2 covariance estimation: no mature coach-race rows found. "
        "Check Stage 1 output and maturity threshold."
    )

# ---------------------------------------------------------------------------
# 2) Pivot to coach-by-race matrix in pandas
# ---------------------------------------------------------------------------
mature_pdf = coach_race_final_df.toPandas()

pivot_df = mature_pdf.pivot(
    index="coach_id",
    columns="race_id",
    values="d_after",
).sort_index(axis=1)

race_ids = [int(c) for c in pivot_df.columns.tolist()]
n_races = len(race_ids)

print(f"Pivot complete: {pivot_df.shape[0]:,} coaches x {pivot_df.shape[1]:,} races")

if n_races < 2:
    raise ValueError(
        "Stage 2 covariance estimation: fewer than 2 mature races available."
    )

# ---------------------------------------------------------------------------
# 3) Pairwise overlap counts
# ---------------------------------------------------------------------------
# overlap_counts[i, j] = number of coaches with observed mature values
# for both race i and race j
obs_mask = (~pivot_df.isna()).astype(int).to_numpy()
overlap_counts = obs_mask.T @ obs_mask

# ---------------------------------------------------------------------------
# 4) Raw covariance estimate using pairwise complete observations
# ---------------------------------------------------------------------------
# pandas.DataFrame.cov() uses pairwise complete observations.
raw_cov_df = pivot_df.cov(min_periods=RR2_COV_MIN_OVERLAP_COACHES)

# Ensure square matrix over the full mature race set
raw_cov_df = raw_cov_df.reindex(index=race_ids, columns=race_ids)

# ---------------------------------------------------------------------------
# 5) Fallback/repair for missing values
# ---------------------------------------------------------------------------
# Some pairs may still be NaN if overlap is too low. We set:
#   - diagonal NaN -> empirical variance if possible, else prior variance proxy
#   - off-diagonal NaN -> 0.0
#
# This is conservative and works naturally with later shrinkage.
prior_var_d = RR2_PRIOR_SIGMA_D ** 2

for r in race_ids:
    if pd.isna(raw_cov_df.loc[r, r]):
        col_vals = pivot_df[r].dropna()
        if len(col_vals) >= 2:
            raw_cov_df.loc[r, r] = float(col_vals.var(ddof=1))
        else:
            raw_cov_df.loc[r, r] = prior_var_d

raw_cov_df = raw_cov_df.fillna(0.0)

raw_cov = raw_cov_df.to_numpy(dtype=float)

# Symmetrise explicitly
raw_cov = 0.5 * (raw_cov + raw_cov.T)

# ---------------------------------------------------------------------------
# 6) Shrinkage toward diagonal
# ---------------------------------------------------------------------------
diag_cov = np.diag(np.diag(raw_cov))
shrunk_cov = (
    (1.0 - RR2_COV_SHRINKAGE_LAMBDA) * raw_cov
    + RR2_COV_SHRINKAGE_LAMBDA * diag_cov
)

# ---------------------------------------------------------------------------
# 7) PSD repair by eigenvalue flooring
# ---------------------------------------------------------------------------
eigvals, eigvecs = np.linalg.eigh(shrunk_cov)
eigvals_floored = np.maximum(eigvals, RR2_COV_EIGEN_FLOOR)
psd_cov = eigvecs @ np.diag(eigvals_floored) @ eigvecs.T
psd_cov = 0.5 * (psd_cov + psd_cov.T)

min_eig_before = float(np.min(eigvals))
min_eig_after = float(np.min(np.linalg.eigvalsh(psd_cov)))

# ---------------------------------------------------------------------------
# 8) Correlation matrix
# ---------------------------------------------------------------------------
stds = np.sqrt(np.maximum(np.diag(psd_cov), RR2_COV_EIGEN_FLOOR))
denom = np.outer(stds, stds)
corr = np.divide(
    psd_cov,
    denom,
    out=np.zeros_like(psd_cov),
    where=denom > 0,
)

# Clamp tiny numerical noise
corr = np.clip(corr, -1.0, 1.0)
np.fill_diagonal(corr, 1.0)

# ---------------------------------------------------------------------------
# 9) Flatten to Spark rows
# ---------------------------------------------------------------------------
load_ts = dt.datetime.utcnow()

rows_out = []
for i, race_id_1 in enumerate(race_ids):
    for j, race_id_2 in enumerate(race_ids):
        rows_out.append((
            SCOPE_TYPE,
            VARIANT_ID,
            int(race_id_1),
            int(race_id_2),
            float(psd_cov[i, j]),
            float(corr[i, j]),
            int(overlap_counts[i, j]),
            int(MATURITY_MIN_GAMES),
            float(RR2_COV_SHRINKAGE_LAMBDA),
            float(RR2_COV_EIGEN_FLOOR),
            ESTIMATION_BASIS,
            load_ts,
        ))

schema = T.StructType([
    T.StructField("scope_type", T.StringType(), False),
    T.StructField("variant_id", T.IntegerType(), True),
    T.StructField("race_id_1", T.IntegerType(), False),
    T.StructField("race_id_2", T.IntegerType(), False),
    T.StructField("cov_value", T.DoubleType(), False),
    T.StructField("corr_value", T.DoubleType(), False),
    T.StructField("n_overlap_coaches", T.IntegerType(), True),
    T.StructField("maturity_min_games", T.IntegerType(), False),
    T.StructField("shrinkage_lambda", T.DoubleType(), False),
    T.StructField("eigen_floor", T.DoubleType(), False),
    T.StructField("estimation_basis", T.StringType(), False),
    T.StructField("load_timestamp", T.TimestampType(), False),
])

cov_out_df = spark.createDataFrame(rows_out, schema)

# ---------------------------------------------------------------------------
# 10) Write covariance table
# ---------------------------------------------------------------------------
(
    cov_out_df
    .write
    .mode("overwrite")
    .saveAsTable(RR_STAGE2_COV_TABLE)
)

n_written = spark.table(RR_STAGE2_COV_TABLE).count()

print(f"✓ Wrote {n_written:,} rows to {RR_STAGE2_COV_TABLE}")
print(
    f"Covariance diagnostics: races={n_races}, "
    f"min_eig_before={min_eig_before:.6f}, "
    f"min_eig_after={min_eig_after:.6f}"
)

# Optional quick preview
display(
    spark.table(RR_STAGE2_COV_TABLE)
    .orderBy("race_id_1", "race_id_2")
    .limit(20)
)

# COMMAND ----------

# DBTITLE 1,Create table gold_fact.race_rating_corr_history_fact
# MAGIC %sql
# MAGIC -- COMMAND ----------
# MAGIC -- DBTITLE 1,Stage 2 History Table DDL
# MAGIC
# MAGIC -- =====================================================================
# MAGIC -- TABLE: naf_catalog.gold_fact.race_rating_corr_history_fact
# MAGIC -- GRAIN        : 1 row per (game_id, coach_id)
# MAGIC -- PURPOSE      : Race-aware rating history for the Stage 2 correlated model.
# MAGIC --                Each row records pre-game and post-game state for:
# MAGIC --                  - global skill (g)
# MAGIC --                  - played-race deviation d[r_played]
# MAGIC --                  - combined played-race rating theta = g + d[r_played]
# MAGIC --                The full race vector and covariance live in memory during
# MAGIC --                engine execution; this table stores the played-race slice
# MAGIC --                needed for evaluation and diagnostics.
# MAGIC -- NOTES        :
# MAGIC --   - Stage 2 uses a learned race-to-race covariance matrix.
# MAGIC --   - A game with one race can update other race deviations indirectly,
# MAGIC --     but only the played-race slice is persisted here.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS naf_catalog.gold_fact.race_rating_corr_history_fact (
# MAGIC   game_id                        INT          NOT NULL,
# MAGIC   coach_id                       INT          NOT NULL,
# MAGIC   coach_game_number              INT          NOT NULL,
# MAGIC
# MAGIC   event_timestamp                TIMESTAMP,
# MAGIC   game_date                      DATE,
# MAGIC   date_id                        INT          NOT NULL,
# MAGIC   game_index                     INT          NOT NULL,
# MAGIC
# MAGIC   race_id                        INT          NOT NULL,
# MAGIC
# MAGIC   -- Global skill component (g)
# MAGIC   g_before                       DOUBLE       NOT NULL,
# MAGIC   g_sigma_before                 DOUBLE       NOT NULL,
# MAGIC   g_after                        DOUBLE       NOT NULL,
# MAGIC   g_sigma_after                  DOUBLE       NOT NULL,
# MAGIC
# MAGIC   -- Played-race deviation d[r_played]
# MAGIC   d_played_before                DOUBLE       NOT NULL,
# MAGIC   d_played_sigma_before          DOUBLE       NOT NULL,
# MAGIC   d_played_after                 DOUBLE       NOT NULL,
# MAGIC   d_played_sigma_after           DOUBLE       NOT NULL,
# MAGIC
# MAGIC   -- Combined played-race rating theta = g + d[r_played]
# MAGIC   theta_before                   DOUBLE       NOT NULL,
# MAGIC   theta_sigma_before             DOUBLE       NOT NULL,
# MAGIC   theta_after                    DOUBLE       NOT NULL,
# MAGIC   theta_sigma_after              DOUBLE       NOT NULL,
# MAGIC
# MAGIC   -- Opponent played-race state
# MAGIC   opponent_coach_id              INT          NOT NULL,
# MAGIC   opponent_race_id               INT          NOT NULL,
# MAGIC   opponent_theta_before          DOUBLE       NOT NULL,
# MAGIC   opponent_theta_sigma_before    DOUBLE       NOT NULL,
# MAGIC
# MAGIC   -- Observation
# MAGIC   score_expected                 DOUBLE       NOT NULL,
# MAGIC   result_numeric                 DOUBLE       NOT NULL,
# MAGIC   innovation                     DOUBLE       NOT NULL,
# MAGIC
# MAGIC   -- Optional EKF diagnostics
# MAGIC   kalman_gain_g                  DOUBLE,
# MAGIC   kalman_gain_d_played           DOUBLE,
# MAGIC
# MAGIC   -- Context
# MAGIC   tournament_id                  INT,
# MAGIC   variant_id                     INT,
# MAGIC
# MAGIC   load_timestamp                 TIMESTAMP    NOT NULL
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------
# DBTITLE 1,Stage 2 Covariance Load + Race Index Helpers

# =============================================================================
# COMPONENT: Stage 2 covariance load
# =============================================================================
# PURPOSE      : Load the persisted Stage 2 race covariance/correlation matrix
#                and reconstruct dense numpy matrices plus race-index helpers
#                for the correlated race-rating engine.
# INPUT        : naf_catalog.gold_fact.race_correlation_matrix_fact
# OUTPUT       : in-memory helpers for Stage 2 engine
# NOTES        :
#   - v1 uses one global matrix across all variants:
#       scope_type = 'GLOBAL_ALL_VARIANTS'
#       variant_id = NULL
#   - matrices are aligned to race_ids_sorted
# =============================================================================

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# 1) Load covariance table
# ---------------------------------------------------------------------------
cov_df = spark.table(RR_STAGE2_COV_TABLE).filter(
    (F.col("scope_type") == "GLOBAL_ALL_VARIANTS") &
    (F.col("variant_id").isNull())
)

n_cov_rows = cov_df.count()
print(f"Stage 2 covariance rows loaded: {n_cov_rows:,}")

if n_cov_rows == 0:
    raise ValueError(
        "Stage 2 covariance load: no rows found in race_correlation_matrix_fact. "
        "Run the covariance estimation cell first."
    )

cov_pdf = cov_df.select(
    "race_id_1",
    "race_id_2",
    "cov_value",
    "corr_value",
    "n_overlap_coaches",
    "maturity_min_games",
    "shrinkage_lambda",
    "eigen_floor",
    "estimation_basis",
).toPandas()

# ---------------------------------------------------------------------------
# 2) Race ordering + sanity checks
# ---------------------------------------------------------------------------
race_ids_1 = sorted(cov_pdf["race_id_1"].dropna().astype(int).unique().tolist())
race_ids_2 = sorted(cov_pdf["race_id_2"].dropna().astype(int).unique().tolist())

if race_ids_1 != race_ids_2:
    raise ValueError(
        "Stage 2 covariance load: race_id_1 and race_id_2 sets do not match."
    )

race_ids_sorted = race_ids_1
n_races = len(race_ids_sorted)

print(f"Stage 2 covariance matrix dimensions: {n_races} x {n_races}")

if n_races < 2:
    raise ValueError(
        "Stage 2 covariance load: fewer than 2 races in matrix."
    )

race_to_idx = {race_id: idx for idx, race_id in enumerate(race_ids_sorted)}
idx_to_race = {idx: race_id for race_id, idx in race_to_idx.items()}

# ---------------------------------------------------------------------------
# 3) Dense covariance / correlation matrices
# ---------------------------------------------------------------------------
cov_matrix_df = (
    cov_pdf
    .pivot(index="race_id_1", columns="race_id_2", values="cov_value")
    .reindex(index=race_ids_sorted, columns=race_ids_sorted)
)

corr_matrix_df = (
    cov_pdf
    .pivot(index="race_id_1", columns="race_id_2", values="corr_value")
    .reindex(index=race_ids_sorted, columns=race_ids_sorted)
)

cov_matrix = cov_matrix_df.to_numpy(dtype=float)
corr_matrix = corr_matrix_df.to_numpy(dtype=float)

# ---------------------------------------------------------------------------
# 4) Matrix repair / validation
# ---------------------------------------------------------------------------
cov_matrix = 0.5 * (cov_matrix + cov_matrix.T)
corr_matrix = 0.5 * (corr_matrix + corr_matrix.T)

if np.isnan(cov_matrix).any():
    raise ValueError("Stage 2 covariance load: NaN values found in covariance matrix.")

if np.isnan(corr_matrix).any():
    raise ValueError("Stage 2 covariance load: NaN values found in correlation matrix.")

eigvals_cov = np.linalg.eigvalsh(cov_matrix)
min_eig_cov = float(np.min(eigvals_cov))
max_eig_cov = float(np.max(eigvals_cov))

diag_cov = np.diag(cov_matrix)
diag_corr = np.diag(corr_matrix)

if np.any(diag_cov <= 0):
    raise ValueError("Stage 2 covariance load: non-positive diagonal found in covariance matrix.")

# Clamp tiny numerical drift in corr diagonal
corr_matrix = np.clip(corr_matrix, -1.0, 1.0)
np.fill_diagonal(corr_matrix, 1.0)

print(
    "Stage 2 covariance diagnostics: "
    f"min_eig={min_eig_cov:.6f}  "
    f"max_eig={max_eig_cov:.6f}  "
    f"mean_var={float(np.mean(diag_cov)):.6f}"
)

# ---------------------------------------------------------------------------
# 5) Useful derived matrices
# ---------------------------------------------------------------------------
# Prior covariance for the race-deviation vector d
RR2_PRIOR_COV_D = (RR2_PRIOR_SIGMA_D ** 2) * corr_matrix

# Per-game process covariance for the race-deviation vector d
RR2_Q_COV_D = RR2_Q_RACE * corr_matrix

# Safety symmetrisation
RR2_PRIOR_COV_D = 0.5 * (RR2_PRIOR_COV_D + RR2_PRIOR_COV_D.T)
RR2_Q_COV_D = 0.5 * (RR2_Q_COV_D + RR2_Q_COV_D.T)

print(
    "Derived Stage 2 matrices built: "
    f"prior_cov_shape={RR2_PRIOR_COV_D.shape}  "
    f"q_cov_shape={RR2_Q_COV_D.shape}"
)

# ---------------------------------------------------------------------------
# 6) Helper accessors
# ---------------------------------------------------------------------------
def rr2_race_index(race_id: int) -> int:
    """Map race_id to dense matrix index used by Stage 2 covariance matrices."""
    if race_id not in race_to_idx:
        raise KeyError(f"Stage 2 race index: race_id {race_id} not found in covariance matrix.")
    return race_to_idx[race_id]

def rr2_unit_vec_for_race(race_id: int) -> np.ndarray:
    """Return one-hot vector e_r for the given race in the Stage 2 race space."""
    idx = rr2_race_index(int(race_id))
    e = np.zeros(n_races, dtype=float)
    e[idx] = 1.0
    return e

# Optional quick preview
preview_pdf = pd.DataFrame(
    corr_matrix[:min(8, n_races), :min(8, n_races)],
    index=race_ids_sorted[:min(8, n_races)],
    columns=race_ids_sorted[:min(8, n_races)],
)

display(preview_pdf)

# COMMAND ----------
# DBTITLE 1,Stage 2 Feed Load + State Containers + Helper Functions

# =============================================================================
# COMPONENT: Stage 2 correlated race-rating engine setup
# =============================================================================
# PURPOSE      : Load the game feed, initialise state containers, and define
#                helper functions for the Stage 2 correlated race-rating model.
# MODEL        :
#   For each coach i:
#       g_i          = global skill scalar
#       d_i          = full race-deviation vector (length = n_races)
#       P_g_i        = variance of global skill
#       P_d_i        = covariance matrix of race-deviation vector
#
#   Played-race strength:
#       theta_i(r) = g_i + d_i[r]
#
# NOTES        :
#   - Stage 2 uses one shared race-correlation matrix for all coaches.
#   - One game updates g and the full d-vector indirectly through covariance.
#   - This cell only sets up state and helpers. The engine loop comes next.
# =============================================================================

import math
import datetime as dt
import numpy as np
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# 1) Load feed in deterministic order
# ---------------------------------------------------------------------------
rr2_feed_df = (
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
    )
)

rr2_bad = rr2_feed_df.filter(
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
    | ~F.col("result_home").isin([0.0, 0.5, 1.0])
    | ~F.col("result_away").isin([0.0, 0.5, 1.0])
)

if len(rr2_bad.take(1)) > 0:
    raise ValueError("Stage 2 race rating: invalid rows detected in rating feed.")

rr2_feed_rows = rr2_feed_df.collect()
print(f"Stage 2 feed loaded: {len(rr2_feed_rows)} games")

# ---------------------------------------------------------------------------
# 2) State containers
# ---------------------------------------------------------------------------
# Per coach:
#   g      : global skill scalar
#   P_g    : global-skill variance
#   d_vec  : full race-deviation vector (length n_races)
#   P_d    : full race covariance matrix (n_races x n_races)
#
# Also track total games and per-race game counts for reporting/evaluation.
rr2_state = {}             # coach_id -> {"g": ..., "P_g": ..., "d_vec": ..., "P_d": ...}
rr2_game_counts = {}       # coach_id -> total game count
rr2_race_game_counts = {}  # (coach_id, race_id) -> game count with that race

RR2_PRIOR_P_G = RR2_PRIOR_SIGMA_G ** 2

# Safety copies of the shared matrices so accidental in-place modification
# does not affect the templates.
RR2_PRIOR_COV_D_TEMPLATE = RR2_PRIOR_COV_D.copy()
RR2_Q_COV_D_TEMPLATE = RR2_Q_COV_D.copy()

# ---------------------------------------------------------------------------
# 3) Helper functions
# ---------------------------------------------------------------------------
def rr2_default_state():
    """Initial state for a new coach."""
    return {
        "g": float(INITIAL_RATING),
        "P_g": float(RR2_PRIOR_P_G),
        "d_vec": np.zeros(n_races, dtype=float),
        "P_d": RR2_PRIOR_COV_D_TEMPLATE.copy(),
    }

def rr2_get_state(coach_id: int):
    """Get coach state, initialising if needed."""
    if coach_id not in rr2_state:
        rr2_state[coach_id] = rr2_default_state()
    return rr2_state[coach_id]

def rr2_set_state(coach_id: int, state_dict: dict):
    """Persist coach state."""
    rr2_state[coach_id] = state_dict

def rr2_increment_game_count(coach_id: int) -> int:
    """Increment and return total game count for coach."""
    rr2_game_counts[coach_id] = rr2_game_counts.get(coach_id, 0) + 1
    return rr2_game_counts[coach_id]

def rr2_increment_race_game_count(coach_id: int, race_id: int) -> int:
    """Increment and return game count for coach with specific race."""
    key = (coach_id, race_id)
    rr2_race_game_counts[key] = rr2_race_game_counts.get(key, 0) + 1
    return rr2_race_game_counts[key]

def rr2_get_race_game_count(coach_id: int, race_id: int) -> int:
    """Return prior number of games for coach with given race."""
    return rr2_race_game_counts.get((coach_id, race_id), 0)

def rr2_win_probability(theta_self: float, theta_opp: float) -> float:
    """Logistic win probability on the Elo scale."""
    return 1.0 / (1.0 + 10.0 ** ((theta_opp - theta_self) / ELO_SCALE))

def rr2_theta_from_state(state_dict: dict, race_id: int) -> float:
    """Combined played-race rating theta = g + d[r]."""
    idx = rr2_race_index(int(race_id))
    return float(state_dict["g"] + state_dict["d_vec"][idx])

def rr2_theta_variance_from_state(state_dict: dict, race_id: int) -> float:
    """
    Variance of theta = g + d[r].

    For v1 of Stage 2 we keep g independent of d-vector, so:
        Var(theta) = Var(g) + Var(d[r])
    """
    idx = rr2_race_index(int(race_id))
    return float(max(state_dict["P_g"] + state_dict["P_d"][idx, idx], RR2_COV_EIGEN_FLOOR))

def rr2_predict_state(state_dict: dict):
    """
    Prediction step for one game:
      g_pred   = g_prev
      P_g_pred = P_g_prev + q_global

      d_pred   = d_prev
      P_d_pred = P_d_prev + Q_cov_d
    """
    g_pred = float(state_dict["g"])
    P_g_pred = float(state_dict["P_g"] + RR2_Q_GLOBAL)

    d_pred = state_dict["d_vec"].copy()
    P_d_pred = state_dict["P_d"] + RR2_Q_COV_D_TEMPLATE
    P_d_pred = 0.5 * (P_d_pred + P_d_pred.T)

    return {
        "g": g_pred,
        "P_g": max(P_g_pred, RR2_COV_EIGEN_FLOOR),
        "d_vec": d_pred,
        "P_d": P_d_pred,
    }

def rr2_observation_components(state_self_pred: dict, race_self: int,
                               state_opp_pred: dict, race_opp: int):
    """
    Compute pre-game played-race strengths and EKF linearisation term.

    Returns
    -------
    theta_self, theta_opp, p_exp, h
    where h = d/d(theta_self) logistic expected score.
    """
    theta_self = rr2_theta_from_state(state_self_pred, race_self)
    theta_opp = rr2_theta_from_state(state_opp_pred, race_opp)

    p_exp = rr2_win_probability(theta_self, theta_opp)
    h = LN10_OVER_SCALE * p_exp * (1.0 - p_exp)

    return theta_self, theta_opp, p_exp, h

def rr2_played_race_variance(state_dict: dict, race_id: int) -> float:
    """Variance of played-race deviation d[r]."""
    idx = rr2_race_index(int(race_id))
    return float(max(state_dict["P_d"][idx, idx], RR2_COV_EIGEN_FLOOR))

print(
    "Stage 2 state setup complete: "
    f"n_races={n_races}, "
    f"prior_var_g={RR2_PRIOR_P_G:.4f}, "
    f"mean_prior_var_d={float(np.mean(np.diag(RR2_PRIOR_COV_D_TEMPLATE))):.4f}"
)

# COMMAND ----------
# DBTITLE 1,Stage 2 Covariance Diagnostics: Correlation + Overlap Heatmaps (with race names)

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# 0) Table names
# ---------------------------------------------------------------------------
RR_STAGE2_COV_TABLE = "naf_catalog.gold_fact.race_correlation_matrix_fact"
RACE_DIM_TABLE = "naf_catalog.gold_dim.race_dim"

print(f"Using covariance table: {RR_STAGE2_COV_TABLE}")
print(f"Using race dimension : {RACE_DIM_TABLE}")

# ---------------------------------------------------------------------------
# 1) Load covariance table
# ---------------------------------------------------------------------------
cov_plot_df = (
    spark.table(RR_STAGE2_COV_TABLE)
    .filter(
        (F.col("scope_type") == "GLOBAL_ALL_VARIANTS") &
        (F.col("variant_id").isNull())
    )
    .select(
        "race_id_1",
        "race_id_2",
        "corr_value",
        "cov_value",
        "n_overlap_coaches",
        "maturity_min_games",
        "shrinkage_lambda",
        "eigen_floor",
    )
)

n_rows = cov_plot_df.count()
print(f"Loaded {n_rows:,} covariance rows")

if n_rows == 0:
    raise ValueError("No covariance rows found — run covariance cell first.")

cov_plot_pdf = cov_plot_df.toPandas()

# ---------------------------------------------------------------------------
# 2) Load race names
# ---------------------------------------------------------------------------
race_dim_pdf = (
    spark.table(RACE_DIM_TABLE)
    .select("race_id", "race_name")
    .filter(F.col("race_id") != 0)   # exclude reserved GLOBAL row
    .toPandas()
)

race_name_map = {
    int(row["race_id"]): str(row["race_name"])
    for _, row in race_dim_pdf.iterrows()
}

# ---------------------------------------------------------------------------
# 3) Build matrices
# ---------------------------------------------------------------------------
race_ids = sorted(set(cov_plot_pdf["race_id_1"]).union(set(cov_plot_pdf["race_id_2"])))
n_races = len(race_ids)

corr_df = (
    cov_plot_pdf
    .pivot(index="race_id_1", columns="race_id_2", values="corr_value")
    .reindex(index=race_ids, columns=race_ids)
)

overlap_df = (
    cov_plot_pdf
    .pivot(index="race_id_1", columns="race_id_2", values="n_overlap_coaches")
    .reindex(index=race_ids, columns=race_ids)
)

corr_mat = corr_df.to_numpy(dtype=float)
overlap_mat = overlap_df.to_numpy(dtype=float)

# Symmetrise
corr_mat = 0.5 * (corr_mat + corr_mat.T)
overlap_mat = 0.5 * (overlap_mat + overlap_mat.T)

race_labels = [race_name_map.get(rid, f"race_{rid}") for rid in race_ids]

print(f"Matrix size: {n_races} x {n_races}")

# ---------------------------------------------------------------------------
# 4) Correlation heatmap
# ---------------------------------------------------------------------------
fig, ax = plt.subplots(figsize=(14, 12))
im = ax.imshow(corr_mat, vmin=-1.0, vmax=1.0)

ax.set_title("Race Correlation Matrix")
ax.set_xticks(range(n_races))
ax.set_yticks(range(n_races))
ax.set_xticklabels(race_labels, rotation=90, fontsize=8)
ax.set_yticklabels(race_labels, fontsize=8)

cbar = plt.colorbar(im, ax=ax)
cbar.set_label("Correlation")

plt.tight_layout()
plt.show()

# ---------------------------------------------------------------------------
# 5) Overlap heatmap
# ---------------------------------------------------------------------------
fig, ax = plt.subplots(figsize=(14, 12))
im = ax.imshow(overlap_mat)

ax.set_title("Race Overlap (number of coaches)")
ax.set_xticks(range(n_races))
ax.set_yticks(range(n_races))
ax.set_xticklabels(race_labels, rotation=90, fontsize=8)
ax.set_yticklabels(race_labels, fontsize=8)

cbar = plt.colorbar(im, ax=ax)
cbar.set_label("n_overlap_coaches")

plt.tight_layout()
plt.show()

# ---------------------------------------------------------------------------
# 6) Top correlations with race names
# ---------------------------------------------------------------------------
pairs = []
for i, r1 in enumerate(race_ids):
    for j, r2 in enumerate(race_ids):
        if j <= i:
            continue
        pairs.append({
            "race_id_1": r1,
            "race_1": race_name_map.get(r1, f"race_{r1}"),
            "race_id_2": r2,
            "race_2": race_name_map.get(r2, f"race_{r2}"),
            "corr": float(corr_mat[i, j]),
            "overlap": int(overlap_mat[i, j]),
        })

pairs_df = pd.DataFrame(pairs)

print("\nTop positive correlations")
display(pairs_df.sort_values(["corr", "overlap"], ascending=[False, False]).head(10))

print("\nTop negative correlations")
display(pairs_df.sort_values(["corr", "overlap"], ascending=[True, False]).head(10))

# COMMAND ----------
# took 27 minutes to run
# DBTITLE 1,Stage 2 Correlated Race Rating Engine (main loop)

# =============================================================================
# COMPONENT: Stage 2 correlated race-rating engine
# =============================================================================
# PURPOSE      : Replay games sequentially and update:
#                  - scalar global skill g
#                  - full race-deviation vector d_vec
#                using the Stage 2 race covariance structure.
#
# OUTPUT       : in-memory list rr2_rows
# NEXT STEP    : create Spark DataFrame + write to RR_STAGE2_TARGET
# =============================================================================

import numpy as np
import math

# ---------------------------------------------------------------------------
# 1) Small linear-algebra helpers
# ---------------------------------------------------------------------------
def rr2_safe_symmetrize(mat: np.ndarray) -> np.ndarray:
    return 0.5 * (mat + mat.T)

def rr2_played_gain_vector(P_d: np.ndarray, race_id: int, h: float, S_innov: float) -> np.ndarray:
    """
    Kalman gain vector for the full race-deviation vector d_vec, when only the
    played race enters the observation directly.

    K_d = P_d H_d' / S
        = h * P_d[:, r] / S
    """
    idx = rr2_race_index(int(race_id))
    return (h / S_innov) * P_d[:, idx]

def rr2_update_Pd(P_d_pred: np.ndarray, race_id: int, h: float, S_innov: float) -> np.ndarray:
    """
    Covariance update for the full race-deviation vector.

    H_d = h * e_r'
    K_d = P_d H_d' / S = h * P_d[:, r] / S

    P_post = P_pred - K_d (H_d P_pred)
           = P_pred - K_d [h * P_pred[r, :]]
    """
    idx = rr2_race_index(int(race_id))
    K_d = rr2_played_gain_vector(P_d_pred, race_id, h, S_innov)
    HdP = h * P_d_pred[idx, :]   # row vector
    P_post = P_d_pred - np.outer(K_d, HdP)
    P_post = rr2_safe_symmetrize(P_post)

    # Eigen-floor repair for numerical stability
    eigvals, eigvecs = np.linalg.eigh(P_post)
    eigvals = np.maximum(eigvals, RR2_COV_EIGEN_FLOOR)
    P_post = eigvecs @ np.diag(eigvals) @ eigvecs.T
    P_post = rr2_safe_symmetrize(P_post)

    return P_post

# ---------------------------------------------------------------------------
# 2) Main loop
# ---------------------------------------------------------------------------
rr2_rows = []

for rr2_row in rr2_feed_rows:
    game_id = int(rr2_row["game_id"])
    game_index = int(rr2_row["game_index"])

    event_ts = rr2_row["event_timestamp"]
    game_date = rr2_row["game_date"]
    date_id = int(rr2_row["date_id"])

    tournament_id = int(rr2_row["tournament_id"]) if rr2_row["tournament_id"] is not None else None
    variant_id = int(rr2_row["variant_id"]) if rr2_row["variant_id"] is not None else None

    coach_home = int(rr2_row["home_coach_id"])
    coach_away = int(rr2_row["away_coach_id"])

    race_home = int(rr2_row["home_race_id"])
    race_away = int(rr2_row["away_race_id"])

    result_home = float(rr2_row["result_home"])
    result_away = float(rr2_row["result_away"])

    # --- prior counts (before this game) ---
    coach_game_number_home = rr2_game_counts.get(coach_home, 0) + 1
    coach_game_number_away = rr2_game_counts.get(coach_away, 0) + 1

    # --- get previous states ---
    state_home_prev = rr2_get_state(coach_home)
    state_away_prev = rr2_get_state(coach_away)

    # --- prediction step ---
    state_home_pred = rr2_predict_state(state_home_prev)
    state_away_pred = rr2_predict_state(state_away_prev)

    # --- played-race strengths before update ---
    theta_home_before, theta_away_before, p_home, h_home = rr2_observation_components(
        state_home_pred, race_home, state_away_pred, race_away
    )
    p_away = 1.0 - p_home
    h_away = h_home  # same magnitude in this symmetric logistic setup

    innovation_home = result_home - p_home
    innovation_away = result_away - p_away

    # --- variances before update ---
    g_var_home_before = float(state_home_pred["P_g"])
    g_var_away_before = float(state_away_pred["P_g"])

    d_var_home_before = rr2_played_race_variance(state_home_pred, race_home)
    d_var_away_before = rr2_played_race_variance(state_away_pred, race_away)

    theta_var_home_before = rr2_theta_variance_from_state(state_home_pred, race_home)
    theta_var_away_before = rr2_theta_variance_from_state(state_away_pred, race_away)

    # --- innovation variances ---
    S_home = max(
        h_home * h_home * (theta_var_home_before + theta_var_away_before) + RR2_SIGMA2_OBS,
        RR2_COV_EIGEN_FLOOR,
    )
    S_away = max(
        h_away * h_away * (theta_var_away_before + theta_var_home_before) + RR2_SIGMA2_OBS,
        RR2_COV_EIGEN_FLOOR,
    )

    # --- scalar gain for g ---
    K_g_home = h_home * g_var_home_before / S_home
    K_g_away = h_away * g_var_away_before / S_away

    # --- vector gain for d_vec ---
    K_d_home = rr2_played_gain_vector(state_home_pred["P_d"], race_home, h_home, S_home)
    K_d_away = rr2_played_gain_vector(state_away_pred["P_d"], race_away, h_away, S_away)

    # --- update means ---
    state_home_post = {
        "g": float(state_home_pred["g"] + K_g_home * innovation_home),
        "P_g": max((1.0 - K_g_home * h_home) * g_var_home_before, RR2_COV_EIGEN_FLOOR),
        "d_vec": state_home_pred["d_vec"] + K_d_home * innovation_home,
        "P_d": rr2_update_Pd(state_home_pred["P_d"], race_home, h_home, S_home),
    }

    state_away_post = {
        "g": float(state_away_pred["g"] + K_g_away * innovation_away),
        "P_g": max((1.0 - K_g_away * h_away) * g_var_away_before, RR2_COV_EIGEN_FLOOR),
        "d_vec": state_away_pred["d_vec"] + K_d_away * innovation_away,
        "P_d": rr2_update_Pd(state_away_pred["P_d"], race_away, h_away, S_away),
    }

    # --- played-race slices after update ---
    d_home_before = float(state_home_pred["d_vec"][rr2_race_index(race_home)])
    d_away_before = float(state_away_pred["d_vec"][rr2_race_index(race_away)])

    d_home_after = float(state_home_post["d_vec"][rr2_race_index(race_home)])
    d_away_after = float(state_away_post["d_vec"][rr2_race_index(race_away)])

    d_sigma_home_before = math.sqrt(d_var_home_before)
    d_sigma_away_before = math.sqrt(d_var_away_before)

    d_sigma_home_after = math.sqrt(rr2_played_race_variance(state_home_post, race_home))
    d_sigma_away_after = math.sqrt(rr2_played_race_variance(state_away_post, race_away))

    theta_home_after = rr2_theta_from_state(state_home_post, race_home)
    theta_away_after = rr2_theta_from_state(state_away_post, race_away)

    theta_sigma_home_before = math.sqrt(theta_var_home_before)
    theta_sigma_away_before = math.sqrt(theta_var_away_before)

    theta_sigma_home_after = math.sqrt(rr2_theta_variance_from_state(state_home_post, race_home))
    theta_sigma_away_after = math.sqrt(rr2_theta_variance_from_state(state_away_post, race_away))

    # --- append HOME row ---
    rr2_rows.append((
        game_id,
        coach_home,
        coach_game_number_home,

        event_ts,
        game_date,
        date_id,
        game_index,

        race_home,

        float(state_home_pred["g"]),
        math.sqrt(g_var_home_before),
        float(state_home_post["g"]),
        math.sqrt(state_home_post["P_g"]),

        d_home_before,
        d_sigma_home_before,
        d_home_after,
        d_sigma_home_after,

        theta_home_before,
        theta_sigma_home_before,
        theta_home_after,
        theta_sigma_home_after,

        coach_away,
        race_away,
        theta_away_before,
        theta_sigma_away_before,

        p_home,
        result_home,
        innovation_home,

        float(K_g_home),
        float(K_d_home[rr2_race_index(race_home)]),

        tournament_id,
        variant_id,
    ))

    # --- append AWAY row ---
    rr2_rows.append((
        game_id,
        coach_away,
        coach_game_number_away,

        event_ts,
        game_date,
        date_id,
        game_index,

        race_away,

        float(state_away_pred["g"]),
        math.sqrt(g_var_away_before),
        float(state_away_post["g"]),
        math.sqrt(state_away_post["P_g"]),

        d_away_before,
        d_sigma_away_before,
        d_away_after,
        d_sigma_away_after,

        theta_away_before,
        theta_sigma_away_before,
        theta_away_after,
        theta_sigma_away_after,

        coach_home,
        race_home,
        theta_home_before,
        theta_sigma_home_before,

        p_away,
        result_away,
        innovation_away,

        float(K_g_away),
        float(K_d_away[rr2_race_index(race_away)]),

        tournament_id,
        variant_id,
    ))

    # --- persist states ---
    rr2_set_state(coach_home, state_home_post)
    rr2_set_state(coach_away, state_away_post)

    rr2_increment_game_count(coach_home)
    rr2_increment_game_count(coach_away)

    rr2_increment_race_game_count(coach_home, race_home)
    rr2_increment_race_game_count(coach_away, race_away)

print(
    f"Stage 2 engine complete: generated {len(rr2_rows):,} rows "
    f"for {len(rr2_state):,} coaches across {len(rr2_feed_rows):,} games"
)

# COMMAND ----------
# DBTITLE 1,Write Stage 2 results

from pyspark.sql import functions as F, types as T

rr2_schema = T.StructType([
    T.StructField("game_id", T.IntegerType(), False),
    T.StructField("coach_id", T.IntegerType(), False),
    T.StructField("coach_game_number", T.IntegerType(), False),

    T.StructField("event_timestamp", T.TimestampType(), True),
    T.StructField("game_date", T.DateType(), True),
    T.StructField("date_id", T.IntegerType(), False),
    T.StructField("game_index", T.IntegerType(), False),

    T.StructField("race_id", T.IntegerType(), False),

    T.StructField("g_before", T.DoubleType(), False),
    T.StructField("g_sigma_before", T.DoubleType(), False),
    T.StructField("g_after", T.DoubleType(), False),
    T.StructField("g_sigma_after", T.DoubleType(), False),

    T.StructField("d_before", T.DoubleType(), False),
    T.StructField("d_sigma_before", T.DoubleType(), False),
    T.StructField("d_after", T.DoubleType(), False),
    T.StructField("d_sigma_after", T.DoubleType(), False),

    T.StructField("theta_before", T.DoubleType(), False),
    T.StructField("theta_sigma_before", T.DoubleType(), False),
    T.StructField("theta_after", T.DoubleType(), False),
    T.StructField("theta_sigma_after", T.DoubleType(), False),

    T.StructField("opponent_coach_id", T.IntegerType(), False),
    T.StructField("opponent_race_id", T.IntegerType(), False),
    T.StructField("opponent_theta_before", T.DoubleType(), False),
    T.StructField("opponent_theta_sigma_before", T.DoubleType(), False),

    T.StructField("score_expected", T.DoubleType(), False),
    T.StructField("result_numeric", T.DoubleType(), False),
    T.StructField("innovation", T.DoubleType(), False),

    T.StructField("kalman_gain_g", T.DoubleType(), True),
    T.StructField("kalman_gain_d_played", T.DoubleType(), True),

    T.StructField("tournament_id", T.IntegerType(), True),
    T.StructField("variant_id", T.IntegerType(), True),
])

rr2_df = (
    spark.createDataFrame(rr2_rows, rr2_schema)
    .withColumn("load_timestamp", F.current_timestamp())
)

(
    rr2_df
    .write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(RR_STAGE2_TARGET)
)

print(f"Wrote {rr2_df.count():,} rows to {RR_STAGE2_TARGET}")

# COMMAND ----------
# DBTITLE 1,Stage 2 sanity checks

df = spark.table(RR_STAGE2_TARGET)

print("Rows:", df.count())
print("Distinct coaches:", df.select("coach_id").distinct().count())

# Check theta = g + d
check = df.selectExpr(
    "abs(theta_before - (g_before + d_before)) as err_before",
    "abs(theta_after - (g_after + d_after)) as err_after"
)

display(check.selectExpr(
    "max(err_before) as max_err_before",
    "max(err_after) as max_err_after"
))

# Check sigmas
display(df.selectExpr(
    "min(theta_sigma_after) as min_sigma",
    "max(theta_sigma_after) as max_sigma"
))

# COMMAND ----------
# DBTITLE 1,Spillover example (manual inspection)

display(
    spark.table(RR_STAGE2_TARGET)
    .filter(F.col("coach_id") == 9524)
    .orderBy("game_index")
    .select(
        "game_index",
        "race_id",
        "d_before",
        "d_after",
        "theta_before",
        "theta_after"
    )
    .limit(50)
)

# COMMAND ----------
# DBTITLE 1,Quick Stage1 vs Stage2 comparison

df1 = spark.table("naf_catalog.gold_fact.race_rating_history_fact") \
    .select("game_id", "coach_id", "theta_after") \
    .withColumnRenamed("theta_after", "theta_s1")

df2 = spark.table(RR_STAGE2_TARGET) \
    .select("game_id", "coach_id", "theta_after") \
    .withColumnRenamed("theta_after", "theta_s2")

df_join = df1.join(df2, ["game_id", "coach_id"])

display(df_join.selectExpr(
    "avg(abs(theta_s1 - theta_s2)) as avg_diff",
    "max(abs(theta_s1 - theta_s2)) as max_diff"
))
