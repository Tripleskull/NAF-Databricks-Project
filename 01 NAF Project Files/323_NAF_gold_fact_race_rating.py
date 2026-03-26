# Databricks notebook source
# MAGIC %md
# MAGIC # 323 — Race-Aware Rating Engine
# MAGIC
# MAGIC **Layer:** GOLD_FACT (production)
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
# =============================================================================
# COMPONENT: Load analytical parameters and game feed
# =============================================================================

import math
import datetime as dt
from pyspark.sql import functions as F, types as T

_cfg = spark.table("naf_catalog.gold_dim.analytical_config").first()

# Shared constants
INITIAL_RATING = float(_cfg["elo_initial_rating"])    # 150.0
ELO_SCALE      = float(_cfg["elo_scale"])             # 150.0
LN10_OVER_SCALE = math.log(10.0) / ELO_SCALE

# Race-rating hyperparameters (from analytical_config)
RR_PRIOR_SIGMA_G = float(_cfg["rr_prior_sigma_g"])    # global prior SD
RR_PRIOR_SIGMA_D = float(_cfg["rr_prior_sigma_d"])    # race-deviation prior SD
RR_SIGMA2_OBS    = float(_cfg["rr_sigma2_obs"])       # observation noise
RR_Q_GLOBAL      = float(_cfg["rr_q_global"])         # per-game process noise for g
RR_Q_RACE        = float(_cfg["rr_q_race"])           # per-game process noise for d

print(f"Config loaded — Race rating: prior_σ_g={RR_PRIOR_SIGMA_G}  "
      f"prior_σ_d={RR_PRIOR_SIGMA_D}  σ²_obs={RR_SIGMA2_OBS}  "
      f"q_global={RR_Q_GLOBAL}  q_race={RR_Q_RACE}")

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
