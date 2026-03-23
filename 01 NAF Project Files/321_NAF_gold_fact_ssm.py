# Databricks notebook source
# MAGIC %md
# MAGIC # 321 — State-Space Model (SSM) Rating Engine
# MAGIC
# MAGIC **Layer:** GOLD_FACT
# MAGIC **Pipeline position:** Runs after 320 (Elo engine)
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC Computes latent skill estimates with uncertainty for each coach using an
# MAGIC **Extended Kalman Filter (EKF)** on an AR(1) mean-reverting state-space model.
# MAGIC
# MAGIC Unlike the Elo engine (which produces a point estimate with no uncertainty),
# MAGIC the SSM produces `(mu, sigma)` — a skill estimate and its standard deviation —
# MAGIC for every game in a coach's history.
# MAGIC
# MAGIC ## Model
# MAGIC
# MAGIC **State equation (skill evolution between games):**
# MAGIC ```
# MAGIC θ_t = μ_global + φ × (θ_{t-1} − μ_global) + η_t     where η_t ~ N(0, σ²_process)
# MAGIC ```
# MAGIC
# MAGIC **Observation equation (game outcome):**
# MAGIC ```
# MAGIC P(win) = 1 / (1 + 10^((θ_opp − θ_self) / ELO_SCALE))
# MAGIC ```
# MAGIC
# MAGIC The observation is non-Gaussian (Bernoulli/ternary W/D/L), so we use the
# MAGIC Extended Kalman Filter to linearise the logistic observation model.
# MAGIC
# MAGIC ## Key Design Choices
# MAGIC
# MAGIC - **Elo-scale compatible:** Initial rating = 150, logistic scale = 150 (same as Elo engine)
# MAGIC - **Game-indexed, not time-indexed:** Process evolves per game, not per calendar day
# MAGIC - **Very slow mean-reversion:** φ ≈ 0.995 — nearly invisible over 50–100 games
# MAGIC - **Flat observation noise:** No tournament-level modulation — opponent skill/uncertainty
# MAGIC   determines informativeness naturally
# MAGIC - **Opponent uncertainty propagation:** The opponent's uncertainty (P_opp) is added to
# MAGIC   the effective observation variance, making wins against poorly-estimated opponents
# MAGIC   less informative
# MAGIC - **Draws modelled as 0.5:** Same as Elo engine — result_numeric ∈ {0.0, 0.5, 1.0}
# MAGIC
# MAGIC ## Dependencies
# MAGIC
# MAGIC - `naf_catalog.gold_fact.game_feed_for_ratings_fact` (same input as Elo engine)
# MAGIC - `naf_catalog.gold_dim.analytical_config` (for elo_initial_rating, elo_scale)
# MAGIC
# MAGIC ## Output
# MAGIC
# MAGIC - `naf_catalog.gold_fact.ssm_rating_history_fact` — 1 row per (game_id, coach_id)
# MAGIC   for GLOBAL scope only. Contains mu/sigma before and after each game.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS naf_catalog.gold_fact;

# COMMAND ----------

# MAGIC %sql -- TABLE: naf_catalog.gold_fact.ssm_rating_history_fact
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : SSM (State-Space Model) rating history per game, GLOBAL scope.
# MAGIC --                Produces latent skill estimate (mu) and uncertainty (sigma)
# MAGIC --                for each coach at each game using an Extended Kalman Filter.
# MAGIC -- LAYER        : GOLD_FACT
# MAGIC -- GRAIN        : 1 row per (game_id, coach_id) — GLOBAL scope only
# MAGIC -- PRIMARY KEY  : (game_id, coach_id)
# MAGIC -- FOREIGN KEYS : game_id          → naf_catalog.gold_fact.games_fact
# MAGIC --               coach_id          → naf_catalog.gold_dim.coach_dim
# MAGIC --               opponent_coach_id → naf_catalog.gold_dim.coach_dim
# MAGIC --               tournament_id     → naf_catalog.gold_dim.tournament_dim
# MAGIC --               date_id           → naf_catalog.gold_dim.date_dim
# MAGIC -- SOURCES      : naf_catalog.gold_fact.game_feed_for_ratings_fact + EKF computation
# MAGIC -- NOTES        : - GLOBAL scope only (no race-level SSM in v1).
# MAGIC --               - mu/sigma are on the Elo scale (initial = 150, scale = 150).
# MAGIC --               - coach_game_number is the 1-based game count for this coach.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS naf_catalog.gold_fact.ssm_rating_history_fact (
# MAGIC   game_id                  INT          NOT NULL,
# MAGIC   coach_id                 INT          NOT NULL,
# MAGIC
# MAGIC   event_timestamp          TIMESTAMP,
# MAGIC   game_date                DATE,
# MAGIC   date_id                  INT          NOT NULL,
# MAGIC   game_index               INT          NOT NULL,
# MAGIC   coach_game_number        INT          NOT NULL,
# MAGIC
# MAGIC   -- SSM state BEFORE observation update (post-prediction / post-AR(1))
# MAGIC   mu_before                DOUBLE       NOT NULL,
# MAGIC   sigma_before             DOUBLE       NOT NULL,
# MAGIC
# MAGIC   -- Opponent state at time of game (also post-prediction / post-AR(1))
# MAGIC   opponent_coach_id        INT          NOT NULL,
# MAGIC   opponent_mu_before       DOUBLE       NOT NULL,
# MAGIC   opponent_sigma_before    DOUBLE       NOT NULL,
# MAGIC
# MAGIC   -- Observation
# MAGIC   result_numeric           DOUBLE       NOT NULL,
# MAGIC   score_expected           DOUBLE       NOT NULL,
# MAGIC
# MAGIC   -- EKF update quantities
# MAGIC   kalman_gain              DOUBLE       NOT NULL,
# MAGIC   innovation               DOUBLE       NOT NULL,
# MAGIC
# MAGIC   -- SSM state AFTER this game's update
# MAGIC   mu_after                 DOUBLE       NOT NULL,
# MAGIC   sigma_after              DOUBLE       NOT NULL,
# MAGIC
# MAGIC   -- Context columns (carried from feed)
# MAGIC   tournament_id            INT,
# MAGIC   variant_id               INT,
# MAGIC
# MAGIC   load_timestamp           TIMESTAMP    NOT NULL
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# COMPONENT: SSM Rating Engine (Extended Kalman Filter)
# =============================================================================
# PURPOSE      : Compute SSM ratings for all coaches, GLOBAL scope.
#                Iterates over games in game_index order (same as Elo engine).
#                For each game, updates both coaches' (mu, P) state using EKF.
# INPUT        : naf_catalog.gold_fact.game_feed_for_ratings_fact
# OUTPUT       : naf_catalog.gold_fact.ssm_rating_history_fact
# GRAIN / PK   : 1 row per (game_id, coach_id)
# =============================================================================

import math
from pyspark.sql import functions as F, types as T

# ---------------------------------------------------------------------------
# 1) Load analytical parameters from central config
# ---------------------------------------------------------------------------
_cfg = spark.table("naf_catalog.gold_dim.analytical_config").first()
INITIAL_RATING = float(_cfg["elo_initial_rating"])   # 150.0
ELO_SCALE      = float(_cfg["elo_scale"])            # 150.0

# ---------------------------------------------------------------------------
# 2) SSM hyperparameters
# ---------------------------------------------------------------------------
# AR(1) mean-reversion target (same as Elo initial rating)
MU_GLOBAL = INITIAL_RATING  # 150.0

# Mean-reversion coefficient: very slow, ~0.5% pull per game
PHI = 0.995

# Prior uncertainty for new coaches: SD = 50 Elo points → P₀ = 2500
PRIOR_SIGMA = 50.0
PRIOR_P = PRIOR_SIGMA ** 2

# Process noise variance: how much true skill can change per game.
# Tuned so that sqrt(P_predict - P_prior) ≈ 1 Elo point after one game
# of "no observation" (i.e., uncertainty grows ~1 pt per game of inactivity).
# P_predict = φ² × P + σ²_process
# For a well-estimated coach (P ≈ 25, i.e. σ ≈ 5):
#   sqrt(φ² × 25 + σ²_process) ≈ sqrt(25 + σ²_process) ≈ 6
#   → σ²_process ≈ 11
# For a new coach (P = 2500): σ²_process is negligible.
SIGMA2_PROCESS = 11.0

# Observation noise baseline: represents inherent game-level randomness
# (dice, matchup variance, etc.) independent of skill. On the logistic
# derivative scale, this controls how much a single game can move the
# estimate. Higher = more conservative updates.
# Tuned to produce Kalman gains of ~0.05-0.15 for well-estimated coaches
# (comparable to Elo K-factors of 8-24 on a 150-scale).
SIGMA2_OBS = 1.0

# Logistic scaling: ln(10) / ELO_SCALE — used in the EKF linearisation
LN10_OVER_SCALE = math.log(10.0) / ELO_SCALE

# ---------------------------------------------------------------------------
# 3) Load the game feed in correct order (same as Elo engine)
# ---------------------------------------------------------------------------
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
        "result_home",
        "result_away",
    )
)

feed_rows = feed_df.collect()
print(f"SSM engine: processing {len(feed_rows)} games")

# ---------------------------------------------------------------------------
# 4) SSM state containers
# ---------------------------------------------------------------------------
# Each coach's state: (mu, P) where mu = skill estimate, P = variance
ssm_state = {}       # key: coach_id → (mu, P)
game_counts = {}     # key: coach_id → int (1-based game counter)

def get_state(coach_id):
    """Get current (mu, P) for a coach, initialising if new."""
    return ssm_state.get(coach_id, (MU_GLOBAL, PRIOR_P))

def set_state(coach_id, mu, P):
    """Update a coach's state."""
    ssm_state[coach_id] = (mu, P)

def increment_game_count(coach_id):
    """Increment and return the coach's game number."""
    game_counts[coach_id] = game_counts.get(coach_id, 0) + 1
    return game_counts[coach_id]

def win_probability(theta_self, theta_opp):
    """Logistic win probability (same formula as Elo engine)."""
    return 1.0 / (1.0 + 10.0 ** ((theta_opp - theta_self) / ELO_SCALE))

# ---------------------------------------------------------------------------
# 5) EKF update function
# ---------------------------------------------------------------------------
def ekf_predict(mu, P):
    """
    EKF prediction step: apply AR(1) mean-reversion.

    Args:
        mu: Current skill estimate (posterior from last game).
        P:  Current skill variance (posterior from last game).

    Returns:
        (mu_predict, P_predict) — predicted state before observing this game.
    """
    mu_predict = MU_GLOBAL + PHI * (mu - MU_GLOBAL)
    P_predict = PHI * PHI * P + SIGMA2_PROCESS
    return (mu_predict, P_predict)


def ekf_observe(mu_pred, P_pred, mu_opp_pred, P_opp_pred, result):
    """
    EKF observation update using the logistic observation model.

    Both self and opponent use their PREDICTED (post-AR(1)) states.
    Opponent uncertainty is propagated into the effective observation
    variance, making games against poorly-estimated opponents less
    informative (design choice per Skill_Estimation_Plan.md).

    Args:
        mu_pred:      Coach's predicted skill estimate.
        P_pred:       Coach's predicted skill variance.
        mu_opp_pred:  Opponent's predicted skill estimate.
        P_opp_pred:   Opponent's predicted skill variance.
        result:       Game result (1.0 = win, 0.5 = draw, 0.0 = loss).

    Returns:
        (mu_post, P_post, K, innovation, p_win)
    """
    # --- Observation model linearisation ---
    p_win = win_probability(mu_pred, mu_opp_pred)

    # Jacobian of logistic: dP(win)/d(theta_self)
    H = p_win * (1.0 - p_win) * LN10_OVER_SCALE

    # Innovation variance:
    # H² × P_pred     = self uncertainty propagated through observation model
    # H² × P_opp_pred = opponent uncertainty (makes uncertain opponents
    #                    less informative — replaces tournament-level K)
    # SIGMA2_OBS       = irreducible game noise (dice, matchups, etc.)
    S = H * H * P_pred + H * H * P_opp_pred + SIGMA2_OBS

    # Guard against degenerate cases (S ≈ 0 when p_win ≈ 0 or 1)
    if S < 1e-12:
        return (mu_pred, P_pred, 0.0, result - p_win, p_win)

    # --- EKF update ---
    K = H * P_pred / S             # Kalman gain
    innovation = result - p_win    # Surprise

    mu_post = mu_pred + K * innovation
    P_post = (1.0 - K * H) * P_pred

    # Ensure P doesn't go negative (numerical safety)
    P_post = max(P_post, 1e-6)

    return (mu_post, P_post, K, innovation, p_win)

# ---------------------------------------------------------------------------
# 6) Iterate over games and compute SSM updates
# ---------------------------------------------------------------------------
rows = []

for row in feed_rows:
    game_id = int(row["game_id"])
    game_index = int(row["game_index"])

    event_ts = row["event_timestamp"]
    game_date = row["game_date"]
    date_id = int(row["date_id"])

    coach_home = int(row["home_coach_id"])
    coach_away = int(row["away_coach_id"])

    result_home = float(row["result_home"])
    result_away = float(row["result_away"])

    tournament_id = int(row["tournament_id"]) if row["tournament_id"] is not None else None
    variant_id = int(row["variant_id"]) if row["variant_id"] is not None else None

    # Get current (posterior) states from previous game
    mu_h, P_h = get_state(coach_home)
    mu_a, P_a = get_state(coach_away)

    # Prediction step for BOTH coaches (AR(1) mean-reversion)
    mu_h_pred, P_h_pred = ekf_predict(mu_h, P_h)
    mu_a_pred, P_a_pred = ekf_predict(mu_a, P_a)

    # Observation update: each coach conditions on opponent's PREDICTED state
    (mu_h_post, P_h_post, K_h, innov_h, p_win_h) = ekf_observe(
        mu_h_pred, P_h_pred, mu_a_pred, P_a_pred, result_home
    )
    (mu_a_post, P_a_post, K_a, innov_a, p_win_a) = ekf_observe(
        mu_a_pred, P_a_pred, mu_h_pred, P_h_pred, result_away
    )

    # Game counts
    gn_h = increment_game_count(coach_home)
    gn_a = increment_game_count(coach_away)

    # Record rows.
    # "before" = post-prediction (after AR(1), before observation update).
    # Both self and opponent use their predicted states — symmetric.
    rows.append((
        game_id, coach_home,
        event_ts, game_date, date_id, game_index, gn_h,
        mu_h_pred, math.sqrt(P_h_pred),               # self: post-prediction
        coach_away, mu_a_pred, math.sqrt(P_a_pred),    # opp: post-prediction
        result_home, p_win_h,
        K_h, innov_h,
        mu_h_post, math.sqrt(P_h_post),                # self: posterior
        tournament_id, variant_id,
    ))
    rows.append((
        game_id, coach_away,
        event_ts, game_date, date_id, game_index, gn_a,
        mu_a_pred, math.sqrt(P_a_pred),                 # self: post-prediction
        coach_home, mu_h_pred, math.sqrt(P_h_pred),     # opp: post-prediction
        result_away, p_win_a,
        K_a, innov_a,
        mu_a_post, math.sqrt(P_a_post),                 # self: posterior
        tournament_id, variant_id,
    ))

    # Update states with posterior
    set_state(coach_home, mu_h_post, P_h_post)
    set_state(coach_away, mu_a_post, P_a_post)

print(f"SSM engine: generated {len(rows)} rating rows for {len(ssm_state)} coaches")

# ---------------------------------------------------------------------------
# 7) Spark schema definition
# ---------------------------------------------------------------------------
schema = T.StructType([
    T.StructField("game_id",               T.IntegerType(),   False),
    T.StructField("coach_id",              T.IntegerType(),   False),

    T.StructField("event_timestamp",       T.TimestampType(), True),
    T.StructField("game_date",             T.DateType(),      True),
    T.StructField("date_id",               T.IntegerType(),   False),
    T.StructField("game_index",            T.IntegerType(),   False),
    T.StructField("coach_game_number",     T.IntegerType(),   False),

    T.StructField("mu_before",             T.DoubleType(),    False),
    T.StructField("sigma_before",          T.DoubleType(),    False),

    T.StructField("opponent_coach_id",     T.IntegerType(),   False),
    T.StructField("opponent_mu_before",    T.DoubleType(),    False),
    T.StructField("opponent_sigma_before", T.DoubleType(),    False),

    T.StructField("result_numeric",        T.DoubleType(),    False),
    T.StructField("score_expected",        T.DoubleType(),    False),

    T.StructField("kalman_gain",           T.DoubleType(),    False),
    T.StructField("innovation",            T.DoubleType(),    False),

    T.StructField("mu_after",              T.DoubleType(),    False),
    T.StructField("sigma_after",           T.DoubleType(),    False),

    T.StructField("tournament_id",         T.IntegerType(),   True),
    T.StructField("variant_id",            T.IntegerType(),   True),
])

# ---------------------------------------------------------------------------
# 8) Create DataFrame and write
# ---------------------------------------------------------------------------
hist_df = (
    spark.createDataFrame(rows, schema)
    .withColumn("load_timestamp", F.current_timestamp())
)

target = "naf_catalog.gold_fact.ssm_rating_history_fact"

cols = [
    "game_id", "coach_id",
    "event_timestamp", "game_date", "date_id", "game_index", "coach_game_number",
    "mu_before", "sigma_before",
    "opponent_coach_id", "opponent_mu_before", "opponent_sigma_before",
    "result_numeric", "score_expected",
    "kalman_gain", "innovation",
    "mu_after", "sigma_after",
    "tournament_id", "variant_id",
    "load_timestamp",
]

(
    hist_df.select(*cols)
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target)
)

print(f"SSM engine: wrote {hist_df.count()} rows to {target}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quick sanity check: sample output for a few coaches
# MAGIC SELECT
# MAGIC   coach_id,
# MAGIC   COUNT(*)                          AS games,
# MAGIC   ROUND(MIN(mu_after), 1)           AS min_mu,
# MAGIC   ROUND(MAX(mu_after), 1)           AS max_mu,
# MAGIC   ROUND(AVG(mu_after), 1)           AS avg_mu,
# MAGIC   ROUND(MIN(sigma_after), 2)        AS min_sigma,
# MAGIC   ROUND(MAX(sigma_after), 2)        AS max_sigma,
# MAGIC   ROUND(AVG(kalman_gain), 4)        AS avg_kalman_gain
# MAGIC FROM naf_catalog.gold_fact.ssm_rating_history_fact
# MAGIC GROUP BY coach_id
# MAGIC HAVING COUNT(*) >= 100
# MAGIC ORDER BY max_mu DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sanity check: first 20 games for a specific coach (highest game count)
# MAGIC WITH top_coach AS (
# MAGIC   SELECT coach_id
# MAGIC   FROM naf_catalog.gold_fact.ssm_rating_history_fact
# MAGIC   GROUP BY coach_id
# MAGIC   ORDER BY COUNT(*) DESC
# MAGIC   LIMIT 1
# MAGIC )
# MAGIC SELECT
# MAGIC   s.coach_game_number,
# MAGIC   ROUND(s.mu_before, 2)          AS mu_before,
# MAGIC   ROUND(s.sigma_before, 2)       AS sigma_before,
# MAGIC   s.result_numeric,
# MAGIC   ROUND(s.score_expected, 4)     AS p_win,
# MAGIC   ROUND(s.kalman_gain, 4)        AS K,
# MAGIC   ROUND(s.innovation, 4)         AS innovation,
# MAGIC   ROUND(s.mu_after, 2)           AS mu_after,
# MAGIC   ROUND(s.sigma_after, 2)        AS sigma_after,
# MAGIC   ROUND(s.opponent_mu_before, 2) AS opp_mu,
# MAGIC   ROUND(s.opponent_sigma_before, 2) AS opp_sigma
# MAGIC FROM naf_catalog.gold_fact.ssm_rating_history_fact s
# MAGIC JOIN top_coach tc ON s.coach_id = tc.coach_id
# MAGIC WHERE s.coach_game_number <= 20
# MAGIC ORDER BY s.coach_game_number;
