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
# MAGIC **Extended Kalman Filter (EKF)** on a state-space model.
# MAGIC
# MAGIC Unlike the Elo engine (which produces a point estimate with no uncertainty),
# MAGIC the SSM produces `(mu, sigma)` — a skill estimate and its standard deviation —
# MAGIC for every game in a coach's history.
# MAGIC
# MAGIC ## Models
# MAGIC
# MAGIC ### SSM v1 (game-indexed random walk)
# MAGIC
# MAGIC **State equation:** `θ_t = θ_{t-1} + η_t` where `η_t ~ N(0, σ²_process)`
# MAGIC (pure random walk, no mean reversion — φ was tuned to 1.0)
# MAGIC
# MAGIC **Observation equation:** `P(win) = 1 / (1 + 10^((θ_opp − θ_self) / ELO_SCALE))`
# MAGIC
# MAGIC Process noise is constant per game. Opponent uncertainty propagated into S.
# MAGIC Output: `ssm_rating_history_fact`.
# MAGIC
# MAGIC ### SSM v2 (time-aware + adaptive volatility)
# MAGIC
# MAGIC **State equation:** `θ_t = θ_{t-1} + η_t` (no mean reversion)
# MAGIC `P_pred = P_prev + q_time × √(min(Δt, 180)) + q_game + volatility`
# MAGIC
# MAGIC **Volatility:** EWMA of squared innovations drives coach-level adaptive noise.
# MAGIC `shock_ewma = decay × prev + (1−decay) × innovation²`
# MAGIC `volatility = clip(v_base + v_scale × shock_ewma, v_min, v_max)`
# MAGIC
# MAGIC **Observation equation:** Same logistic model as v1.
# MAGIC Output: `ssm2_rating_history_fact`.
# MAGIC
# MAGIC See `00 NAF Project Design/ssm_model_outline_v2_with_suggestions.md` for full v2 spec.
# MAGIC
# MAGIC ## Key Design Choices
# MAGIC
# MAGIC - **Elo-scale compatible:** Initial rating = 150, logistic scale = 150 (same as Elo engine)
# MAGIC - **No mean reversion:** Both v1 and v2 use φ=1.0 (random walk) since we aim to
# MAGIC   track Elo which itself has no mean reversion
# MAGIC - **Opponent uncertainty propagation:** The opponent's P_opp is added to
# MAGIC   the innovation variance S, making games against poorly-estimated opponents
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
# MAGIC - `naf_catalog.gold_fact.ssm_rating_history_fact` — v1, 1 row per (game_id, coach_id)
# MAGIC - `naf_catalog.gold_fact.ssm2_rating_history_fact` — v2, 1 row per (game_id, coach_id)
# MAGIC   Both GLOBAL scope only. Contain mu/sigma before and after each game.

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
PHI = 1.000 # 0.995

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
SIGMA2_PROCESS = 2.0

# Observation noise baseline: represents inherent game-level randomness
# (dice, matchup variance, etc.) independent of skill. On the logistic
# derivative scale, this controls how much a single game can move the
# estimate. Higher = more conservative updates.
# Tuned to produce Kalman gains of ~0.05-0.15 for well-estimated coaches
# (comparable to Elo K-factors of 8-24 on a 150-scale).
SIGMA2_OBS = 0.02

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

# COMMAND ----------

# DBTITLE 1,SSM Validation Tests & Summary Statistics
# MAGIC %sql
# MAGIC -- =======================================================================
# MAGIC -- SSM VALIDATION TESTS
# MAGIC -- Each row is a named test with pass/fail. fail_rows = 0 means pass.
# MAGIC -- =======================================================================
# MAGIC
# MAGIC -- 1. Row count: every (game_id, coach_id) pair in rating_history_fact
# MAGIC --    (GLOBAL scope) should have a matching SSM row.
# MAGIC SELECT 'ssm_vs_elo_row_count_match' AS check_name,
# MAGIC   ABS(
# MAGIC     (SELECT COUNT(*) FROM naf_catalog.gold_fact.ssm_rating_history_fact)
# MAGIC     - (SELECT COUNT(*) FROM naf_catalog.gold_fact.rating_history_fact WHERE scope = 'GLOBAL')
# MAGIC   ) AS fail_rows
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 2. No NULL mu/sigma in output
# MAGIC SELECT 'ssm_no_null_mu_sigma' AS check_name,
# MAGIC   COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_fact.ssm_rating_history_fact
# MAGIC WHERE mu_after IS NULL OR sigma_after IS NULL
# MAGIC   OR mu_before IS NULL OR sigma_before IS NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 3. sigma_after must be positive and finite
# MAGIC SELECT 'ssm_sigma_after_positive' AS check_name,
# MAGIC   COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_fact.ssm_rating_history_fact
# MAGIC WHERE sigma_after <= 0 OR sigma_after > 200 OR isnan(sigma_after)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 4. sigma_after < sigma_before (observation should reduce uncertainty)
# MAGIC --    Allow small tolerance for edge cases (degenerate S).
# MAGIC SELECT 'ssm_sigma_decreases_on_update' AS check_name,
# MAGIC   COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_fact.ssm_rating_history_fact
# MAGIC WHERE sigma_after > sigma_before + 0.01
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 5. Kalman gain in reasonable range [0, 1]
# MAGIC SELECT 'ssm_kalman_gain_in_0_1' AS check_name,
# MAGIC   COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_fact.ssm_rating_history_fact
# MAGIC WHERE kalman_gain < -0.001 OR kalman_gain > 1.001
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 6. mu_after should be in a sane range [50, 350] for coaches with 10+ games
# MAGIC SELECT 'ssm_mu_after_sane_range_10plus' AS check_name,
# MAGIC   COUNT(*) AS fail_rows
# MAGIC FROM (
# MAGIC   SELECT coach_id, mu_after, coach_game_number
# MAGIC   FROM naf_catalog.gold_fact.ssm_rating_history_fact
# MAGIC   WHERE coach_game_number >= 10
# MAGIC     AND (mu_after < 50 OR mu_after > 350)
# MAGIC )
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 7. First game for each coach should have sigma_before ≈ prior (50)
# MAGIC SELECT 'ssm_first_game_prior_sigma' AS check_name,
# MAGIC   COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_fact.ssm_rating_history_fact
# MAGIC WHERE coach_game_number = 1
# MAGIC   AND ABS(sigma_before - 50.0) > 1.0
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 8. PK uniqueness: (game_id, coach_id) must be unique
# MAGIC SELECT 'ssm_pk_unique' AS check_name,
# MAGIC   COUNT(*) AS fail_rows
# MAGIC FROM (
# MAGIC   SELECT game_id, coach_id
# MAGIC   FROM naf_catalog.gold_fact.ssm_rating_history_fact
# MAGIC   GROUP BY game_id, coach_id
# MAGIC   HAVING COUNT(*) > 1
# MAGIC )
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 9. coach_game_number sequential: max should equal count per coach
# MAGIC SELECT 'ssm_game_number_sequential' AS check_name,
# MAGIC   COUNT(*) AS fail_rows
# MAGIC FROM (
# MAGIC   SELECT coach_id, COUNT(*) AS cnt, MAX(coach_game_number) AS max_gn
# MAGIC   FROM naf_catalog.gold_fact.ssm_rating_history_fact
# MAGIC   GROUP BY coach_id
# MAGIC   HAVING cnt <> max_gn
# MAGIC )
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 10. score_expected (p_win) must be in [0, 1]
# MAGIC SELECT 'ssm_p_win_in_0_1' AS check_name,
# MAGIC   COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_fact.ssm_rating_history_fact
# MAGIC WHERE score_expected < -0.001 OR score_expected > 1.001;

# COMMAND ----------

# DBTITLE 1,SSM Summary Statistics
# MAGIC %sql
# MAGIC -- =======================================================================
# MAGIC -- SUMMARY STATISTICS: Overall SSM output characteristics
# MAGIC -- =======================================================================
# MAGIC SELECT
# MAGIC   COUNT(DISTINCT coach_id)                              AS total_coaches,
# MAGIC   COUNT(*)                                              AS total_rows,
# MAGIC   ROUND(COUNT(*) / COUNT(DISTINCT coach_id), 1)        AS avg_games_per_coach,
# MAGIC
# MAGIC   -- Final mu distribution (last game per coach)
# MAGIC   ROUND(AVG(final_mu), 1)                              AS avg_final_mu,
# MAGIC   ROUND(PERCENTILE_APPROX(final_mu, 0.50), 1)          AS median_final_mu,
# MAGIC   ROUND(STDDEV(final_mu), 1)                            AS sd_final_mu,
# MAGIC   ROUND(MIN(final_mu), 1)                               AS min_final_mu,
# MAGIC   ROUND(MAX(final_mu), 1)                               AS max_final_mu,
# MAGIC
# MAGIC   -- Final sigma distribution
# MAGIC   ROUND(AVG(final_sigma), 2)                            AS avg_final_sigma,
# MAGIC   ROUND(PERCENTILE_APPROX(final_sigma, 0.10), 2)       AS p10_final_sigma,
# MAGIC   ROUND(PERCENTILE_APPROX(final_sigma, 0.50), 2)       AS median_final_sigma,
# MAGIC   ROUND(PERCENTILE_APPROX(final_sigma, 0.90), 2)       AS p90_final_sigma,
# MAGIC
# MAGIC   -- Kalman gain distribution (all games)
# MAGIC   ROUND(AVG(avg_K), 4)                                  AS avg_kalman_gain,
# MAGIC   ROUND(MIN(avg_K), 4)                                  AS min_avg_K,
# MAGIC   ROUND(MAX(avg_K), 4)                                  AS max_avg_K
# MAGIC
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     coach_id,
# MAGIC     -- Last game state
# MAGIC     LAST_VALUE(mu_after) OVER (
# MAGIC       PARTITION BY coach_id ORDER BY coach_game_number
# MAGIC       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
# MAGIC     ) AS final_mu,
# MAGIC     LAST_VALUE(sigma_after) OVER (
# MAGIC       PARTITION BY coach_id ORDER BY coach_game_number
# MAGIC       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
# MAGIC     ) AS final_sigma,
# MAGIC     AVG(kalman_gain) OVER (PARTITION BY coach_id) AS avg_K
# MAGIC   FROM naf_catalog.gold_fact.ssm_rating_history_fact
# MAGIC );

# COMMAND ----------

# DBTITLE 1,SSM Summary: Sigma convergence by game count bucket
# MAGIC %sql
# MAGIC -- How quickly does uncertainty shrink as coaches play more games?
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN coach_game_number <= 5   THEN '01-05'
# MAGIC     WHEN coach_game_number <= 10  THEN '06-10'
# MAGIC     WHEN coach_game_number <= 25  THEN '11-25'
# MAGIC     WHEN coach_game_number <= 50  THEN '26-50'
# MAGIC     WHEN coach_game_number <= 100 THEN '51-100'
# MAGIC     ELSE '100+'
# MAGIC   END AS game_bucket,
# MAGIC   COUNT(*)                                   AS observations,
# MAGIC   ROUND(AVG(sigma_after), 2)                 AS avg_sigma,
# MAGIC   ROUND(PERCENTILE_APPROX(sigma_after, 0.10), 2) AS p10_sigma,
# MAGIC   ROUND(PERCENTILE_APPROX(sigma_after, 0.50), 2) AS median_sigma,
# MAGIC   ROUND(PERCENTILE_APPROX(sigma_after, 0.90), 2) AS p90_sigma,
# MAGIC   ROUND(AVG(kalman_gain), 4)                 AS avg_K,
# MAGIC   ROUND(AVG(ABS(innovation)), 4)             AS avg_abs_innovation
# MAGIC FROM naf_catalog.gold_fact.ssm_rating_history_fact
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1;

# COMMAND ----------

# DBTITLE 1,SSM vs Elo: correlation of final ratings
# MAGIC %sql
# MAGIC -- Compare SSM final mu against Elo current rating for coaches with 50+ games
# MAGIC WITH ssm_final AS (
# MAGIC   SELECT coach_id, mu_after AS ssm_mu, sigma_after AS ssm_sigma, coach_game_number AS total_games
# MAGIC   FROM (
# MAGIC     SELECT *, ROW_NUMBER() OVER (PARTITION BY coach_id ORDER BY coach_game_number DESC) AS rn
# MAGIC     FROM naf_catalog.gold_fact.ssm_rating_history_fact
# MAGIC   )
# MAGIC   WHERE rn = 1
# MAGIC ),
# MAGIC elo_final AS (
# MAGIC   SELECT coach_id, rating_after AS elo_rating
# MAGIC   FROM (
# MAGIC     SELECT *, ROW_NUMBER() OVER (PARTITION BY coach_id ORDER BY game_index DESC) AS rn
# MAGIC     FROM naf_catalog.gold_fact.rating_history_fact
# MAGIC     WHERE scope = 'GLOBAL'
# MAGIC   )
# MAGIC   WHERE rn = 1
# MAGIC )
# MAGIC SELECT
# MAGIC   COUNT(*)                                                AS coaches,
# MAGIC   ROUND(CORR(s.ssm_mu, e.elo_rating), 4)                 AS correlation,
# MAGIC   ROUND(AVG(s.ssm_mu - e.elo_rating), 2)                 AS avg_diff_ssm_minus_elo,
# MAGIC   ROUND(STDDEV(s.ssm_mu - e.elo_rating), 2)              AS sd_diff,
# MAGIC   ROUND(PERCENTILE_APPROX(s.ssm_mu - e.elo_rating, 0.05), 2) AS p05_diff,
# MAGIC   ROUND(PERCENTILE_APPROX(s.ssm_mu - e.elo_rating, 0.50), 2) AS median_diff,
# MAGIC   ROUND(PERCENTILE_APPROX(s.ssm_mu - e.elo_rating, 0.95), 2) AS p95_diff,
# MAGIC   ROUND(AVG(s.ssm_sigma), 2)                             AS avg_ssm_sigma
# MAGIC FROM ssm_final s
# MAGIC JOIN elo_final e ON s.coach_id = e.coach_id
# MAGIC WHERE s.total_games >= 50;

# COMMAND ----------

# DBTITLE 1,Plot: SSM rating trajectory with uncertainty band (top coach by games)
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

# --- Pick the coach with the most games ---
top_coach_row = spark.sql("""
    SELECT coach_id, COUNT(*) AS games
    FROM naf_catalog.gold_fact.ssm_rating_history_fact
    GROUP BY coach_id
    ORDER BY games DESC
    LIMIT 1
""").first()

coach_id = top_coach_row["coach_id"]
# coach_id = 9524   # uncomment to override with a specific coach
total_games = top_coach_row["games"]

# --- Load SSM trajectory ---
ssm_df = spark.sql(f"""
    SELECT coach_game_number, mu_after, sigma_after
    FROM naf_catalog.gold_fact.ssm_rating_history_fact
    WHERE coach_id = {coach_id}
    ORDER BY coach_game_number
""").toPandas()

# --- Load Elo trajectory for comparison ---
elo_df = spark.sql(f"""
    SELECT
      ROW_NUMBER() OVER (ORDER BY game_index, game_id) AS coach_game_number,
      rating_after AS elo_rating
    FROM naf_catalog.gold_fact.rating_history_fact
    WHERE coach_id = {coach_id}
      AND scope = 'GLOBAL'
    ORDER BY game_index, game_id
""").toPandas()

# --- Compute 50-game rolling median Elo ---
SSM1_ROLLING_MEDIAN_WINDOW = 50
if len(elo_df) > 0:
    elo_df["elo_rolling_median"] = (
        elo_df["elo_rating"]
        .rolling(window=SSM1_ROLLING_MEDIAN_WINDOW, min_periods=1)
        .median()
    )

# --- Load coach name ---
coach_name_row = spark.sql(f"""
    SELECT coach_name FROM naf_catalog.gold_dim.coach_dim WHERE coach_id = {coach_id}
""").first()
coach_name = coach_name_row["coach_name"] if coach_name_row else str(coach_id)

# --- Plot ---
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), sharex=True,
                                gridspec_kw={"height_ratios": [3, 1]})

game_num = ssm_df["coach_game_number"].values
mu = ssm_df["mu_after"].values
sigma = ssm_df["sigma_after"].values

# Top panel: rating + uncertainty band
ax1.fill_between(game_num, mu - 2 * sigma, mu + 2 * sigma,
                 alpha=0.2, color="steelblue", label="SSM v1 ± 2σ")
ax1.plot(game_num, mu, color="steelblue", linewidth=1.2, label="SSM v1 μ")

if len(elo_df) == len(ssm_df):
    ax1.plot(game_num, elo_df["elo_rating"].values,
             color="coral", linewidth=0.6, alpha=0.4, linestyle="--",
             label="Elo (raw)")
    ax1.plot(game_num, elo_df["elo_rolling_median"].values,
             color="coral", linewidth=1.2, alpha=0.9,
             label=f"Elo (rolling {SSM1_ROLLING_MEDIAN_WINDOW}-game median)")

ax1.axhline(y=150, color="gray", linestyle=":", linewidth=0.7, alpha=0.5,
            label="Initial (150)")

# Hyperparameter summary
hp_text = (
    f"φ={PHI}  σ²_proc={SIGMA2_PROCESS}  σ²_obs={SIGMA2_OBS}  "
    f"prior_σ={PRIOR_SIGMA}"
)
ax1.plot([], [], ' ', label=hp_text)

ax1.set_ylabel("Rating (Elo scale)")
ax1.set_title(f"SSM v1 Rating Trajectory — {coach_name} (coach {coach_id}, {total_games} games)")
ax1.legend(loc="lower right", fontsize=7)
ax1.grid(True, alpha=0.3)

# Bottom panel: uncertainty (sigma) over time
ax2.plot(game_num, sigma, color="steelblue", linewidth=1.0)
ax2.set_ylabel("σ (uncertainty)")
ax2.set_xlabel("Game number")
ax2.grid(True, alpha=0.3)
ax2.set_ylim(bottom=0)

plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Plot: Sigma distribution by experience tier (violin-style)
import matplotlib.pyplot as plt

# --- Bucket coaches by total games and get their final sigma ---
final_df = spark.sql("""
    WITH final AS (
      SELECT coach_id, mu_after, sigma_after, coach_game_number AS total_games
      FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY coach_id ORDER BY coach_game_number DESC) AS rn
        FROM naf_catalog.gold_fact.ssm_rating_history_fact
      ) WHERE rn = 1
    )
    SELECT
      CASE
        WHEN total_games <= 10  THEN '1-10'
        WHEN total_games <= 25  THEN '11-25'
        WHEN total_games <= 50  THEN '26-50'
        WHEN total_games <= 100 THEN '51-100'
        WHEN total_games <= 200 THEN '101-200'
        ELSE '200+'
      END AS tier,
      CASE
        WHEN total_games <= 10  THEN 1
        WHEN total_games <= 25  THEN 2
        WHEN total_games <= 50  THEN 3
        WHEN total_games <= 100 THEN 4
        WHEN total_games <= 200 THEN 5
        ELSE 6
      END AS tier_order,
      sigma_after
    FROM final
""").toPandas()

# Group data for box plot
tiers = final_df.sort_values("tier_order").groupby(["tier_order", "tier"], sort=True)
labels = []
data = []
for (order, tier), group in tiers:
    labels.append(f"{tier}\n(n={len(group)})")
    data.append(group["sigma_after"].values)

fig, ax = plt.subplots(figsize=(10, 5))
bp = ax.boxplot(data, labels=labels, patch_artist=True, showfliers=False,
                medianprops=dict(color="darkblue", linewidth=1.5))
for patch in bp["boxes"]:
    patch.set_facecolor("steelblue")
    patch.set_alpha(0.4)

ax.set_ylabel("Final σ (uncertainty)")
ax.set_xlabel("Total games played")
ax.set_title("SSM Uncertainty by Coach Experience")
ax.grid(True, axis="y", alpha=0.3)
plt.tight_layout()
plt.show()

# COMMAND ----------

# COMPONENT: SSM2 Rating Engine (time-aware + volatility-aware, no mean reversion)
# =============================================================================
# PURPOSE      : Compute GLOBAL SSM ratings with:
#                - no mean reversion
#                - time-aware uncertainty growth
#                - small baseline per-game process noise
#                - coach-level adaptive volatility
#                - expected-score treatment of draws (0.0 / 0.5 / 1.0)
# INPUT        : naf_catalog.gold_fact.game_feed_for_ratings_fact
# CONFIG       : naf_catalog.gold_dim.analytical_config (elo_initial_rating, elo_scale)
# OUTPUT       : naf_catalog.gold_fact.ssm2_rating_history_fact
# GRAIN / PK   : 1 row per (game_id, coach_id)
# NOTES        : Uses distinct SSM2_* names and a new output table to avoid conflicts.
# Tunes parameters: sigma2_obs = 0.1000   q_time     = 2.0000   q_game     = 0.0250   v_scale    = 24.0000
# =============================================================================

import math
import datetime as dt
from pyspark.sql import functions as F, types as T

# ---------------------------------------------------------------------------
# 1) Core config from existing singleton
# ---------------------------------------------------------------------------
_ssm2_cfg = spark.table("naf_catalog.gold_dim.analytical_config").first()
SSM2_INITIAL_RATING = float(_ssm2_cfg["elo_initial_rating"])
SSM2_ELO_SCALE      = float(_ssm2_cfg["elo_scale"])
SSM2_LN10_OVER_SCALE = math.log(10.0) / SSM2_ELO_SCALE

# ---------------------------------------------------------------------------
# 2) SSM2 hyperparameters (local for now; can later move into analytical_config)
# ---------------------------------------------------------------------------
# Prior
SSM2_PRIOR_SIGMA = 50.0
SSM2_PRIOR_P = SSM2_PRIOR_SIGMA ** 2

# Observation noise (same role as in current SSM)
SSM2_SIGMA2_OBS = 0.10      # tuned 2026-03-24 (was 0.05)

# Time-aware process variance:
#   P_pred = P_prev + q_time * sqrt(capped_days_since_prev_game) + q_game + volatility
SSM2_Q_TIME = 2.00          # variance added per sqrt(day) — tuned 2026-03-24 (was 1.50)
SSM2_Q_GAME = 0.025         # baseline per-game noise — tuned 2026-03-24 (was 0.25)
SSM2_MAX_DAYS = 180.0       # cap very long inactivity gaps
SSM2_MIN_P = 1e-6

# Volatility dynamics
# shock_ewma_post = decay * shock_ewma_prev + (1-decay) * innovation^2
# volatility_post = clip(v_base + v_scale * shock_ewma_post, v_min, v_max)
SSM2_V_BASE = 0.25
SSM2_V_SCALE = 24.0         # tuned 2026-03-24 (was 12.0)
SSM2_V_DECAY = 0.90
SSM2_V_MIN = 0.00
SSM2_V_MAX = 16.0

# ---------------------------------------------------------------------------
# 3) Load feed in deterministic order
# ---------------------------------------------------------------------------
ssm2_feed_df = (
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

ssm2_bad = ssm2_feed_df.filter(
    F.col("game_id").isNull()
    | F.col("game_index").isNull()
    | F.col("date_id").isNull()
    | F.col("home_coach_id").isNull()
    | F.col("away_coach_id").isNull()
    | (F.col("home_coach_id") == F.col("away_coach_id"))
    | ~F.col("result_home").isin([0.0, 0.5, 1.0])
    | ~F.col("result_away").isin([0.0, 0.5, 1.0])
)

if len(ssm2_bad.take(1)) > 0:
    raise ValueError("SSM2: invalid rows detected in rating feed.")

ssm2_feed_rows = ssm2_feed_df.collect()
print(f"SSM2 engine: processing {len(ssm2_feed_rows)} games")

# ---------------------------------------------------------------------------
# 4) State containers
# ---------------------------------------------------------------------------
# Per coach:
#   mu          : current skill estimate
#   P           : posterior variance
#   volatility  : adaptive extra process variance term
#   shock_ewma  : EWMA of squared innovations
#   last_dt     : datetime of previous game
ssm2_state = {}
ssm2_game_counts = {}

def ssm2_default_state():
    return {
        "mu": SSM2_INITIAL_RATING,
        "P": SSM2_PRIOR_P,
        "volatility": SSM2_V_BASE,
        "shock_ewma": 0.0,
        "last_dt": None,
    }

def ssm2_get_state(coach_id):
    return ssm2_state.get(coach_id, ssm2_default_state().copy())

def ssm2_set_state(coach_id, state_dict):
    ssm2_state[coach_id] = state_dict

def ssm2_increment_game_count(coach_id):
    ssm2_game_counts[coach_id] = ssm2_game_counts.get(coach_id, 0) + 1
    return ssm2_game_counts[coach_id]

def ssm2_win_probability(theta_self, theta_opp):
    return 1.0 / (1.0 + 10.0 ** ((theta_opp - theta_self) / SSM2_ELO_SCALE))

def ssm2_row_datetime(row):
    event_ts = row["event_timestamp"]
    game_date = row["game_date"]
    date_id = int(row["date_id"])

    if event_ts is not None:
        return event_ts.replace(tzinfo=None) if getattr(event_ts, "tzinfo", None) is not None else event_ts
    if game_date is not None:
        return dt.datetime.combine(game_date, dt.time(0, 0))
    # fallback from date_id = YYYYMMDD
    ds = str(date_id)
    return dt.datetime.strptime(ds, "%Y%m%d")

def ssm2_days_since(prev_dt, current_dt):
    if prev_dt is None:
        return 0.0
    delta_days = (current_dt - prev_dt).total_seconds() / 86400.0
    return max(delta_days, 0.0)

def ssm2_time_scale(days_since_prev):
    return math.sqrt(min(days_since_prev, SSM2_MAX_DAYS))

def ssm2_predict(state_dict, current_dt):
    days_since_prev = ssm2_days_since(state_dict["last_dt"], current_dt)
    time_scale = ssm2_time_scale(days_since_prev)
    process_variance_added = (
        SSM2_Q_TIME * time_scale
        + SSM2_Q_GAME
        + state_dict["volatility"]
    )

    mu_pred = state_dict["mu"]                 # no mean reversion
    P_pred = state_dict["P"] + process_variance_added

    return (
        mu_pred,
        P_pred,
        days_since_prev,
        time_scale,
        process_variance_added,
    )

def ssm2_observe(mu_pred, P_pred, mu_opp_pred, P_opp_pred, result):
    p_exp = ssm2_win_probability(mu_pred, mu_opp_pred)

    # EKF linearisation of expected score wrt own skill
    H = p_exp * (1.0 - p_exp) * SSM2_LN10_OVER_SCALE

    # Opponent uncertainty propagated into effective observation variance
    S = H * H * P_pred + H * H * P_opp_pred + SSM2_SIGMA2_OBS

    innovation = result - p_exp

    if S < 1e-12:
        return (mu_pred, max(P_pred, SSM2_MIN_P), 0.0, innovation, p_exp)

    K = H * P_pred / S
    mu_post = mu_pred + K * innovation
    P_post = max((1.0 - K * H) * P_pred, SSM2_MIN_P)

    return (mu_post, P_post, K, innovation, p_exp)

def ssm2_update_volatility(state_dict, innovation):
    shock_ewma_post = (
        SSM2_V_DECAY * state_dict["shock_ewma"]
        + (1.0 - SSM2_V_DECAY) * (innovation ** 2)
    )

    volatility_post = SSM2_V_BASE + SSM2_V_SCALE * shock_ewma_post
    volatility_post = max(SSM2_V_MIN, min(volatility_post, SSM2_V_MAX))

    return volatility_post, shock_ewma_post

# ---------------------------------------------------------------------------
# 5) Iterate sequentially over games
# ---------------------------------------------------------------------------
ssm2_rows = []

for ssm2_row in ssm2_feed_rows:
    game_id = int(ssm2_row["game_id"])
    game_index = int(ssm2_row["game_index"])

    event_ts = ssm2_row["event_timestamp"]
    game_date = ssm2_row["game_date"]
    date_id = int(ssm2_row["date_id"])

    tournament_id = int(ssm2_row["tournament_id"]) if ssm2_row["tournament_id"] is not None else None
    variant_id = int(ssm2_row["variant_id"]) if ssm2_row["variant_id"] is not None else None

    coach_home = int(ssm2_row["home_coach_id"])
    coach_away = int(ssm2_row["away_coach_id"])

    result_home = float(ssm2_row["result_home"])
    result_away = float(ssm2_row["result_away"])

    current_dt = ssm2_row_datetime(ssm2_row)

    state_h_prev = ssm2_get_state(coach_home)
    state_a_prev = ssm2_get_state(coach_away)

    # Predict independently for each coach using own elapsed time
    (
        mu_h_pred,
        P_h_pred,
        days_h,
        time_scale_h,
        q_added_h,
    ) = ssm2_predict(state_h_prev, current_dt)

    (
        mu_a_pred,
        P_a_pred,
        days_a,
        time_scale_a,
        q_added_a,
    ) = ssm2_predict(state_a_prev, current_dt)

    # Observe game from each side
    (
        mu_h_post,
        P_h_post,
        K_h,
        innov_h,
        p_exp_h,
    ) = ssm2_observe(mu_h_pred, P_h_pred, mu_a_pred, P_a_pred, result_home)

    (
        mu_a_post,
        P_a_post,
        K_a,
        innov_a,
        p_exp_a,
    ) = ssm2_observe(mu_a_pred, P_a_pred, mu_h_pred, P_h_pred, result_away)

    # Update volatility after observing surprise
    vol_h_post, shock_ewma_h_post = ssm2_update_volatility(state_h_prev, innov_h)
    vol_a_post, shock_ewma_a_post = ssm2_update_volatility(state_a_prev, innov_a)

    # Game counts
    gn_h = ssm2_increment_game_count(coach_home)
    gn_a = ssm2_increment_game_count(coach_away)

    # Record home perspective
    ssm2_rows.append((
        game_id, coach_home,
        event_ts, game_date, date_id, game_index, gn_h,
        mu_h_pred, math.sqrt(P_h_pred),
        coach_away, mu_a_pred, math.sqrt(P_a_pred),
        result_home, p_exp_h,
        K_h, innov_h,
        days_h, time_scale_h, q_added_h,
        state_h_prev["volatility"], vol_h_post, shock_ewma_h_post,
        mu_h_post, math.sqrt(P_h_post),
        tournament_id, variant_id,
    ))

    # Record away perspective
    ssm2_rows.append((
        game_id, coach_away,
        event_ts, game_date, date_id, game_index, gn_a,
        mu_a_pred, math.sqrt(P_a_pred),
        coach_home, mu_h_pred, math.sqrt(P_h_pred),
        result_away, p_exp_a,
        K_a, innov_a,
        days_a, time_scale_a, q_added_a,
        state_a_prev["volatility"], vol_a_post, shock_ewma_a_post,
        mu_a_post, math.sqrt(P_a_post),
        tournament_id, variant_id,
    ))

    # Persist posteriors
    ssm2_set_state(coach_home, {
        "mu": mu_h_post,
        "P": P_h_post,
        "volatility": vol_h_post,
        "shock_ewma": shock_ewma_h_post,
        "last_dt": current_dt,
    })

    ssm2_set_state(coach_away, {
        "mu": mu_a_post,
        "P": P_a_post,
        "volatility": vol_a_post,
        "shock_ewma": shock_ewma_a_post,
        "last_dt": current_dt,
    })

print(f"SSM2 engine: generated {len(ssm2_rows)} rows for {len(ssm2_state)} coaches")

# ---------------------------------------------------------------------------
# 6) Schema
# ---------------------------------------------------------------------------
ssm2_schema = T.StructType([
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

    T.StructField("days_since_prev_game",  T.DoubleType(),    False),
    T.StructField("time_scale_value",      T.DoubleType(),    False),
    T.StructField("process_variance_added",T.DoubleType(),    False),

    T.StructField("volatility_before",     T.DoubleType(),    False),
    T.StructField("volatility_after",      T.DoubleType(),    False),
    T.StructField("shock_ewma_after",      T.DoubleType(),    False),

    T.StructField("mu_after",              T.DoubleType(),    False),
    T.StructField("sigma_after",           T.DoubleType(),    False),

    T.StructField("tournament_id",         T.IntegerType(),   True),
    T.StructField("variant_id",            T.IntegerType(),   True),
])

# ---------------------------------------------------------------------------
# 7) Create DataFrame and write to a distinct target table
# ---------------------------------------------------------------------------
ssm2_hist_df = (
    spark.createDataFrame(ssm2_rows, ssm2_schema)
    .withColumn("load_timestamp", F.current_timestamp())
)

ssm2_target = "naf_catalog.gold_fact.ssm2_rating_history_fact"

ssm2_cols = [
    "game_id", "coach_id",
    "event_timestamp", "game_date", "date_id", "game_index", "coach_game_number",
    "mu_before", "sigma_before",
    "opponent_coach_id", "opponent_mu_before", "opponent_sigma_before",
    "result_numeric", "score_expected",
    "kalman_gain", "innovation",
    "days_since_prev_game", "time_scale_value", "process_variance_added",
    "volatility_before", "volatility_after", "shock_ewma_after",
    "mu_after", "sigma_after",
    "tournament_id", "variant_id",
    "load_timestamp",
]

(
    ssm2_hist_df.select(*ssm2_cols)
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(ssm2_target)
)

print(f"SSM2 engine: wrote {ssm2_hist_df.count()} rows to {ssm2_target}")

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np

# --- Pick coach ---
ssm2_top_coach_row = spark.sql("""
    SELECT coach_id, COUNT(*) AS games
    FROM naf_catalog.gold_fact.ssm2_rating_history_fact
    GROUP BY coach_id
    ORDER BY games DESC, coach_id
    LIMIT 1
""").first()

ssm2_coach_id = ssm2_top_coach_row["coach_id"]
ssm2_coach_id = 9524   # uncomment to override with a specific coach
ssm2_coach_id = 34738 
ssm2_coach_id = 35505

ssm2_total_games_row = spark.sql(f"""
    SELECT COUNT(*) AS games
    FROM naf_catalog.gold_fact.ssm2_rating_history_fact
    WHERE coach_id = {ssm2_coach_id}
""").first()
ssm2_total_games = ssm2_total_games_row["games"]

# --- Load SSM2 trajectory ---
ssm2_df = spark.sql(f"""
    SELECT
        coach_game_number,
        mu_after,
        sigma_after,
        volatility_after,
        days_since_prev_game
    FROM naf_catalog.gold_fact.ssm2_rating_history_fact
    WHERE coach_id = {ssm2_coach_id}
    ORDER BY coach_game_number
""").toPandas()

# --- Load Elo trajectory for comparison ---
elo_df = spark.sql(f"""
    SELECT
        ROW_NUMBER() OVER (ORDER BY game_index, game_id) AS coach_game_number,
        rating_after AS elo_rating
    FROM naf_catalog.gold_fact.rating_history_fact
    WHERE coach_id = {ssm2_coach_id}
      AND scope = 'GLOBAL'
    ORDER BY game_index, game_id
""").toPandas()

# --- Compute 50-game rolling median Elo (calibration target) ---
ROLLING_MEDIAN_WINDOW = 50
if len(elo_df) > 0:
    elo_df["elo_rolling_median"] = (
        elo_df["elo_rating"]
        .rolling(window=ROLLING_MEDIAN_WINDOW, min_periods=1)
        .median()
    )

# --- Load coach name ---
ssm2_coach_name_row = spark.sql(f"""
    SELECT coach_name
    FROM naf_catalog.gold_dim.coach_dim
    WHERE coach_id = {ssm2_coach_id}
""").first()

ssm2_coach_name = (
    ssm2_coach_name_row["coach_name"]
    if ssm2_coach_name_row is not None
    else str(ssm2_coach_id)
)

# --- Extract arrays ---
game_num = ssm2_df["coach_game_number"].values
mu = ssm2_df["mu_after"].values
sigma = ssm2_df["sigma_after"].values
volatility = ssm2_df["volatility_after"].values
days_since_prev = ssm2_df["days_since_prev_game"].values

# --- Plot ---
fig, (ax1, ax2, ax3, ax4) = plt.subplots(
    4, 1,
    figsize=(14, 11),
    sharex=True,
    gridspec_kw={"height_ratios": [3, 1, 1, 1]}
)

# Panel 1: rating + uncertainty band
ax1.fill_between(
    game_num,
    mu - 2 * sigma,
    mu + 2 * sigma,
    alpha=0.20,
    color="steelblue",
    label="SSM2 ± 2σ"
)
ax1.plot(
    game_num,
    mu,
    color="steelblue",
    linewidth=1.2,
    label="SSM2 μ"
)

if len(elo_df) == len(ssm2_df):
    ax1.plot(
        game_num,
        elo_df["elo_rating"].values,
        color="coral",
        linewidth=0.6,
        alpha=0.4,
        linestyle="--",
        label="Elo (raw)"
    )
    ax1.plot(
        game_num,
        elo_df["elo_rolling_median"].values,
        color="coral",
        linewidth=1.2,
        alpha=0.9,
        label=f"Elo (rolling {ROLLING_MEDIAN_WINDOW}-game median)"
    )

ax1.axhline(
    y=150,
    color="gray",
    linestyle=":",
    linewidth=0.7,
    alpha=0.6,
    label="Initial (150)"
)

ax1.set_ylabel("Rating")
ax1.set_title(
    f"SSM2 Rating Diagnostics — {ssm2_coach_name} "
    f"(coach {ssm2_coach_id}, {ssm2_total_games} games)"
)

# Hyperparameter summary in legend
hp_text = (
    f"σ²_obs={SSM2_SIGMA2_OBS}  q_time={SSM2_Q_TIME}  q_game={SSM2_Q_GAME}  "
    f"prior_σ={SSM2_PRIOR_SIGMA}\n"
    f"v_base={SSM2_V_BASE}  v_scale={SSM2_V_SCALE}  v_decay={SSM2_V_DECAY}  "
    f"v_min={SSM2_V_MIN}  v_max={SSM2_V_MAX}  max_days={SSM2_MAX_DAYS}"
)
ax1.plot([], [], ' ', label=hp_text)

ax1.legend(loc="lower right", fontsize=7)
ax1.grid(True, alpha=0.3)

# Panel 2: posterior uncertainty
ax2.plot(
    game_num,
    sigma,
    color="steelblue",
    linewidth=1.0
)
ax2.set_ylabel("σ")
ax2.set_ylim(bottom=0)
ax2.grid(True, alpha=0.3)

# Panel 3: volatility
ax3.plot(
    game_num,
    volatility,
    color="darkorange",
    linewidth=1.0
)
ax3.set_ylabel("Volatility")
ax3.set_ylim(bottom=0)
ax3.grid(True, alpha=0.3)

# Panel 4: days since previous game
ax4.plot(
    game_num,
    days_since_prev,
    color="seagreen",
    linewidth=1.0
)
ax4.set_ylabel("Days gap")
ax4.set_xlabel("Game number")
ax4.set_ylim(bottom=0)
ax4.grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

# COMMAND ----------

# =============================================================================
# COMPONENT: Coach Plot Export Function
# =============================================================================
# PURPOSE      : Generate and save SSM v2 diagnostic plots for a list of coaches.
#                Each coach gets a 4-panel PNG: rating+Elo, σ, volatility, days gap.
# USAGE        : export_coach_plots([9524, 34738, 35505])
#                export_coach_plots([9524], output_dir="/dbfs/FileStore/my_plots")
# OUTPUT       : One PNG per coach in the output directory.
# =============================================================================

import os
import matplotlib.pyplot as plt
import numpy as np

def export_coach_plots(coach_ids, output_dir="/dbfs/FileStore/ssm_coach_plots"):
    """Generate and save SSM v2 diagnostic plots for specified coaches.

    Args:
        coach_ids: List of coach_id integers to plot.
        output_dir: Directory to save PNGs (created if needed).

    Returns:
        List of saved file paths.
    """
    os.makedirs(output_dir, exist_ok=True)
    saved = []

    ROLLING_WINDOW = 50

    for cid in coach_ids:
        # Load SSM v2 trajectory
        cdf = spark.sql(f"""
            SELECT coach_game_number, mu_after, sigma_after,
                   volatility_after, days_since_prev_game
            FROM naf_catalog.gold_fact.ssm2_rating_history_fact
            WHERE coach_id = {cid}
            ORDER BY coach_game_number
        """).toPandas()

        if len(cdf) == 0:
            print(f"  Coach {cid}: no SSM v2 data — skipped")
            continue

        # Load Elo trajectory
        edf = spark.sql(f"""
            SELECT
                ROW_NUMBER() OVER (ORDER BY game_index, game_id) AS coach_game_number,
                rating_after AS elo_rating
            FROM naf_catalog.gold_fact.rating_history_fact
            WHERE coach_id = {cid} AND scope = 'GLOBAL'
            ORDER BY game_index, game_id
        """).toPandas()

        if len(edf) > 0:
            edf["elo_rolling_median"] = (
                edf["elo_rating"]
                .rolling(window=ROLLING_WINDOW, min_periods=1)
                .median()
            )

        # Load coach name
        name_row = spark.sql(f"""
            SELECT coach_name FROM naf_catalog.gold_dim.coach_dim
            WHERE coach_id = {cid}
        """).first()
        coach_name = name_row["coach_name"] if name_row else str(cid)

        # Extract arrays
        gn = cdf["coach_game_number"].values
        mu = cdf["mu_after"].values
        sig = cdf["sigma_after"].values
        vol = cdf["volatility_after"].values
        days = cdf["days_since_prev_game"].values

        # Plot
        fig, (ax1, ax2, ax3, ax4) = plt.subplots(
            4, 1, figsize=(14, 11), sharex=True,
            gridspec_kw={"height_ratios": [3, 1, 1, 1]}
        )

        # Panel 1: rating + uncertainty
        ax1.fill_between(gn, mu - 2 * sig, mu + 2 * sig,
                         alpha=0.20, color="steelblue", label="SSM2 ± 2σ")
        ax1.plot(gn, mu, color="steelblue", linewidth=1.2, label="SSM2 μ")

        if len(edf) == len(cdf):
            ax1.plot(gn, edf["elo_rating"].values, color="coral",
                     linewidth=0.6, alpha=0.4, linestyle="--", label="Elo (raw)")
            ax1.plot(gn, edf["elo_rolling_median"].values, color="coral",
                     linewidth=1.2, alpha=0.9,
                     label=f"Elo (rolling {ROLLING_WINDOW}-game median)")

        ax1.axhline(y=150, color="gray", linestyle=":", linewidth=0.7,
                     alpha=0.6, label="Initial (150)")
        ax1.set_ylabel("Rating")
        ax1.set_title(f"SSM2 Rating Diagnostics — {coach_name} "
                      f"(coach {cid}, {len(cdf)} games)")
        hp_text = (
            f"σ²_obs={SSM2_SIGMA2_OBS}  q_time={SSM2_Q_TIME}  "
            f"q_game={SSM2_Q_GAME}  v_scale={SSM2_V_SCALE}"
        )
        ax1.plot([], [], ' ', label=hp_text)
        ax1.legend(loc="lower right", fontsize=7)
        ax1.grid(True, alpha=0.3)

        # Panel 2: σ
        ax2.plot(gn, sig, color="steelblue", linewidth=1.0)
        ax2.set_ylabel("σ")
        ax2.set_ylim(bottom=0)
        ax2.grid(True, alpha=0.3)

        # Panel 3: volatility
        ax3.plot(gn, vol, color="darkorange", linewidth=1.0)
        ax3.set_ylabel("Volatility")
        ax3.set_ylim(bottom=0)
        ax3.grid(True, alpha=0.3)

        # Panel 4: days gap
        ax4.plot(gn, days, color="seagreen", linewidth=1.0)
        ax4.set_ylabel("Days gap")
        ax4.set_xlabel("Game number")
        ax4.set_ylim(bottom=0)
        ax4.grid(True, alpha=0.3)

        fig.tight_layout()

        fpath = os.path.join(output_dir, f"coach_{cid}_{coach_name.replace(' ', '_')}.png")
        fig.savefig(fpath, dpi=150, bbox_inches="tight")
        plt.close(fig)
        saved.append(fpath)
        print(f"  Coach {cid} ({coach_name}): saved to {fpath}")

    print(f"\n{len(saved)} plots saved to {output_dir}")
    return saved

# --- Example usage (uncomment to run) ---
# export_coach_plots([9524, 34738, 35505])

# COMMAND ----------

# DBTITLE 1,SSM2 Calibration Coverage
# =============================================================================
# SSM2 Calibration Coverage: does rolling median Elo fall inside SSM2 ± 2σ
# ~95% of the time? Broken down by experience tier.
# Calibration target matches the tuner: 50-game rolling median Elo.
# =============================================================================

import pandas as pd

CAL_MEDIAN_WINDOW = 50

cal_df = spark.sql(f"""
    WITH elo_numbered AS (
        SELECT
            coach_id,
            game_id,
            game_index,
            rating_after,
            ROW_NUMBER() OVER (
                PARTITION BY coach_id
                ORDER BY game_index, game_id
            ) AS coach_game_number
        FROM naf_catalog.gold_fact.rating_history_fact
        WHERE scope = 'GLOBAL'
    ),
    elo_with_median AS (
        SELECT
            coach_id,
            coach_game_number,
            rating_after,
            PERCENTILE_APPROX(rating_after, 0.5) OVER (
                PARTITION BY coach_id
                ORDER BY coach_game_number
                ROWS BETWEEN {CAL_MEDIAN_WINDOW - 1} PRECEDING AND CURRENT ROW
            ) AS elo_rolling_median
        FROM elo_numbered
    ),
    ssm2_elo AS (
        SELECT
            s.coach_id,
            s.coach_game_number,
            s.mu_after,
            s.sigma_after,
            e.rating_after AS elo_raw,
            e.elo_rolling_median
        FROM naf_catalog.gold_fact.ssm2_rating_history_fact s
        JOIN elo_with_median e
            ON  s.coach_id = e.coach_id
            AND s.coach_game_number = e.coach_game_number
    ),
    flagged AS (
        SELECT
            se.*,
            CASE
                WHEN se.coach_game_number <= 30 THEN '01: 1-30 (burn-in)'
                WHEN se.coach_game_number <= 100 THEN '02: 31-100 (developing)'
                WHEN se.coach_game_number <= 300 THEN '03: 101-300 (established)'
                ELSE '04: 301+ (veteran)'
            END AS experience_tier,
            CASE
                WHEN ABS(se.elo_rolling_median - se.mu_after) <= 2.0 * se.sigma_after
                THEN 1 ELSE 0
            END AS median_inside_2sigma,
            CASE
                WHEN ABS(se.elo_raw - se.mu_after) <= 2.0 * se.sigma_after
                THEN 1 ELSE 0
            END AS raw_inside_2sigma
        FROM ssm2_elo se
    )
    SELECT
        experience_tier,
        COUNT(*) AS n_observations,
        COUNT(DISTINCT coach_id) AS n_coaches,
        ROUND(AVG(median_inside_2sigma) * 100, 2) AS median_coverage_pct,
        ROUND(AVG(raw_inside_2sigma) * 100, 2) AS raw_coverage_pct,
        ROUND(AVG(sigma_after), 2) AS avg_sigma,
        ROUND(PERCENTILE_APPROX(sigma_after, 0.5), 2) AS median_sigma
    FROM flagged
    GROUP BY experience_tier
    ORDER BY experience_tier
""").toPandas()

# Overall and mature-only summary
cal_overall = spark.sql(f"""
    WITH elo_numbered AS (
        SELECT
            coach_id,
            game_id,
            game_index,
            rating_after,
            ROW_NUMBER() OVER (
                PARTITION BY coach_id
                ORDER BY game_index, game_id
            ) AS coach_game_number
        FROM naf_catalog.gold_fact.rating_history_fact
        WHERE scope = 'GLOBAL'
    ),
    elo_with_median AS (
        SELECT
            coach_id,
            coach_game_number,
            rating_after,
            PERCENTILE_APPROX(rating_after, 0.5) OVER (
                PARTITION BY coach_id
                ORDER BY coach_game_number
                ROWS BETWEEN {CAL_MEDIAN_WINDOW - 1} PRECEDING AND CURRENT ROW
            ) AS elo_rolling_median
        FROM elo_numbered
    ),
    ssm2_elo AS (
        SELECT
            s.coach_id,
            s.coach_game_number,
            s.mu_after,
            s.sigma_after,
            e.rating_after AS elo_raw,
            e.elo_rolling_median
        FROM naf_catalog.gold_fact.ssm2_rating_history_fact s
        JOIN elo_with_median e
            ON  s.coach_id = e.coach_id
            AND s.coach_game_number = e.coach_game_number
    )
    SELECT
        'ALL' AS scope,
        COUNT(*) AS n_obs,
        ROUND(AVG(CASE WHEN ABS(elo_rolling_median - mu_after) <= 2.0 * sigma_after THEN 1 ELSE 0 END) * 100, 2) AS median_coverage_pct,
        ROUND(AVG(CASE WHEN ABS(elo_raw - mu_after) <= 2.0 * sigma_after THEN 1 ELSE 0 END) * 100, 2) AS raw_coverage_pct
    FROM ssm2_elo
    UNION ALL
    SELECT
        'MATURE (game 100+)' AS scope,
        COUNT(*) AS n_obs,
        ROUND(AVG(CASE WHEN ABS(elo_rolling_median - mu_after) <= 2.0 * sigma_after THEN 1 ELSE 0 END) * 100, 2) AS median_coverage_pct,
        ROUND(AVG(CASE WHEN ABS(elo_raw - mu_after) <= 2.0 * sigma_after THEN 1 ELSE 0 END) * 100, 2) AS raw_coverage_pct
    FROM ssm2_elo
    WHERE coach_game_number >= 100
""").toPandas()

print("=" * 70)
print(f"SSM2 CALIBRATION COVERAGE — {CAL_MEDIAN_WINDOW}-game rolling median "
      f"Elo inside ±2σ (target ≈ 95%)")
print("(raw Elo coverage shown for comparison)")
print("=" * 70)
print(f"\nHyperparameters: σ²_obs={SSM2_SIGMA2_OBS}  q_time={SSM2_Q_TIME}  "
      f"q_game={SSM2_Q_GAME}  prior_σ={SSM2_PRIOR_SIGMA}")
print(f"  v_base={SSM2_V_BASE}  v_scale={SSM2_V_SCALE}  v_decay={SSM2_V_DECAY}  "
      f"v_min={SSM2_V_MIN}  v_max={SSM2_V_MAX}\n")
print(cal_df.to_string(index=False))
print()
print(cal_overall.to_string(index=False))
print("=" * 70)

# COMMAND ----------

# # =============================================================================
# # COMPONENT: SSM2 Hyperparameter Tuner
# # =============================================================================
# # PURPOSE      : Find optimal (sigma2_obs, q_time, q_game, v_scale) by grid
# #                search, calibrated against 50-game rolling median Elo.
# # TARGET       : Rolling median Elo inside SSM2 ± 2σ ~95% of the time for
# #                coaches with 100+ games (weighted toward veterans).
# # STRATEGY     : Two-pass grid search — coarse (4D, wide) then fine (4D,
# #                narrow neighbourhood around coarse best).
# # FIXED PARAMS : prior_sigma=50, max_days=180, v_decay=0.90, v_base=0.25,
# #                v_min=0.0, v_max=16.0
# # PREREQS      : Requires ssm2_feed_rows (from the SSM2 engine cell above)
# #                and Elo data loaded from rating_history_fact.
# # RUNTIME NOTE : Each grid point runs the full SSM2 loop over all games.
# #                Coarse grid (~80 points) + fine grid (~81 points) ≈ 160 runs.
# #                At ~30-60s per run on Community Edition, expect 1-3 hours.
# # =============================================================================

# import math
# import datetime as dt
# import time
# from collections import defaultdict
# import itertools

# # ---------------------------------------------------------------------------
# # 1) Fixed parameters (structural — not tuned)
# # ---------------------------------------------------------------------------
# TUNE_PRIOR_SIGMA = 50.0
# TUNE_PRIOR_P     = TUNE_PRIOR_SIGMA ** 2
# TUNE_MAX_DAYS    = 180.0
# TUNE_V_DECAY     = 0.90
# TUNE_V_BASE      = 0.25
# TUNE_V_MIN       = 0.00
# TUNE_V_MAX       = 16.0
# TUNE_MIN_P       = 1e-6

# # Use same initial rating and Elo scale from config (already loaded above)
# TUNE_INITIAL_RATING  = SSM2_INITIAL_RATING
# TUNE_ELO_SCALE       = SSM2_ELO_SCALE
# TUNE_LN10_OVER_SCALE = SSM2_LN10_OVER_SCALE

# # Median window for calibration target
# TUNE_MEDIAN_WINDOW = 50

# # ---------------------------------------------------------------------------
# # 2) Load Elo trajectories into a dict: coach_id -> list of (game_number, elo)
# #    Reuses the rating_history_fact table. Built once for all grid points.
# # ---------------------------------------------------------------------------
# print("Loading Elo trajectories for calibration target...")
# _elo_raw = spark.sql("""
#     SELECT
#         coach_id,
#         game_id,
#         game_index,
#         rating_after,
#         ROW_NUMBER() OVER (
#             PARTITION BY coach_id
#             ORDER BY game_index, game_id
#         ) AS coach_game_number
#     FROM naf_catalog.gold_fact.rating_history_fact
#     WHERE scope = 'GLOBAL'
# """).collect()

# # Build per-coach Elo arrays and rolling medians
# _elo_by_coach = defaultdict(list)
# for row in _elo_raw:
#     _elo_by_coach[int(row["coach_id"])].append(
#         (int(row["coach_game_number"]), float(row["rating_after"]))
#     )

# # Pre-compute rolling median Elo per coach: dict[coach_id][game_number] -> median
# import statistics

# elo_rolling_median = {}
# for cid, games in _elo_by_coach.items():
#     games.sort(key=lambda x: x[0])
#     elos = [e for _, e in games]
#     medians = {}
#     for i, (gn, _) in enumerate(games):
#         window_start = max(0, i - TUNE_MEDIAN_WINDOW + 1)
#         window = elos[window_start:i + 1]
#         medians[gn] = statistics.median(window)
#     elo_rolling_median[cid] = medians

# del _elo_raw, _elo_by_coach
# print(f"Loaded rolling {TUNE_MEDIAN_WINDOW}-game median Elo for "
#       f"{len(elo_rolling_median)} coaches")

# # ---------------------------------------------------------------------------
# # 3) Pre-process feed rows into a compact list for fast iteration
# #    (avoid repeated dict lookups inside the inner loop)
# # ---------------------------------------------------------------------------
# print("Pre-processing feed rows...")
# _feed_compact = []
# for r in ssm2_feed_rows:
#     event_ts = r["event_timestamp"]
#     game_date = r["game_date"]
#     date_id = int(r["date_id"])
#     if event_ts is not None:
#         cur_dt = event_ts.replace(tzinfo=None) if getattr(event_ts, "tzinfo", None) else event_ts
#     elif game_date is not None:
#         cur_dt = dt.datetime.combine(game_date, dt.time(0, 0))
#     else:
#         cur_dt = dt.datetime.strptime(str(date_id), "%Y%m%d")

#     _feed_compact.append((
#         int(r["home_coach_id"]),
#         int(r["away_coach_id"]),
#         float(r["result_home"]),
#         float(r["result_away"]),
#         cur_dt,
#     ))
# print(f"Feed ready: {len(_feed_compact)} games")

# # ---------------------------------------------------------------------------
# # 4) Core engine function — runs the full SSM2 loop for one parameter set.
# #    Returns per-coach lists of (game_number, mu_after, sigma_after).
# # ---------------------------------------------------------------------------
# def run_ssm2_engine(sigma2_obs, q_time, q_game, v_scale):
#     """Run the full SSM2 engine with given tunable params.

#     Returns:
#         dict[coach_id] -> list of (game_number, mu_after, P_after)
#     """
#     # State: mu, P, volatility, shock_ewma, last_dt
#     state = {}
#     game_counts = {}
#     results = defaultdict(list)

#     def _default():
#         return [TUNE_INITIAL_RATING, TUNE_PRIOR_P, TUNE_V_BASE, 0.0, None]

#     for home_id, away_id, res_h, res_a, cur_dt in _feed_compact:
#         # Get or init states
#         sh = state.get(home_id)
#         if sh is None:
#             sh = _default()
#             state[home_id] = sh
#         sa = state.get(away_id)
#         if sa is None:
#             sa = _default()
#             state[away_id] = sa

#         # --- Predict home ---
#         days_h = 0.0
#         if sh[4] is not None:
#             days_h = max((cur_dt - sh[4]).total_seconds() / 86400.0, 0.0)
#         ts_h = math.sqrt(min(days_h, TUNE_MAX_DAYS))
#         q_added_h = q_time * ts_h + q_game + sh[2]  # sh[2] = volatility
#         mu_h_pred = sh[0]
#         P_h_pred = sh[1] + q_added_h

#         # --- Predict away ---
#         days_a = 0.0
#         if sa[4] is not None:
#             days_a = max((cur_dt - sa[4]).total_seconds() / 86400.0, 0.0)
#         ts_a = math.sqrt(min(days_a, TUNE_MAX_DAYS))
#         q_added_a = q_time * ts_a + q_game + sa[2]
#         mu_a_pred = sa[0]
#         P_a_pred = sa[1] + q_added_a

#         # --- Observe home ---
#         p_exp_h = 1.0 / (1.0 + 10.0 ** ((mu_a_pred - mu_h_pred) / TUNE_ELO_SCALE))
#         H_h = p_exp_h * (1.0 - p_exp_h) * TUNE_LN10_OVER_SCALE
#         S_h = H_h * H_h * P_h_pred + H_h * H_h * P_a_pred + sigma2_obs
#         innov_h = res_h - p_exp_h
#         if S_h < 1e-12:
#             K_h = 0.0
#             mu_h_post = mu_h_pred
#             P_h_post = max(P_h_pred, TUNE_MIN_P)
#         else:
#             K_h = H_h * P_h_pred / S_h
#             mu_h_post = mu_h_pred + K_h * innov_h
#             P_h_post = max((1.0 - K_h * H_h) * P_h_pred, TUNE_MIN_P)

#         # --- Observe away ---
#         p_exp_a = 1.0 / (1.0 + 10.0 ** ((mu_h_pred - mu_a_pred) / TUNE_ELO_SCALE))
#         H_a = p_exp_a * (1.0 - p_exp_a) * TUNE_LN10_OVER_SCALE
#         S_a = H_a * H_a * P_a_pred + H_a * H_a * P_h_pred + sigma2_obs
#         innov_a = res_a - p_exp_a
#         if S_a < 1e-12:
#             K_a = 0.0
#             mu_a_post = mu_a_pred
#             P_a_post = max(P_a_pred, TUNE_MIN_P)
#         else:
#             K_a = H_a * P_a_pred / S_a
#             mu_a_post = mu_a_pred + K_a * innov_a
#             P_a_post = max((1.0 - K_a * H_a) * P_a_pred, TUNE_MIN_P)

#         # --- Volatility update ---
#         shock_h = TUNE_V_DECAY * sh[3] + (1.0 - TUNE_V_DECAY) * (innov_h ** 2)
#         vol_h = max(TUNE_V_MIN, min(TUNE_V_BASE + v_scale * shock_h, TUNE_V_MAX))

#         shock_a = TUNE_V_DECAY * sa[3] + (1.0 - TUNE_V_DECAY) * (innov_a ** 2)
#         vol_a = max(TUNE_V_MIN, min(TUNE_V_BASE + v_scale * shock_a, TUNE_V_MAX))

#         # --- Game counts ---
#         gn_h = game_counts.get(home_id, 0) + 1
#         game_counts[home_id] = gn_h
#         gn_a = game_counts.get(away_id, 0) + 1
#         game_counts[away_id] = gn_a

#         # --- Record results for calibration ---
#         results[home_id].append((gn_h, mu_h_post, P_h_post))
#         results[away_id].append((gn_a, mu_a_post, P_a_post))

#         # --- Persist state (in-place for speed) ---
#         sh[0] = mu_h_post
#         sh[1] = P_h_post
#         sh[2] = vol_h
#         sh[3] = shock_h
#         sh[4] = cur_dt

#         sa[0] = mu_a_post
#         sa[1] = P_a_post
#         sa[2] = vol_a
#         sa[3] = shock_a
#         sa[4] = cur_dt

#     return results

# # ---------------------------------------------------------------------------
# # 5) Calibration scorer — computes weighted coverage against rolling median
# # ---------------------------------------------------------------------------
# def compute_coverage(engine_results):
#     """Compute coverage: fraction of games where rolling median Elo is
#     inside SSM2 mu ± 2*sigma.

#     Returns:
#         dict with keys: 'overall', 'mature', 'veteran', 'established',
#                         'developing', 'burnin', 'objective'
#     """
#     tier_hits = {
#         "burnin": [0, 0],       # [inside, total]  games 1-30
#         "developing": [0, 0],   # games 31-100
#         "established": [0, 0],  # games 101-300
#         "veteran": [0, 0],      # games 301+
#     }

#     for coach_id, trajectory in engine_results.items():
#         medians = elo_rolling_median.get(coach_id)
#         if medians is None:
#             continue

#         for gn, mu_post, P_post in trajectory:
#             median_elo = medians.get(gn)
#             if median_elo is None:
#                 continue

#             sigma = math.sqrt(P_post)
#             inside = 1 if abs(median_elo - mu_post) <= 2.0 * sigma else 0

#             if gn <= 30:
#                 tier_hits["burnin"][0] += inside
#                 tier_hits["burnin"][1] += 1
#             elif gn <= 100:
#                 tier_hits["developing"][0] += inside
#                 tier_hits["developing"][1] += 1
#             elif gn <= 300:
#                 tier_hits["established"][0] += inside
#                 tier_hits["established"][1] += 1
#             else:
#                 tier_hits["veteran"][0] += inside
#                 tier_hits["veteran"][1] += 1

#     def _pct(key):
#         h, t = tier_hits[key]
#         return (h / t * 100) if t > 0 else 0.0

#     cov = {
#         "burnin":      _pct("burnin"),
#         "developing":  _pct("developing"),
#         "established": _pct("established"),
#         "veteran":     _pct("veteran"),
#         "n_burnin":      tier_hits["burnin"][1],
#         "n_developing":  tier_hits["developing"][1],
#         "n_established": tier_hits["established"][1],
#         "n_veteran":     tier_hits["veteran"][1],
#     }

#     # Mature = 100+ games
#     mat_inside = tier_hits["established"][0] + tier_hits["veteran"][0]
#     mat_total  = tier_hits["established"][1] + tier_hits["veteran"][1]
#     cov["mature"] = (mat_inside / mat_total * 100) if mat_total > 0 else 0.0

#     all_inside = sum(v[0] for v in tier_hits.values())
#     all_total  = sum(v[1] for v in tier_hits.values())
#     cov["overall"] = (all_inside / all_total * 100) if all_total > 0 else 0.0

#     # Weighted objective: minimise |coverage - 95%|
#     # Weights: veteran 60%, established 25%, developing 10%, burn-in 5%
#     obj = (
#         0.60 * abs(cov["veteran"]     - 95.0)
#         + 0.25 * abs(cov["established"] - 95.0)
#         + 0.10 * abs(cov["developing"]  - 95.0)
#         + 0.05 * abs(cov["burnin"]      - 95.0)
#     )
#     cov["objective"] = obj
#     return cov

# # ---------------------------------------------------------------------------
# # 6) Grid definitions
# # ---------------------------------------------------------------------------
# # Coarse grid: ~4 values per param = 4^4 = 256 points (manageable)
# COARSE_GRID = {
#     "sigma2_obs": [0.01, 0.03, 0.05, 0.10],
#     "q_time":     [0.50, 1.00, 1.50, 2.50],
#     "q_game":     [0.05, 0.15, 0.30, 0.60],
#     "v_scale":    [4.0,  8.0,  12.0, 20.0],
# }

# def make_fine_grid(best_params):
#     """Create a fine grid centred on best coarse params.

#     3 values per param (below, at, above) = 3^4 = 81 points.
#     Step sizes are ~half the coarse spacing around the best value.
#     """
#     def _neighbourhood(val, candidates):
#         idx = min(range(len(candidates)), key=lambda i: abs(candidates[i] - val))
#         if idx == 0:
#             step = (candidates[1] - candidates[0]) / 2
#         elif idx == len(candidates) - 1:
#             step = (candidates[-1] - candidates[-2]) / 2
#         else:
#             step = min(candidates[idx] - candidates[idx - 1],
#                        candidates[idx + 1] - candidates[idx]) / 2
#         return [max(val - step, candidates[0] * 0.5), val, val + step]

#     return {
#         "sigma2_obs": _neighbourhood(best_params["sigma2_obs"],
#                                      COARSE_GRID["sigma2_obs"]),
#         "q_time":     _neighbourhood(best_params["q_time"],
#                                      COARSE_GRID["q_time"]),
#         "q_game":     _neighbourhood(best_params["q_game"],
#                                      COARSE_GRID["q_game"]),
#         "v_scale":    _neighbourhood(best_params["v_scale"],
#                                      COARSE_GRID["v_scale"]),
#     }

# # ---------------------------------------------------------------------------
# # 7) Run grid search
# # ---------------------------------------------------------------------------
# def run_grid(grid, label="Grid"):
#     """Run all combinations in the grid. Returns list of (params, coverage)."""
#     keys = list(grid.keys())
#     combos = list(itertools.product(*[grid[k] for k in keys]))
#     print(f"\n{'='*70}")
#     print(f"{label}: {len(combos)} parameter combinations")
#     print(f"{'='*70}")

#     all_results = []
#     t0 = time.time()

#     for i, vals in enumerate(combos):
#         params = dict(zip(keys, vals))
#         t_start = time.time()

#         engine_out = run_ssm2_engine(
#             sigma2_obs=params["sigma2_obs"],
#             q_time=params["q_time"],
#             q_game=params["q_game"],
#             v_scale=params["v_scale"],
#         )
#         cov = compute_coverage(engine_out)
#         cov["params"] = params
#         all_results.append(cov)

#         elapsed = time.time() - t_start
#         total_elapsed = time.time() - t0
#         avg_per = total_elapsed / (i + 1)
#         remaining = avg_per * (len(combos) - i - 1)

#         if (i + 1) % 10 == 0 or (i + 1) == len(combos):
#             print(
#                 f"  [{i+1}/{len(combos)}] "
#                 f"obj={cov['objective']:5.2f}  "
#                 f"mature={cov['mature']:5.1f}%  "
#                 f"vet={cov['veteran']:5.1f}%  "
#                 f"est={cov['established']:5.1f}%  "
#                 f"σ²_obs={params['sigma2_obs']:.3f} "
#                 f"q_t={params['q_time']:.2f} "
#                 f"q_g={params['q_game']:.2f} "
#                 f"v_s={params['v_scale']:.1f}  "
#                 f"({elapsed:.0f}s, ~{remaining/60:.0f}m left)"
#             )

#     return all_results

# # --- Coarse pass ---
# coarse_results = run_grid(COARSE_GRID, "COARSE GRID")
# coarse_best = min(coarse_results, key=lambda x: x["objective"])

# print(f"\nCoarse best: objective={coarse_best['objective']:.3f}")
# print(f"  Params: {coarse_best['params']}")
# print(f"  Veteran={coarse_best['veteran']:.1f}%  "
#       f"Established={coarse_best['established']:.1f}%  "
#       f"Developing={coarse_best['developing']:.1f}%  "
#       f"Burn-in={coarse_best['burnin']:.1f}%")

# # --- Fine pass ---
# fine_grid = make_fine_grid(coarse_best["params"])
# print(f"\nFine grid neighbourhood:")
# for k, v in fine_grid.items():
#     print(f"  {k}: {[round(x, 4) for x in v]}")

# fine_results = run_grid(fine_grid, "FINE GRID")
# fine_best = min(fine_results, key=lambda x: x["objective"])

# print(f"\n{'='*70}")
# print(f"FINAL BEST PARAMETERS")
# print(f"{'='*70}")
# print(f"  sigma2_obs = {fine_best['params']['sigma2_obs']:.4f}")
# print(f"  q_time     = {fine_best['params']['q_time']:.4f}")
# print(f"  q_game     = {fine_best['params']['q_game']:.4f}")
# print(f"  v_scale    = {fine_best['params']['v_scale']:.4f}")
# print(f"\n  Objective  = {fine_best['objective']:.3f}")
# print(f"  Veteran    = {fine_best['veteran']:.2f}%  (n={fine_best['n_veteran']})")
# print(f"  Established= {fine_best['established']:.2f}%  (n={fine_best['n_established']})")
# print(f"  Developing = {fine_best['developing']:.2f}%  (n={fine_best['n_developing']})")
# print(f"  Burn-in    = {fine_best['burnin']:.2f}%  (n={fine_best['n_burnin']})")
# print(f"  Mature(100+)={fine_best['mature']:.2f}%")
# print(f"  Overall    = {fine_best['overall']:.2f}%")
# print(f"\nFixed params: prior_σ={TUNE_PRIOR_SIGMA} max_days={TUNE_MAX_DAYS} "
#       f"v_decay={TUNE_V_DECAY} v_base={TUNE_V_BASE} v_min={TUNE_V_MIN} "
#       f"v_max={TUNE_V_MAX}")
# print(f"\nCalibration target: {TUNE_MEDIAN_WINDOW}-game rolling median Elo "
#       f"inside ±2σ")
# print(f"Objective: weighted |coverage - 95%| "
#       f"(vet 60%, est 25%, dev 10%, burn 5%)")

# # --- Top 10 from fine grid ---
# fine_sorted = sorted(fine_results, key=lambda x: x["objective"])
# print(f"\nTop 10 fine-grid candidates:")
# print(f"{'Obj':>6}  {'Vet%':>6}  {'Est%':>6}  {'Dev%':>6}  {'Brn%':>6}  "
#       f"{'σ²_obs':>7}  {'q_time':>7}  {'q_game':>7}  {'v_scale':>7}")
# for r in fine_sorted[:10]:
#     p = r["params"]
#     print(f"{r['objective']:6.2f}  {r['veteran']:6.1f}  {r['established']:6.1f}  "
#           f"{r['developing']:6.1f}  {r['burnin']:6.1f}  "
#           f"{p['sigma2_obs']:7.4f}  {p['q_time']:7.3f}  "
#           f"{p['q_game']:7.3f}  {p['v_scale']:7.2f}")

# print(f"\n{'='*70}")
# print("To apply: update SSM2_SIGMA2_OBS, SSM2_Q_TIME, SSM2_Q_GAME, "
#       "SSM2_V_SCALE above and re-run the engine cell.")
# print(f"{'='*70}")

# COMMAND ----------

# =============================================================================
# COMPONENT: Structured Model Comparison — SSM v1, SSM v2, Elo
# =============================================================================
# PURPOSE      : Compare 6 predictive models on a held-out test window:
#                1. Elo              — baseline, logistic(Δrating)
#                2. SSM v1           — logistic(Δμ), ignores σ
#                3. SSM v1-UA        — uncertainty-aware: logistic scaled by σ
#                4. SSM v2           — logistic(Δμ), ignores σ
#                5. SSM v2-UA        — uncertainty-aware: logistic scaled by σ
#                6. Naive            — always predict 0.5
#
# TEST SET     : Last 12 months of games (by date_id). All systems were
#                trained on all prior games when making each prediction.
#
# UA METHOD    : Probit approximation for integrating over Gaussian uncertainty.
#                Instead of logistic(Δμ / S), use logistic(Δμ / S_eff) where
#                S_eff = S × √(1 + (π/8) × (ln(10)/S)² × (σ²_self + σ²_opp))
#                This shrinks predictions toward 0.5 when uncertainty is high.
#
# METRICS      : Log-loss, Brier score, accuracy.
# PLOTS        : Calibration curves (4 panels), uncertainty-stratified Brier.
# TABLES       : Overall comparison, per-tier breakdown, in-sample/OOS check.
# =============================================================================

import math
import numpy as np
import matplotlib.pyplot as plt
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# 1) Define test window: last 12 months of games
# ---------------------------------------------------------------------------
EVAL_TEST_MONTHS = 12
eval_cutoff_row = spark.sql(f"""
    SELECT CAST(
        DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -{EVAL_TEST_MONTHS}), 'yyyyMMdd')
    AS INT) AS cutoff_date_id
""").first()
eval_cutoff = eval_cutoff_row["cutoff_date_id"]
print(f"Test window: games with date_id >= {eval_cutoff} (last {EVAL_TEST_MONTHS} months)")

# ---------------------------------------------------------------------------
# 2) Load predictions from all systems for the test window
# ---------------------------------------------------------------------------
eval_df = spark.sql(f"""
    WITH elo AS (
        SELECT
            game_id,
            coach_id,
            game_index,
            date_id,
            result_numeric,
            score_expected AS elo_pred
        FROM naf_catalog.gold_fact.rating_history_fact
        WHERE scope = 'GLOBAL'
          AND date_id >= {eval_cutoff}
    ),
    ssm1 AS (
        SELECT
            game_id,
            coach_id,
            score_expected AS ssm1_pred,
            sigma_before AS ssm1_sigma_before,
            opponent_sigma_before AS ssm1_opp_sigma_before,
            coach_game_number AS ssm1_game_number
        FROM naf_catalog.gold_fact.ssm_rating_history_fact
        WHERE date_id >= {eval_cutoff}
    ),
    ssm2 AS (
        SELECT
            game_id,
            coach_id,
            score_expected AS ssm2_pred,
            sigma_before AS ssm2_sigma_before,
            opponent_sigma_before AS ssm2_opp_sigma_before,
            coach_game_number AS ssm2_game_number
        FROM naf_catalog.gold_fact.ssm2_rating_history_fact
        WHERE date_id >= {eval_cutoff}
    )
    SELECT
        e.game_id,
        e.coach_id,
        e.game_index,
        e.date_id,
        e.result_numeric,
        e.elo_pred,
        -- SSM v1
        s1.ssm1_pred,
        s1.ssm1_sigma_before,
        s1.ssm1_opp_sigma_before,
        s1.ssm1_game_number,
        -- SSM v2
        s2.ssm2_pred,
        s2.ssm2_sigma_before,
        s2.ssm2_opp_sigma_before,
        s2.ssm2_game_number
    FROM elo e
    JOIN ssm1 s1
        ON e.game_id = s1.game_id AND e.coach_id = s1.coach_id
    JOIN ssm2 s2
        ON e.game_id = s2.game_id AND e.coach_id = s2.coach_id
""").toPandas()

print(f"Test set: {len(eval_df)} observations "
      f"({eval_df['game_id'].nunique()} games, "
      f"{eval_df['coach_id'].nunique()} coaches)")

# ---------------------------------------------------------------------------
# 3) Compute uncertainty-aware predictions (probit approximation)
# ---------------------------------------------------------------------------
# The standard logistic prediction is: p = 1 / (1 + 10^(Δμ / S))
# which is equivalent to sigmoid(-Δμ × ln(10) / S).
#
# For uncertainty-aware predictions, we integrate over Gaussian uncertainty
# in both players' ratings. The probit approximation gives:
#   p_UA ≈ sigmoid(-Δμ × ln(10) / S / √(1 + (π/8) × (ln(10)/S)² × σ²_combined))
#
# This is equivalent to using an effective scale:
#   S_eff = S × √(1 + (π/8) × (ln(10)/S)² × σ²_combined)
#
# When σ_combined is large, S_eff increases, pulling predictions toward 0.5.
# When σ_combined is small, S_eff ≈ S, preserving the original prediction.

ELO_SCALE = 150.0  # Same as SSM engine
LN10_OVER_S = math.log(10.0) / ELO_SCALE
PI_OVER_8 = math.pi / 8.0
UA_COEFF = PI_OVER_8 * LN10_OVER_S ** 2  # constant: π/8 × (ln10/S)²

def compute_ua_predictions(pred_raw, sigma_self, sigma_opp):
    """Compute uncertainty-aware predictions using probit approximation.

    Args:
        pred_raw: Original logistic predictions (from score_expected).
        sigma_self: Coach's σ_before (standard deviation, not variance).
        sigma_opp: Opponent's σ_before.

    Returns:
        Uncertainty-aware predictions, shrunk toward 0.5 by combined σ.
    """
    sigma2_combined = sigma_self ** 2 + sigma_opp ** 2
    scale_factor = np.sqrt(1.0 + UA_COEFF * sigma2_combined)

    # Convert raw prediction back to rating difference, then rescale.
    # p = 1 / (1 + 10^(Δ/S))  →  Δ = -S × log10(1/p - 1)
    # Then p_UA = 1 / (1 + 10^(Δ / S_eff)) = 1 / (1 + 10^(Δ / (S × scale_factor)))
    eps = 1e-10
    p_clipped = np.clip(pred_raw, eps, 1.0 - eps)
    log_odds = np.log10(p_clipped / (1.0 - p_clipped))  # = -Δ/S in base-10 log
    # Shrink log-odds by scale factor
    log_odds_ua = log_odds / scale_factor
    return 1.0 / (1.0 + 10.0 ** (-log_odds_ua))

# SSM v1 uncertainty-aware
eval_df["ssm1_ua_pred"] = compute_ua_predictions(
    eval_df["ssm1_pred"].values,
    eval_df["ssm1_sigma_before"].values,
    eval_df["ssm1_opp_sigma_before"].values,
)

# SSM v2 uncertainty-aware
eval_df["ssm2_ua_pred"] = compute_ua_predictions(
    eval_df["ssm2_pred"].values,
    eval_df["ssm2_sigma_before"].values,
    eval_df["ssm2_opp_sigma_before"].values,
)

# Combined sigma for binning
eval_df["ssm2_combined_sigma"] = np.sqrt(
    eval_df["ssm2_sigma_before"].values ** 2
    + eval_df["ssm2_opp_sigma_before"].values ** 2
)
eval_df["ssm1_combined_sigma"] = np.sqrt(
    eval_df["ssm1_sigma_before"].values ** 2
    + eval_df["ssm1_opp_sigma_before"].values ** 2
)

print(f"\nUA predictions computed. Scale coefficient (π/8 × (ln10/S)²) = {UA_COEFF:.6f}")
print(f"SSM v1 σ_combined: mean={eval_df['ssm1_combined_sigma'].mean():.1f}, "
      f"std={eval_df['ssm1_combined_sigma'].std():.1f}, "
      f"range=[{eval_df['ssm1_combined_sigma'].min():.1f}, "
      f"{eval_df['ssm1_combined_sigma'].max():.1f}]")
print(f"SSM v2 σ_combined: mean={eval_df['ssm2_combined_sigma'].mean():.1f}, "
      f"std={eval_df['ssm2_combined_sigma'].std():.1f}, "
      f"range=[{eval_df['ssm2_combined_sigma'].min():.1f}, "
      f"{eval_df['ssm2_combined_sigma'].max():.1f}]")

# ---------------------------------------------------------------------------
# 4) Metric functions
# ---------------------------------------------------------------------------
EPS = 1e-10

def log_loss(y_true, y_pred):
    """Log-loss for expected-score predictions (y ∈ {0, 0.5, 1})."""
    p = np.clip(y_pred, EPS, 1.0 - EPS)
    return -np.mean(y_true * np.log(p) + (1 - y_true) * np.log(1 - p))

def brier_score(y_true, y_pred):
    """Brier score: mean squared error of predictions."""
    return np.mean((y_pred - y_true) ** 2)

def accuracy(y_true, y_pred):
    """Accuracy: fraction where predicted favourite wins (draws excluded)."""
    mask = y_true != 0.5
    if mask.sum() == 0:
        return float("nan")
    correct = ((y_pred[mask] > 0.5) & (y_true[mask] == 1.0)) | \
              ((y_pred[mask] < 0.5) & (y_true[mask] == 0.0))
    return correct.mean()

# ---------------------------------------------------------------------------
# 5) Compute metrics for all 6 models
# ---------------------------------------------------------------------------
y = eval_df["result_numeric"].values

models = {
    "Elo":        np.clip(eval_df["elo_pred"].values, EPS, 1 - EPS),
    "SSM v1":     np.clip(eval_df["ssm1_pred"].values, EPS, 1 - EPS),
    "SSM v1-UA":  np.clip(eval_df["ssm1_ua_pred"].values, EPS, 1 - EPS),
    "SSM v2":     np.clip(eval_df["ssm2_pred"].values, EPS, 1 - EPS),
    "SSM v2-UA":  np.clip(eval_df["ssm2_ua_pred"].values, EPS, 1 - EPS),
    "Naive":      np.full_like(y, 0.5),
}

metrics = {}
for name, preds in models.items():
    metrics[name] = {
        "log_loss": log_loss(y, preds),
        "brier": brier_score(y, preds),
        "accuracy": accuracy(y, preds),
    }

# Print overall comparison table
print(f"\n{'='*78}")
print(f"STRUCTURED MODEL COMPARISON — test set: date_id >= {eval_cutoff}")
print(f"{'='*78}")
print(f"\n{'Model':<15} {'Log-loss':>10} {'Brier':>10} {'Accuracy':>10}  "
      f"{'ΔLL vs Elo':>11} {'ΔBr vs Elo':>11}")
print(f"{'-'*15} {'-'*10} {'-'*10} {'-'*10}  {'-'*11} {'-'*11}")

elo_ll = metrics["Elo"]["log_loss"]
elo_br = metrics["Elo"]["brier"]

for name, m in metrics.items():
    ll_delta = elo_ll - m["log_loss"]
    br_delta = elo_br - m["brier"]
    delta_str_ll = f"{ll_delta:+11.5f}" if name != "Elo" else f"{'—':>11}"
    delta_str_br = f"{br_delta:+11.5f}" if name != "Elo" else f"{'—':>11}"
    print(f"{name:<15} {m['log_loss']:10.5f} {m['brier']:10.5f} "
          f"{m['accuracy']:10.3f}  {delta_str_ll} {delta_str_br}")

print(f"\nPositive Δ = model is better than Elo (lower loss)")

# Relative improvement of best UA vs Elo
for ua_name in ["SSM v1-UA", "SSM v2-UA"]:
    ll_imp = (elo_ll - metrics[ua_name]["log_loss"]) / elo_ll * 100
    br_imp = (elo_br - metrics[ua_name]["brier"]) / elo_br * 100
    print(f"{ua_name} vs Elo: log-loss {ll_imp:+.3f}%, Brier {br_imp:+.3f}%")

# ---------------------------------------------------------------------------
# 6) Calibration plots — 4 panels
# ---------------------------------------------------------------------------
fig, axes = plt.subplots(1, 4, figsize=(20, 5))

cal_models = [
    ("Elo", models["Elo"]),
    ("SSM v1-UA", models["SSM v1-UA"]),
    ("SSM v2", models["SSM v2"]),
    ("SSM v2-UA", models["SSM v2-UA"]),
]

for ax, (name, preds) in zip(axes, cal_models):
    bins = np.arange(0.0, 1.05, 0.1)
    bin_indices = np.digitize(preds, bins) - 1
    bin_centres = []
    bin_actuals = []
    bin_counts = []

    for i in range(len(bins) - 1):
        mask = bin_indices == i
        if mask.sum() >= 20:
            bin_centres.append((bins[i] + bins[i + 1]) / 2)
            bin_actuals.append(y[mask].mean())
            bin_counts.append(mask.sum())

    ax.plot([0, 1], [0, 1], "k--", alpha=0.5, label="Perfect calibration")
    ax.bar(bin_centres, bin_actuals, width=0.08, alpha=0.6, color="steelblue",
           label="Actual outcome rate")
    ax.scatter(bin_centres, bin_actuals, color="coral", zorder=5, s=40)
    ax.set_xlabel("Predicted win probability")
    ax.set_ylabel("Actual outcome (mean)")
    ax.set_title(f"{name} Calibration")
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.legend(loc="lower right", fontsize=7)
    ax.grid(True, alpha=0.3)

    for xc, yc, n in zip(bin_centres, bin_actuals, bin_counts):
        ax.text(xc, yc + 0.03, f"n={n}", ha="center", fontsize=5, alpha=0.7)

plt.suptitle("Calibration Comparison", fontsize=14, y=1.02)
plt.tight_layout()
plt.show()

# ---------------------------------------------------------------------------
# 7) Uncertainty-stratified Brier score — SSM v2 σ quintiles
# ---------------------------------------------------------------------------
fig, ax = plt.subplots(figsize=(10, 5))

sigma_combined = eval_df["ssm2_combined_sigma"].values
sigma_bins = np.percentile(sigma_combined, [0, 20, 40, 60, 80, 100])
sigma_bin_idx = np.digitize(sigma_combined, sigma_bins) - 1
sigma_bin_idx = np.clip(sigma_bin_idx, 0, len(sigma_bins) - 2)

sigma_labels = []
brier_by_model = {name: [] for name in ["Elo", "SSM v2", "SSM v2-UA"]}

for i in range(len(sigma_bins) - 1):
    mask = sigma_bin_idx == i
    if mask.sum() >= 50:
        sigma_labels.append(f"σ∈[{sigma_bins[i]:.0f},{sigma_bins[i+1]:.0f})")
        for name in brier_by_model:
            brier_by_model[name].append(brier_score(y[mask], models[name][mask]))

x_pos = np.arange(len(sigma_labels))
width = 0.25
colors = {"Elo": "coral", "SSM v2": "steelblue", "SSM v2-UA": "seagreen"}

for j, (name, briers) in enumerate(brier_by_model.items()):
    ax.bar(x_pos + (j - 1) * width, briers, width, alpha=0.7,
           color=colors[name], label=name)

ax.set_xticks(x_pos)
ax.set_xticklabels(sigma_labels, fontsize=8, rotation=15)
ax.set_ylabel("Brier score (lower = better)")
ax.set_title("Prediction quality by SSM v2 uncertainty level")
ax.legend(loc="lower right", fontsize=9)
ax.grid(True, alpha=0.3, axis="y")
plt.tight_layout()
plt.show()

# ---------------------------------------------------------------------------
# 8) Per-tier breakdown (UNWEIGHTED — all observations count equally)
# ---------------------------------------------------------------------------
eval_df["tier"] = eval_df["ssm2_game_number"].apply(
    lambda g: "01: 1-30" if g <= 30
    else "02: 31-100" if g <= 100
    else "03: 101-300" if g <= 300
    else "04: 301+"
)

print(f"\n{'='*110}")
print("METRICS BY EXPERIENCE TIER (unweighted)")
print(f"{'='*110}")
print(f"\n{'Tier':<15} {'N':>8}  {'Elo LL':>9} {'v1-UA LL':>9} {'v2 LL':>9} {'v2-UA LL':>9}  "
      f"{'Elo Br':>9} {'v1-UA Br':>9} {'v2 Br':>9} {'v2-UA Br':>9}")
print(f"{'-'*15} {'-'*8}  {'-'*9} {'-'*9} {'-'*9} {'-'*9}  "
      f"{'-'*9} {'-'*9} {'-'*9} {'-'*9}")

for tier in sorted(eval_df["tier"].unique()):
    tmask = eval_df["tier"] == tier
    y_t = eval_df.loc[tmask, "result_numeric"].values
    n_t = tmask.sum()

    tier_metrics = {}
    for name in ["Elo", "SSM v1-UA", "SSM v2", "SSM v2-UA"]:
        p_t = np.clip(models[name][tmask.values], EPS, 1 - EPS)
        tier_metrics[name] = {
            "ll": log_loss(y_t, p_t),
            "br": brier_score(y_t, p_t),
        }

    print(f"{tier:<15} {n_t:>8}  "
          f"{tier_metrics['Elo']['ll']:9.5f} "
          f"{tier_metrics['SSM v1-UA']['ll']:9.5f} "
          f"{tier_metrics['SSM v2']['ll']:9.5f} "
          f"{tier_metrics['SSM v2-UA']['ll']:9.5f}  "
          f"{tier_metrics['Elo']['br']:9.5f} "
          f"{tier_metrics['SSM v1-UA']['br']:9.5f} "
          f"{tier_metrics['SSM v2']['br']:9.5f} "
          f"{tier_metrics['SSM v2-UA']['br']:9.5f}")

# ---------------------------------------------------------------------------
# 9) In-sample / out-of-sample check
#    Split coaches by whether they had 301+ games BEFORE the test window
#    (i.e. the population the tuner optimised for).
# ---------------------------------------------------------------------------
# A coach was "in the tuned population" if their first game in the test
# window already has coach_game_number > 301 (meaning they had 301+ games
# from training data).
coach_first_game_in_test = (
    eval_df.groupby("coach_id")["ssm2_game_number"].min().reset_index()
)
coach_first_game_in_test.columns = ["coach_id", "first_test_game_number"]

eval_df = eval_df.merge(coach_first_game_in_test, on="coach_id", how="left")
eval_df["tuning_population"] = eval_df["first_test_game_number"].apply(
    lambda g: "In-sample (301+ before test)" if g > 301
    else "Out-of-sample (≤301 before test)"
)

print(f"\n{'='*110}")
print("IN-SAMPLE vs OUT-OF-SAMPLE (tuner targeted 301+ coaches)")
print(f"{'='*110}")
print(f"\n{'Population':<35} {'N':>8}  {'Elo LL':>9} {'v1-UA LL':>9} {'v2-UA LL':>9}  "
      f"{'Elo Br':>9} {'v1-UA Br':>9} {'v2-UA Br':>9}")
print(f"{'-'*35} {'-'*8}  {'-'*9} {'-'*9} {'-'*9}  {'-'*9} {'-'*9} {'-'*9}")

# Re-extract model predictions aligned to eval_df after merge
y_all = eval_df["result_numeric"].values
models_aligned = {
    "Elo":       np.clip(eval_df["elo_pred"].values, EPS, 1 - EPS),
    "SSM v1-UA": np.clip(eval_df["ssm1_ua_pred"].values, EPS, 1 - EPS),
    "SSM v2-UA": np.clip(eval_df["ssm2_ua_pred"].values, EPS, 1 - EPS),
}

for pop in sorted(eval_df["tuning_population"].unique()):
    pmask = (eval_df["tuning_population"] == pop).values
    y_p = y_all[pmask]
    n_p = pmask.sum()

    pop_metrics = {}
    for name in ["Elo", "SSM v1-UA", "SSM v2-UA"]:
        pop_metrics[name] = {
            "ll": log_loss(y_p, models_aligned[name][pmask]),
            "br": brier_score(y_p, models_aligned[name][pmask]),
        }

    print(f"{pop:<35} {n_p:>8}  "
          f"{pop_metrics['Elo']['ll']:9.5f} "
          f"{pop_metrics['SSM v1-UA']['ll']:9.5f} "
          f"{pop_metrics['SSM v2-UA']['ll']:9.5f}  "
          f"{pop_metrics['Elo']['br']:9.5f} "
          f"{pop_metrics['SSM v1-UA']['br']:9.5f} "
          f"{pop_metrics['SSM v2-UA']['br']:9.5f}")

# ---------------------------------------------------------------------------
# 10) Structured summary
# ---------------------------------------------------------------------------
print(f"\n{'='*78}")
print("STRUCTURED SUMMARY")
print(f"{'='*78}")
print(f"""
Test window : date_id >= {eval_cutoff} ({EVAL_TEST_MONTHS} months)
Observations: {len(eval_df)} ({eval_df['game_id'].nunique()} games, {eval_df['coach_id'].nunique()} coaches)

UA method   : Probit approximation — logistic scaled by
              √(1 + π/8 × (ln10/S)² × (σ²_self + σ²_opp))
              Shrinks predictions toward 0.5 when uncertainty is high.
              Coefficient = {UA_COEFF:.6f}

Key findings:
  1. Do the UA variants improve over their base models?
     → Check ΔLL and ΔBr in the main table above.
  2. Is SSM v2's extra complexity (time-awareness, adaptive volatility)
     justified over SSM v1?
     → Compare SSM v1-UA vs SSM v2-UA across all metrics.
  3. Does improvement generalise beyond the tuning population?
     → Check in-sample vs out-of-sample table above.
  4. Does uncertainty-awareness fix the calibration overconfidence?
     → Compare SSM v2 vs SSM v2-UA calibration plots.
""")
print(f"{'='*78}")

# ---------------------------------------------------------------------------
# 11) Export results: text report + plots to folder
# ---------------------------------------------------------------------------
import os
from datetime import datetime

EVAL_OUTPUT_DIR = "/dbfs/FileStore/ssm_evaluation"
os.makedirs(EVAL_OUTPUT_DIR, exist_ok=True)

run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# --- Build text report ---
report_lines = []
report_lines.append("=" * 78)
report_lines.append(f"SSM MODEL COMPARISON REPORT — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
report_lines.append("=" * 78)
report_lines.append("")
report_lines.append(f"Test window : date_id >= {eval_cutoff} ({EVAL_TEST_MONTHS} months)")
report_lines.append(f"Observations: {len(eval_df)} ({eval_df['game_id'].nunique()} games, "
                     f"{eval_df['coach_id'].nunique()} coaches)")
report_lines.append(f"UA coeff    : {UA_COEFF:.6f}")
report_lines.append(f"SSM v1 σ_combined: mean={eval_df['ssm1_combined_sigma'].mean():.1f}, "
                     f"std={eval_df['ssm1_combined_sigma'].std():.1f}, "
                     f"range=[{eval_df['ssm1_combined_sigma'].min():.1f}, "
                     f"{eval_df['ssm1_combined_sigma'].max():.1f}]")
report_lines.append(f"SSM v2 σ_combined: mean={eval_df['ssm2_combined_sigma'].mean():.1f}, "
                     f"std={eval_df['ssm2_combined_sigma'].std():.1f}, "
                     f"range=[{eval_df['ssm2_combined_sigma'].min():.1f}, "
                     f"{eval_df['ssm2_combined_sigma'].max():.1f}]")

# Overall comparison
report_lines.append("")
report_lines.append("=" * 78)
report_lines.append("OVERALL MODEL COMPARISON")
report_lines.append("=" * 78)
report_lines.append("")
report_lines.append(f"{'Model':<15} {'Log-loss':>10} {'Brier':>10} {'Accuracy':>10}  "
                     f"{'ΔLL vs Elo':>11} {'ΔBr vs Elo':>11}")
report_lines.append(f"{'-'*15} {'-'*10} {'-'*10} {'-'*10}  {'-'*11} {'-'*11}")
for name, m in metrics.items():
    ll_d = elo_ll - m["log_loss"]
    br_d = elo_br - m["brier"]
    dll = f"{ll_d:+11.5f}" if name != "Elo" else f"{'—':>11}"
    dbr = f"{br_d:+11.5f}" if name != "Elo" else f"{'—':>11}"
    report_lines.append(f"{name:<15} {m['log_loss']:10.5f} {m['brier']:10.5f} "
                        f"{m['accuracy']:10.3f}  {dll} {dbr}")
report_lines.append("")
report_lines.append("Positive Δ = model is better than Elo (lower loss)")
for ua_name in ["SSM v1-UA", "SSM v2-UA"]:
    ll_imp = (elo_ll - metrics[ua_name]["log_loss"]) / elo_ll * 100
    br_imp = (elo_br - metrics[ua_name]["brier"]) / elo_br * 100
    report_lines.append(f"{ua_name} vs Elo: log-loss {ll_imp:+.3f}%, Brier {br_imp:+.3f}%")

# Per-tier breakdown
report_lines.append("")
report_lines.append("=" * 110)
report_lines.append("METRICS BY EXPERIENCE TIER (unweighted)")
report_lines.append("=" * 110)
report_lines.append("")
report_lines.append(f"{'Tier':<15} {'N':>8}  {'Elo LL':>9} {'v1-UA LL':>9} {'v2 LL':>9} {'v2-UA LL':>9}  "
                     f"{'Elo Br':>9} {'v1-UA Br':>9} {'v2 Br':>9} {'v2-UA Br':>9}")
report_lines.append(f"{'-'*15} {'-'*8}  {'-'*9} {'-'*9} {'-'*9} {'-'*9}  "
                     f"{'-'*9} {'-'*9} {'-'*9} {'-'*9}")
for tier in sorted(eval_df["tier"].unique()):
    tmask = eval_df["tier"] == tier
    y_t = eval_df.loc[tmask, "result_numeric"].values
    n_t = tmask.sum()
    tm = {}
    for name in ["Elo", "SSM v1-UA", "SSM v2", "SSM v2-UA"]:
        p_t = np.clip(models[name][tmask.values], EPS, 1 - EPS)
        tm[name] = {"ll": log_loss(y_t, p_t), "br": brier_score(y_t, p_t)}
    report_lines.append(f"{tier:<15} {n_t:>8}  "
                        f"{tm['Elo']['ll']:9.5f} {tm['SSM v1-UA']['ll']:9.5f} "
                        f"{tm['SSM v2']['ll']:9.5f} {tm['SSM v2-UA']['ll']:9.5f}  "
                        f"{tm['Elo']['br']:9.5f} {tm['SSM v1-UA']['br']:9.5f} "
                        f"{tm['SSM v2']['br']:9.5f} {tm['SSM v2-UA']['br']:9.5f}")

# In-sample / out-of-sample
report_lines.append("")
report_lines.append("=" * 110)
report_lines.append("IN-SAMPLE vs OUT-OF-SAMPLE (tuner targeted 301+ coaches)")
report_lines.append("=" * 110)
report_lines.append("")
report_lines.append(f"{'Population':<35} {'N':>8}  {'Elo LL':>9} {'v1-UA LL':>9} {'v2-UA LL':>9}  "
                     f"{'Elo Br':>9} {'v1-UA Br':>9} {'v2-UA Br':>9}")
report_lines.append(f"{'-'*35} {'-'*8}  {'-'*9} {'-'*9} {'-'*9}  {'-'*9} {'-'*9} {'-'*9}")
for pop in sorted(eval_df["tuning_population"].unique()):
    pmask = (eval_df["tuning_population"] == pop).values
    y_p = y_all[pmask]
    n_p = pmask.sum()
    pm = {}
    for name in ["Elo", "SSM v1-UA", "SSM v2-UA"]:
        pm[name] = {"ll": log_loss(y_p, models_aligned[name][pmask]),
                    "br": brier_score(y_p, models_aligned[name][pmask])}
    report_lines.append(f"{pop:<35} {n_p:>8}  "
                        f"{pm['Elo']['ll']:9.5f} {pm['SSM v1-UA']['ll']:9.5f} "
                        f"{pm['SSM v2-UA']['ll']:9.5f}  "
                        f"{pm['Elo']['br']:9.5f} {pm['SSM v1-UA']['br']:9.5f} "
                        f"{pm['SSM v2-UA']['br']:9.5f}")

report_lines.append("")
report_lines.append("=" * 78)
report_lines.append("END OF REPORT")
report_lines.append("=" * 78)

# Write text report
report_path = os.path.join(EVAL_OUTPUT_DIR, f"model_comparison_{run_timestamp}.txt")
with open(report_path, "w") as f:
    f.write("\n".join(report_lines))
print(f"\nReport saved to: {report_path}")

# --- Save plots ---
# Re-create calibration plot for saving
fig_cal, axes_cal = plt.subplots(1, 4, figsize=(20, 5))
for ax, (name, preds) in zip(axes_cal, cal_models):
    bins = np.arange(0.0, 1.05, 0.1)
    bin_indices = np.digitize(preds, bins) - 1
    bc, ba, bn = [], [], []
    for i in range(len(bins) - 1):
        mask = bin_indices == i
        if mask.sum() >= 20:
            bc.append((bins[i] + bins[i + 1]) / 2)
            ba.append(y[mask].mean())
            bn.append(mask.sum())
    ax.plot([0, 1], [0, 1], "k--", alpha=0.5, label="Perfect calibration")
    ax.bar(bc, ba, width=0.08, alpha=0.6, color="steelblue", label="Actual outcome rate")
    ax.scatter(bc, ba, color="coral", zorder=5, s=40)
    ax.set_xlabel("Predicted win probability")
    ax.set_ylabel("Actual outcome (mean)")
    ax.set_title(f"{name} Calibration")
    ax.set_xlim(0, 1); ax.set_ylim(0, 1)
    ax.legend(loc="lower right", fontsize=7)
    ax.grid(True, alpha=0.3)
    for xc, yc, n in zip(bc, ba, bn):
        ax.text(xc, yc + 0.03, f"n={n}", ha="center", fontsize=5, alpha=0.7)
fig_cal.suptitle("Calibration Comparison", fontsize=14, y=1.02)
fig_cal.tight_layout()
cal_path = os.path.join(EVAL_OUTPUT_DIR, f"calibration_{run_timestamp}.png")
fig_cal.savefig(cal_path, dpi=150, bbox_inches="tight")
plt.close(fig_cal)
print(f"Calibration plot saved to: {cal_path}")

# Re-create uncertainty-stratified Brier for saving
fig_br, ax_br = plt.subplots(figsize=(10, 5))
for j, (name, briers) in enumerate(brier_by_model.items()):
    ax_br.bar(x_pos + (j - 1) * width, briers, width, alpha=0.7,
              color=colors[name], label=name)
ax_br.set_xticks(x_pos)
ax_br.set_xticklabels(sigma_labels, fontsize=8, rotation=15)
ax_br.set_ylabel("Brier score (lower = better)")
ax_br.set_title("Prediction quality by SSM v2 uncertainty level")
ax_br.legend(loc="lower right", fontsize=9)
ax_br.grid(True, alpha=0.3, axis="y")
fig_br.tight_layout()
brier_path = os.path.join(EVAL_OUTPUT_DIR, f"brier_by_sigma_{run_timestamp}.png")
fig_br.savefig(brier_path, dpi=150, bbox_inches="tight")
plt.close(fig_br)
print(f"Brier-by-σ plot saved to: {brier_path}")

print(f"\nAll outputs in: {EVAL_OUTPUT_DIR}")
