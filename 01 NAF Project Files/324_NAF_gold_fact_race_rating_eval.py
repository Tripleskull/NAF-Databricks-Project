# Databricks notebook source
# MAGIC %md
# MAGIC # 324 — Race Rating Evaluation & Tuning
# MAGIC
# MAGIC **Layer:** GOLD_FACT  |  **Status:** Research (not production)
# MAGIC **Pipeline position:** After 310 (config), 320 (Elo + race Elo), 323 (race rating engine)
# MAGIC
# MAGIC Evaluation, tuning, and diagnostics for the race-aware rating model. Run on demand.
# MAGIC Benchmarks against baselines (0.5, global Elo, race Elo) using Brier scores, log-loss,
# MAGIC accuracy, experience-sliced analysis, and calibration plots.
# MAGIC
# MAGIC ## Dependencies
# MAGIC - `gold_dim.analytical_config` (310)
# MAGIC - `gold_fact.game_feed_for_ratings_fact` (320)
# MAGIC - `gold_fact.rating_history_fact` (320) — global Elo + race Elo baselines
# MAGIC - `gold_fact.race_rating_history_fact` (323)
# MAGIC
# MAGIC ## Outputs
# MAGIC - No persistent outputs; produces evaluation metrics, plots, and comparison tables
# MAGIC
# MAGIC **Design authority:** `NAF_Design_Specification.md`, `style_guides.md`
# MAGIC **Design reference:** `00 NAF Project Design/archive/Race_Rating_Design.md`

# COMMAND ----------

# DBTITLE 1,Load Config and Data
# =============================================================================
# COMPONENT: Load analytical config, game feed, and model predictions
# =============================================================================

import math
import datetime as dt
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from collections import defaultdict

_cfg = spark.table("naf_catalog.gold_dim.analytical_config").first()
ELO_SCALE = float(_cfg["elo_scale"])

# ---------------------------------------------------------------------------
# Evaluation windows (same style as 322)
# ---------------------------------------------------------------------------
EVAL_TEST_MONTHS = 6
EVAL_VAL_MONTHS = 6
EVAL_TOTAL_MONTHS = EVAL_TEST_MONTHS + EVAL_VAL_MONTHS

cutoffs = spark.sql(f"""
    SELECT
        CAST(DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -{EVAL_TOTAL_MONTHS}), 'yyyyMMdd') AS INT) AS val_cutoff,
        CAST(DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -{EVAL_TEST_MONTHS}), 'yyyyMMdd') AS INT) AS test_cutoff
""").first()

val_cutoff  = int(cutoffs["val_cutoff"])
test_cutoff = int(cutoffs["test_cutoff"])
# Evaluation uses test window only (val is for tuning)
eval_cutoff = test_cutoff

print(f"Validation window: {val_cutoff} to {test_cutoff} ({EVAL_VAL_MONTHS} months)")
print(f"Test window:       date_id >= {test_cutoff} ({EVAL_TEST_MONTHS} months)")

# COMMAND ----------

# DBTITLE 1,Assemble Evaluation DataFrame
# =============================================================================
# Load predictions from all models and merge into a single evaluation DF.
# Each row = one (game_id, coach_id) observation in the test window.
# =============================================================================

# --- 1. Race-aware model Stage 1 (323) ---
rr1_df = spark.sql(f"""
    SELECT game_id, coach_id, race_id, date_id, game_index,
           coach_game_number,
           score_expected AS rr1_pred,
           result_numeric,
           opponent_coach_id, opponent_race_id
    FROM naf_catalog.gold_fact.race_rating_history_fact
    WHERE date_id >= {eval_cutoff}
""").toPandas()

print(f"Race-aware model Stage 1: {len(rr1_df)} observations in test window")

# --- 2. Race-aware model Stage 2 correlated (323) ---
rr2_table = "naf_catalog.gold_fact.race_rating_corr_history_fact"

if spark.catalog.tableExists(rr2_table):
    rr2_df = spark.sql(f"""
        SELECT game_id, coach_id,
               score_expected AS rr2_pred
        FROM {rr2_table}
        WHERE date_id >= {eval_cutoff}
    """).toPandas()
    print(f"Race-aware model Stage 2: {len(rr2_df)} observations in test window")
    rr2_available = True
else:
    rr2_df = pd.DataFrame(columns=["game_id", "coach_id", "rr2_pred"])
    print("Race-aware model Stage 2: table not found — skipping Stage 2 comparison")
    rr2_available = False

# --- 3. Global Elo (320, GLOBAL scope) ---
elo_global_df = spark.sql(f"""
    SELECT game_id, coach_id,
           score_expected AS elo_global_pred
    FROM naf_catalog.gold_fact.rating_history_fact
    WHERE scope = 'GLOBAL'
      AND date_id >= {eval_cutoff}
""").toPandas()

# --- 4. Race Elo (320, RACE scope) ---
elo_race_df = spark.sql(f"""
    SELECT game_id, coach_id,
           score_expected AS elo_race_pred
    FROM naf_catalog.gold_fact.rating_history_fact
    WHERE scope = 'RACE'
      AND date_id >= {eval_cutoff}
""").toPandas()

# --- 5. Merge ---
eval_df = rr1_df.copy()
eval_df = eval_df.merge(rr2_df, on=["game_id", "coach_id"], how="left")
eval_df = eval_df.merge(elo_global_df, on=["game_id", "coach_id"], how="left")
eval_df = eval_df.merge(elo_race_df, on=["game_id", "coach_id"], how="left")

# --- 6. Add 0.5 baseline ---
eval_df["baseline_pred"] = 0.5

# --- 7. Count prior games per (coach, race) for slicing ---
race_game_counts = spark.sql(f"""
    SELECT coach_id, race_id, coach_game_number AS overall_game_num,
           ROW_NUMBER() OVER (
               PARTITION BY coach_id, race_id
               ORDER BY game_index
           ) AS race_game_number,
           game_id
    FROM naf_catalog.gold_fact.race_rating_history_fact
""").toPandas()

eval_df = eval_df.merge(
    race_game_counts[["game_id", "coach_id", "race_game_number"]],
    on=["game_id", "coach_id"],
    how="left"
)

eval_df["prior_race_games"] = eval_df["race_game_number"] - 1
eval_df["race_experience"] = pd.cut(
    eval_df["prior_race_games"],
    bins=[-1, 0, 5, 25, float("inf")],
    labels=["first_game", "sparse_1_5", "moderate_6_25", "established_25plus"]
)

print(f"\nEvaluation DF: {len(eval_df)} rows")
print(f"  Models: baseline, elo_global, elo_race, race_rating_stage1, race_rating_stage2")
print(f"\nRace experience distribution:")
print(eval_df["race_experience"].value_counts().sort_index())

for col in ["elo_race_pred", "rr1_pred", "rr2_pred"]:
    n_missing = eval_df[col].isna().sum()
    if n_missing > 0:
        print(f"  WARNING: {n_missing} rows missing {col}")

# COMMAND ----------

# DBTITLE 1,Overall Evaluation
# =============================================================================
# Compute Brier score, log-loss, and accuracy for all models.
# =============================================================================

def brier_score(y_true, y_pred):
    return np.mean((y_pred - y_true) ** 2)

def log_loss(y_true, y_pred, eps=1e-10):
    p = np.clip(y_pred, eps, 1.0 - eps)
    return -np.mean(y_true * np.log(p) + (1.0 - y_true) * np.log(1.0 - p))

def accuracy(y_true, y_pred):
    pred_win = y_pred > 0.5
    actual_win = y_true > 0.5
    draws = y_true == 0.5
    return (pred_win == actual_win).sum() / max(1, len(y_true) - draws.sum())

y = eval_df["result_numeric"].values

models = {
    "0.5 baseline":   eval_df["baseline_pred"].values,
    "Global Elo":     eval_df["elo_global_pred"].values,
    "Race Elo":       eval_df["elo_race_pred"].values,
    "Race Rating S1": eval_df["rr1_pred"].values,
}

if rr2_available and "rr2_pred" in eval_df.columns and eval_df["rr2_pred"].notna().any():
    models["Race Rating Corr"] = eval_df["rr2_pred"].values

# Compute metrics
metrics = {}
for name, preds in models.items():
    mask = ~np.isnan(preds)
    y_m = y[mask]
    p_m = preds[mask]
    metrics[name] = {
        "brier": brier_score(y_m, p_m),
        "log_loss": log_loss(y_m, p_m),
        "accuracy": accuracy(y_m, p_m),
        "n": int(mask.sum()),
    }

# Display
elo_br = metrics["Global Elo"]["brier"]
print(f"{'='*78}")
print(f"OVERALL MODEL COMPARISON (test set, date_id >= {eval_cutoff})")
print(f"{'='*78}")
print(f"{'Model':<18} {'Brier':>10} {'Log-loss':>10} {'Accuracy':>10}  "
      f"{'dBr vs Elo':>11} {'n':>8}")

for name, m in metrics.items():
    br_d = elo_br - m["brier"]
    dbr = f"{br_d:+11.5f}" if name != "Global Elo" else f"{'---':>11}"
    print(f"{name:<18} {m['brier']:10.5f} {m['log_loss']:10.5f} "
          f"{m['accuracy']:10.3f}  {dbr} {m['n']:8d}")

# COMMAND ----------

# DBTITLE 1,Sliced Evaluation (Race Experience)
# =============================================================================
# Evaluate each model within race-experience slices.
# This tests whether the model helps across race-history depth.
# =============================================================================

print(f"{'='*110}")
print("BRIER SCORE BY RACE EXPERIENCE SLICE")
print(f"{'='*110}")

slices = ["first_game", "sparse_1_5", "moderate_6_25", "established_25plus"]
model_names = list(models.keys())

# Header
hdr = f"{'Slice':<22} {'n':>7}"
for name in model_names:
    hdr += f"  {name:>16}"
print(hdr)
print("-" * len(hdr))

for sl in slices:
    mask = eval_df["race_experience"] == sl
    n_sl = int(mask.sum())
    if n_sl == 0:
        continue

    y_sl = y[mask]
    line = f"{sl:<22} {n_sl:>7}"

    for name, preds in models.items():
        p_sl = preds[mask]
        valid = ~np.isnan(p_sl)
        if valid.sum() > 0:
            br = brier_score(y_sl[valid], p_sl[valid])
            line += f"  {br:>16.5f}"
        else:
            line += f"  {'N/A':>16}"
    print(line)

print("\n--- Key comparisons ---")
for sl in slices:
    mask = eval_df["race_experience"] == sl
    if mask.sum() == 0:
        continue

    y_sl = y[mask]
    rr1 = eval_df["rr1_pred"].values[mask]
    rr2 = eval_df["rr2_pred"].values[mask]
    re  = eval_df["elo_race_pred"].values[mask]
    eg  = eval_df["elo_global_pred"].values[mask]

    valid = ~(np.isnan(rr1) | np.isnan(rr2) | np.isnan(re) | np.isnan(eg))
    if valid.sum() == 0:
        continue

    br_rr1 = brier_score(y_sl[valid], rr1[valid])
    br_rr2 = brier_score(y_sl[valid], rr2[valid])
    br_re  = brier_score(y_sl[valid], re[valid])
    br_eg  = brier_score(y_sl[valid], eg[valid])

    print(
        f"  {sl:<18}"
        f" S2-S1 = {br_rr1 - br_rr2:+.5f}"
        f" | RaceElo-S2 = {br_re - br_rr2:+.5f}"
        f" | GlobalElo-S2 = {br_eg - br_rr2:+.5f}"
    )

# COMMAND ----------

# DBTITLE 1,Calibration Plots
# =============================================================================
# Calibration comparison
# =============================================================================

cal_models = [
    ("0.5 baseline", eval_df["baseline_pred"].values),
    ("Global Elo", eval_df["elo_global_pred"].values),
    ("Race Elo", eval_df["elo_race_pred"].values),
    ("Race Rating S1", eval_df["rr1_pred"].values),
]

if "Race Rating Corr" in models:
    cal_models.append(("Race Rating Corr", eval_df["rr2_pred"].values))

n_panels = len(cal_models)
fig, axes = plt.subplots(1, n_panels, figsize=(5 * n_panels, 5))

if n_panels == 1:
    axes = [axes]

for ax, (name, preds) in zip(axes, cal_models):
    valid = ~np.isnan(preds)
    p_v = preds[valid]
    y_v = y[valid]

    bins = np.arange(0.0, 1.05, 0.1)
    bin_idx = np.digitize(p_v, bins) - 1
    bc, ba, bn = [], [], []

    for i in range(len(bins) - 1):
        m = bin_idx == i
        if m.sum() >= 20:
            bc.append((bins[i] + bins[i + 1]) / 2)
            ba.append(y_v[m].mean())
            bn.append(int(m.sum()))

    ax.plot([0, 1], [0, 1], "k--", alpha=0.5)
    ax.bar(bc, ba, width=0.08, alpha=0.6)
    ax.scatter(bc, ba, zorder=5, s=40)

    for x, y_bin, n_bin in zip(bc, ba, bn):
        ax.text(x, y_bin + 0.03, str(n_bin), ha="center", va="bottom", fontsize=8)

    ax.set_xlabel("Predicted win probability")
    ax.set_ylabel("Observed mean result")
    ax.set_title(name)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.grid(True, alpha=0.3)

fig.suptitle("Race Rating — Calibration Comparison", fontsize=14, y=1.02)
fig.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Export Results
# =============================================================================
# Store evaluation results — Delta table + CSV + text report
# =============================================================================

import os

run_timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")

# --- Delta table ---
metrics_rows = []
for name, m in metrics.items():
    metrics_rows.append({
        "run_timestamp": run_timestamp,
        "model_name": name,
        "brier_score": float(m["brier"]),
        "log_loss": float(m["log_loss"]),
        "accuracy": float(m["accuracy"]),
        "n_observations": int(m["n"]),
        "test_cutoff_date_id": eval_cutoff,
        "test_months": EVAL_TEST_MONTHS,
    })

metrics_spark_df = spark.createDataFrame(metrics_rows)

spark.sql("""
    CREATE TABLE IF NOT EXISTS naf_catalog.gold_summary.race_rating_evaluation_metrics (
        run_timestamp STRING,
        model_name STRING,
        brier_score DOUBLE,
        log_loss DOUBLE,
        accuracy DOUBLE,
        n_observations BIGINT,
        test_cutoff_date_id INT,
        test_months INT
    )
    USING DELTA
""")

metrics_spark_df.write.mode("append").saveAsTable(
    "naf_catalog.gold_summary.race_rating_evaluation_metrics"
)
print("✓ Metrics stored in naf_catalog.gold_summary.race_rating_evaluation_metrics")

# --- CSV + report ---
spark.sql("CREATE VOLUME IF NOT EXISTS naf_catalog.gold_summary.exports")
model_tests_dir = "/Volumes/naf_catalog/gold_summary/exports/model_tests"
run_dir = f"{model_tests_dir}/race_eval_{run_timestamp}"
os.makedirs(run_dir, exist_ok=True)

# CSV
metrics_spark_df.coalesce(1).write.mode("overwrite").option(
    "header", "true"
).csv(f"{run_dir}/_csv_tmp")
part_files = [
    f.path for f in dbutils.fs.ls(f"{run_dir}/_csv_tmp")
    if f.name.startswith("part-") and f.name.endswith(".csv")
]
if part_files:
    dbutils.fs.cp(part_files[0], f"{run_dir}/metrics.csv", True)
dbutils.fs.rm(f"{run_dir}/_csv_tmp", recurse=True)
print(f"✓ CSV: {run_dir}/metrics.csv")

# Text report
report_lines = []
report_lines.append("RACE RATING MODEL EVALUATION REPORT")
report_lines.append(f"Run: {run_timestamp}")
report_lines.append(f"Test: date_id >= {eval_cutoff} ({EVAL_TEST_MONTHS} months)")
report_lines.append(f"Observations: {len(eval_df)}")
report_lines.append("")

# --- Overall comparison ---
report_lines.append(f"{'='*110}")
report_lines.append("OVERALL COMPARISON")
report_lines.append(f"{'='*110}")
report_lines.append(
    f"{'Model':<18} {'Brier':>10} {'Log-loss':>10} {'Accuracy':>10}  {'dBr vs Global Elo':>16}"
)

global_elo_br = metrics["Global Elo"]["brier"]

for name, m in metrics.items():
    br_d = global_elo_br - m["brier"]
    dbr = f"{br_d:+16.5f}" if name != "Global Elo" else f"{'---':>16}"
    report_lines.append(
        f"{name:<18} {m['brier']:10.5f} {m['log_loss']:10.5f} "
        f"{m['accuracy']:10.3f}  {dbr}"
    )
report_lines.append("")

# --- Sliced evaluation ---
report_lines.append(f"{'='*110}")
report_lines.append("BRIER SCORE BY RACE EXPERIENCE")
report_lines.append(f"{'='*110}")

slice_hdr = f"{'Slice':<22} {'n':>7}"
for name in models:
    slice_hdr += f"  {name:>16}"
report_lines.append(slice_hdr)
report_lines.append("-" * len(slice_hdr))

for sl in ["first_game", "sparse_1_5", "moderate_6_25", "established_25plus"]:
    mask = eval_df["race_experience"] == sl
    n_sl = int(mask.sum())
    if n_sl == 0:
        continue

    y_sl = y[mask]
    line = f"{sl:<22} {n_sl:>7}"

    for name, preds in models.items():
        p_sl = preds[mask]
        valid = ~np.isnan(p_sl)
        if valid.sum() > 0:
            br = brier_score(y_sl[valid], p_sl[valid])
            line += f"  {br:>16.5f}"
        else:
            line += f"  {'N/A':>16}"

    report_lines.append(line)

report_lines.append("")

# --- Key comparisons ---
if "Race Rating Corr" in models and "rr2_pred" in eval_df.columns:
    report_lines.append("KEY COMPARISONS")
    for sl in ["first_game", "sparse_1_5", "moderate_6_25", "established_25plus"]:
        mask = eval_df["race_experience"] == sl
        if mask.sum() == 0:
            continue

        y_sl = y[mask]
        rr1 = eval_df["rr1_pred"].values[mask]
        rr2 = eval_df["rr2_pred"].values[mask]
        re  = eval_df["elo_race_pred"].values[mask]
        eg  = eval_df["elo_global_pred"].values[mask]

        valid = ~(np.isnan(rr1) | np.isnan(rr2) | np.isnan(re) | np.isnan(eg))
        if valid.sum() > 0:
            br_rr1 = brier_score(y_sl[valid], rr1[valid])
            br_rr2 = brier_score(y_sl[valid], rr2[valid])
            br_re  = brier_score(y_sl[valid], re[valid])
            br_eg  = brier_score(y_sl[valid], eg[valid])

            report_lines.append(
                f"  {sl:<22}  "
                f"S2-S1: {br_rr1 - br_rr2:+.5f}  "
                f"RaceElo-S2: {br_re - br_rr2:+.5f}  "
                f"GlobalElo-S2: {br_eg - br_rr2:+.5f}"
            )
    report_lines.append("")
else:
    report_lines.append("KEY COMPARISONS")
    report_lines.append("  Stage 2 correlated model not available in this run.")
    report_lines.append("")

# --- Parameters ---
report_lines.append("PARAMETERS — STAGE 1")
report_lines.append(f"  prior_sigma_g = {float(_cfg['rr_prior_sigma_g'])}")
report_lines.append(f"  prior_sigma_d = {float(_cfg['rr_prior_sigma_d'])}")
report_lines.append(f"  sigma2_obs    = {float(_cfg['rr_sigma2_obs'])}")
report_lines.append(f"  q_global      = {float(_cfg['rr_q_global'])}")
report_lines.append(f"  q_race        = {float(_cfg['rr_q_race'])}")
report_lines.append("")

if "Race Rating Corr" in models:
    report_lines.append("PARAMETERS — STAGE 2")
    report_lines.append(f"  prior_sigma_g        = {float(_cfg['rr2_prior_sigma_g'])}")
    report_lines.append(f"  prior_sigma_d        = {float(_cfg['rr2_prior_sigma_d'])}")
    report_lines.append(f"  sigma2_obs           = {float(_cfg['rr2_sigma2_obs'])}")
    report_lines.append(f"  q_global             = {float(_cfg['rr2_q_global'])}")
    report_lines.append(f"  q_race               = {float(_cfg['rr2_q_race'])}")
    report_lines.append(f"  cov_shrinkage_lambda = {float(_cfg['rr2_cov_shrinkage_lambda'])}")
    report_lines.append(f"  cov_min_games_race   = {int(_cfg['rr2_cov_min_games_per_race'])}")
    report_lines.append(f"  cov_min_overlap      = {int(_cfg['rr2_cov_min_overlap_coaches'])}")
    report_lines.append(f"  cov_eigen_floor      = {float(_cfg['rr2_cov_eigen_floor'])}")
else:
    report_lines.append("PARAMETERS — STAGE 2")
    report_lines.append("  Not reported because Stage 2 was not available in this run.")

report_lines.append("")

report_text = "\n".join(report_lines)

report_path = f"{run_dir}/evaluation_report.txt"
with open(report_path, "w") as f:
    f.write(report_text)
print(f"✓ Report: {report_path}")

# Calibration plot
fig.savefig(f"{run_dir}/calibration_plots.png", dpi=150, bbox_inches="tight")
print(f"✓ Plot: {run_dir}/calibration_plots.png")

print(f"\n{'='*110}")
print(f"EXPORT COMPLETE — {run_dir}")
print(f"{'='*110}")

# COMMAND ----------

# DBTITLE 1,Race Rating Tuner (validation-window, stage 1)
# =============================================================================
# PURPOSE
#   Tune the stage-1 race-aware EKF on the validation window [val_cutoff, test_cutoff)
#   using chronological replay of the full game feed.
#
# MODEL PARAMS
#   rr_prior_sigma_g
#   rr_prior_sigma_d
#   rr_sigma2_obs
#   rr_q_global
#   rr_q_race
#
# OBJECTIVE
#   Overall validation Brier score
#
# NOTES
#   - Uses the same validation window already defined in 324:
#       val_cutoff <= date_id < test_cutoff
#   - Replays ALL games in order, but only scores games in the validation window.
#   - This is an in-memory research tuner; it does NOT overwrite production tables
#     unless you explicitly save the tuning results at the end.
# =============================================================================

import math
import time
import itertools
import numpy as np
import pandas as pd
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# 0) Shared setup
# ---------------------------------------------------------------------------
_cfg = spark.table("naf_catalog.gold_dim.analytical_config").first()

RR_INITIAL_RATING   = float(_cfg["elo_initial_rating"])
RR_ELO_SCALE        = float(_cfg["elo_scale"])
RR_LN10_OVER_SCALE  = math.log(10.0) / RR_ELO_SCALE
RR_MIN_VAR          = 1e-6
RR_EPS              = 1e-10

# Same windows as 324
TUNE_VAL_START = val_cutoff
TUNE_VAL_END   = test_cutoff

print(f"Validation window: [{TUNE_VAL_START}, {TUNE_VAL_END})")

# ---------------------------------------------------------------------------
# 1) Load feed once + fail-fast validation
# ---------------------------------------------------------------------------
rr_feed_df = (
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

bad_rr_feed = rr_feed_df.filter(
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

if len(bad_rr_feed.take(1)) > 0:
    raise ValueError("Race tuning feed: invalid rows detected in game_feed_for_ratings_fact.")

rr_feed_rows = rr_feed_df.collect()
print(f"Race tuning feed loaded: {len(rr_feed_rows)} games")

# ---------------------------------------------------------------------------
# 2) Helper functions
# ---------------------------------------------------------------------------
def rr_win_prob(theta_self, theta_opp):
    return 1.0 / (1.0 + 10.0 ** ((theta_opp - theta_self) / RR_ELO_SCALE))

def rr_brier(y_true, y_pred):
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    if len(y_true) == 0:
        return float("nan")
    return float(np.mean((y_pred - y_true) ** 2))

def rr_log_loss(y_true, y_pred, eps=RR_EPS):
    """
    Kept consistent with 322/324:
    - works on result_numeric in {0, 0.5, 1}
    - clips probabilities for numerical stability
    """
    y_true = np.asarray(y_true, dtype=float)
    p = np.clip(np.asarray(y_pred, dtype=float), eps, 1.0 - eps)
    if len(y_true) == 0:
        return float("nan")
    return float(-np.mean(y_true * np.log(p) + (1.0 - y_true) * np.log(1.0 - p)))

def rr_accuracy(y_true, y_pred):
    """
    Match the 322 style:
    - exclude draws (0.5) from accuracy
    - score win/loss only
    """
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)

    mask = y_true != 0.5
    if mask.sum() == 0:
        return float("nan")

    correct = (
        ((y_pred[mask] > 0.5) & (y_true[mask] == 1.0)) |
        ((y_pred[mask] < 0.5) & (y_true[mask] == 0.0))
    )
    return float(correct.mean())

def rr_exp_slice(prior_race_games):
    if prior_race_games == 0:
        return "first_game"
    if 1 <= prior_race_games <= 5:
        return "sparse_1_5"
    if 6 <= prior_race_games <= 25:
        return "moderate_6_25"
    return "established_25plus"

# ---------------------------------------------------------------------------
# 3) Reusable race-engine runner
# ---------------------------------------------------------------------------
def run_rr_variant(
    prior_sigma_g,
    prior_sigma_d,
    sigma2_obs,
    q_global,
    q_race,
    eval_cutoff_date_id=None,
):
    """
    Run stage-1 race-aware EKF in memory for one parameter variant.

    Replays the full game history in order, but only returns rows with
    date_id >= eval_cutoff_date_id if that cutoff is provided.

    Returns
    -------
    pandas.DataFrame with one row per (game_id, coach_id), including:
      game_id, coach_id, date_id, game_index,
      race_id, coach_game_number, prior_race_games, race_experience,
      rr_pred, result_numeric
    """
    prior_p_g = float(prior_sigma_g) ** 2
    prior_p_d = float(prior_sigma_d) ** 2

    g_state = {}            # coach_id -> [g, P_g]
    d_state = {}            # (coach_id, race_id) -> [d, P_d]
    coach_game_counts = {}  # coach_id -> total games seen so far
    coach_race_counts = {}  # (coach_id, race_id) -> games with that race seen so far

    def get_g(coach_id):
        if coach_id not in g_state:
            g_state[coach_id] = [RR_INITIAL_RATING, prior_p_g]
        return g_state[coach_id]

    def get_d(coach_id, race_id):
        key = (coach_id, race_id)
        if key not in d_state:
            d_state[key] = [0.0, prior_p_d]
        return d_state[key]

    out_rows = []

    for row in rr_feed_rows:
        game_id    = int(row["game_id"])
        game_index = int(row["game_index"])
        date_id    = int(row["date_id"])

        c_h = int(row["home_coach_id"])
        c_a = int(row["away_coach_id"])
        r_h = int(row["home_race_id"])
        r_a = int(row["away_race_id"])

        res_h = float(row["result_home"])
        res_a = float(row["result_away"])

        # --- counts BEFORE this game ---
        prior_games_h = coach_game_counts.get(c_h, 0)
        prior_games_a = coach_game_counts.get(c_a, 0)

        prior_race_games_h = coach_race_counts.get((c_h, r_h), 0)
        prior_race_games_a = coach_race_counts.get((c_a, r_a), 0)

        coach_game_number_h = prior_games_h + 1
        coach_game_number_a = prior_games_a + 1

        race_exp_h = rr_exp_slice(prior_race_games_h)
        race_exp_a = rr_exp_slice(prior_race_games_a)

        # --- get states ---
        g_h, Pg_h = get_g(c_h)
        g_a, Pg_a = get_g(c_a)

        d_h, Pd_h = get_d(c_h, r_h)
        d_a, Pd_a = get_d(c_a, r_a)

        # --- prediction step (stage 1 independent global + played-race deviation) ---
        Pg_h_pred = Pg_h + q_global
        Pg_a_pred = Pg_a + q_global
        Pd_h_pred = Pd_h + q_race
        Pd_a_pred = Pd_a + q_race

        theta_h = g_h + d_h
        theta_a = g_a + d_a

        p_h = rr_win_prob(theta_h, theta_a)
        p_a = 1.0 - p_h

        h_val = RR_LN10_OVER_SCALE * p_h * (1.0 - p_h)

        innov_h = res_h - p_h
        innov_a = res_a - p_a

        # --- home update ---
        S_h = h_val * h_val * (Pg_h_pred + Pd_h_pred) + sigma2_obs
        if S_h < 1e-12:
            S_h = 1e-12

        Kg_h = h_val * Pg_h_pred / S_h
        Kd_h = h_val * Pd_h_pred / S_h

        g_h_post  = g_h + Kg_h * innov_h
        d_h_post  = d_h + Kd_h * innov_h
        Pg_h_post = max(RR_MIN_VAR, (1.0 - Kg_h * h_val) * Pg_h_pred)
        Pd_h_post = max(RR_MIN_VAR, (1.0 - Kd_h * h_val) * Pd_h_pred)

        # --- away update ---
        S_a = h_val * h_val * (Pg_a_pred + Pd_a_pred) + sigma2_obs
        if S_a < 1e-12:
            S_a = 1e-12

        Kg_a = h_val * Pg_a_pred / S_a
        Kd_a = h_val * Pd_a_pred / S_a

        g_a_post  = g_a + Kg_a * innov_a
        d_a_post  = d_a + Kd_a * innov_a
        Pg_a_post = max(RR_MIN_VAR, (1.0 - Kg_a * h_val) * Pg_a_pred)
        Pd_a_post = max(RR_MIN_VAR, (1.0 - Kd_a * h_val) * Pd_a_pred)

        # --- emit rows only from cutoff onward ---
        if eval_cutoff_date_id is None or date_id >= eval_cutoff_date_id:
            out_rows.append((
                game_id, c_h, date_id, game_index, r_h,
                coach_game_number_h, prior_race_games_h, race_exp_h,
                p_h, res_h
            ))
            out_rows.append((
                game_id, c_a, date_id, game_index, r_a,
                coach_game_number_a, prior_race_games_a, race_exp_a,
                p_a, res_a
            ))

        # --- persist posterior states ---
        g_state[c_h] = [g_h_post, Pg_h_post]
        g_state[c_a] = [g_a_post, Pg_a_post]
        d_state[(c_h, r_h)] = [d_h_post, Pd_h_post]
        d_state[(c_a, r_a)] = [d_a_post, Pd_a_post]

        coach_game_counts[c_h] = coach_game_number_h
        coach_game_counts[c_a] = coach_game_number_a
        coach_race_counts[(c_h, r_h)] = prior_race_games_h + 1
        coach_race_counts[(c_a, r_a)] = prior_race_games_a + 1

    out_df = pd.DataFrame(
        out_rows,
        columns=[
            "game_id", "coach_id", "date_id", "game_index", "race_id",
            "coach_game_number", "prior_race_games", "race_experience",
            "rr_pred", "result_numeric"
        ],
    )

    if len(out_df) > 0:
        out_df["rr_pred"] = np.clip(out_df["rr_pred"].values, RR_EPS, 1.0 - RR_EPS)

    return out_df

# ---------------------------------------------------------------------------
# 4) Validation scorer
# ---------------------------------------------------------------------------
def compute_rr_objective(engine_out, val_start, val_end):
    """
    Score a race-model variant on the validation window.

    Objective:
      overall validation Brier only
    """
    df = engine_out[
        (engine_out["date_id"] >= val_start) &
        (engine_out["date_id"] <  val_end)
    ].copy()

    if len(df) == 0:
        return {
            "objective": np.inf,
            "brier_overall": np.nan,
            "logloss_overall": np.nan,
            "accuracy_overall": np.nan,
            "n_obs": 0,
            "brier_by_slice": {},
        }

    y = df["result_numeric"].to_numpy(dtype=float)
    p = df["rr_pred"].to_numpy(dtype=float)

    brier_overall = rr_brier(y, p)
    logloss_overall = rr_log_loss(y, p)
    accuracy_overall = rr_accuracy(y, p)

    brier_by_slice = {}
    for sl in ["first_game", "sparse_1_5", "moderate_6_25", "established_25plus"]:
        m = (df["race_experience"] == sl).values
        if m.sum() > 0:
            y_sl = df.loc[m, "result_numeric"].to_numpy(dtype=float)
            p_sl = df.loc[m, "rr_pred"].to_numpy(dtype=float)
            brier_by_slice[sl] = {
                "brier": rr_brier(y_sl, p_sl),
                "logloss": rr_log_loss(y_sl, p_sl),
                "accuracy": rr_accuracy(y_sl, p_sl),
                "n": int(m.sum()),
            }

    return {
        "objective": float(brier_overall),
        "brier_overall": brier_overall,
        "logloss_overall": logloss_overall,
        "accuracy_overall": accuracy_overall,
        "n_obs": int(len(df)),
        "brier_by_slice": brier_by_slice,
    }

# ---------------------------------------------------------------------------
# 5) Grid definitions
# ---------------------------------------------------------------------------
RR_COARSE_GRID = {
    "prior_sigma_g": [40.0, 50.0, 60.0],
    "prior_sigma_d": [15.0, 30.0, 45.0],
    "sigma2_obs":    [0.03, 0.10, 0.20],
    "q_global":      [0.00, 0.25, 0.50, 1.00],
    "q_race":        [0.05, 0.25, 0.50, 1.00],
}

def make_rr_fine_grid(best_params):
    """
    3-point neighbourhood around each best coarse parameter.
    """
    def _neighbourhood(val, candidates):
        candidates = sorted(candidates)
        idx = min(range(len(candidates)), key=lambda i: abs(candidates[i] - val))

        if len(candidates) == 1:
            return [round(val, 6)]

        if idx == 0:
            step = (candidates[1] - candidates[0]) / 2.0
        elif idx == len(candidates) - 1:
            step = (candidates[-1] - candidates[-2]) / 2.0
        else:
            step = min(
                candidates[idx] - candidates[idx - 1],
                candidates[idx + 1] - candidates[idx]
            ) / 2.0

        lo_floor = min(candidates) * 0.5 if min(candidates) > 0 else 0.0
        lo = max(val - step, lo_floor)
        hi = val + step
        return sorted(set([round(lo, 6), round(val, 6), round(hi, 6)]))

    return {
        "prior_sigma_g": _neighbourhood(best_params["prior_sigma_g"], RR_COARSE_GRID["prior_sigma_g"]),
        "prior_sigma_d": _neighbourhood(best_params["prior_sigma_d"], RR_COARSE_GRID["prior_sigma_d"]),
        "sigma2_obs":    _neighbourhood(best_params["sigma2_obs"],    RR_COARSE_GRID["sigma2_obs"]),
        "q_global":      _neighbourhood(best_params["q_global"],      RR_COARSE_GRID["q_global"]),
        "q_race":        _neighbourhood(best_params["q_race"],        RR_COARSE_GRID["q_race"]),
    }

# ---------------------------------------------------------------------------
# 6) Grid search runner
# ---------------------------------------------------------------------------
def run_rr_grid(grid, val_start, val_end, label="Race Grid"):
    keys = list(grid.keys())
    combos = list(itertools.product(*[grid[k] for k in keys]))

    print(f"\n{'='*96}")
    print(f"{label}: {len(combos)} parameter combinations")
    print("Objective: overall validation Brier")
    print(f"Validation window: date_id ∈ [{val_start}, {val_end})")
    print(f"{'='*96}")

    all_results = []
    t0 = time.time()

    for i, vals in enumerate(combos, start=1):
        params = dict(zip(keys, vals))
        t_start = time.time()

        engine_out = run_rr_variant(
            prior_sigma_g=params["prior_sigma_g"],
            prior_sigma_d=params["prior_sigma_d"],
            sigma2_obs=params["sigma2_obs"],
            q_global=params["q_global"],
            q_race=params["q_race"],
            eval_cutoff_date_id=val_start,
        )

        scores = compute_rr_objective(engine_out, val_start, val_end)
        scores["params"] = params
        all_results.append(scores)

        elapsed = time.time() - t_start
        total_elapsed = time.time() - t0
        avg_per = total_elapsed / i
        remaining = avg_per * (len(combos) - i)

        if (i % 5 == 0) or (i == len(combos)):
            sl = scores["brier_by_slice"]
            fg = sl.get("first_game", {}).get("brier", np.nan)
            sp = sl.get("sparse_1_5", {}).get("brier", np.nan)

            print(
                f"[{i:>3}/{len(combos)}] "
                f"obj={scores['objective']:.5f}  "
                f"ll={scores['logloss_overall']:.5f}  "
                f"acc={scores['accuracy_overall']:.4f}  "
                f"fg={fg:.5f}  sp={sp:.5f}  "
                f"σg={params['prior_sigma_g']:.2f}  "
                f"σd={params['prior_sigma_d']:.2f}  "
                f"σ²={params['sigma2_obs']:.3f}  "
                f"qg={params['q_global']:.3f}  "
                f"qr={params['q_race']:.3f}  "
                f"({elapsed:.1f}s, ~{remaining/60:.1f}m left)"
            )

    res_df = pd.DataFrame([
        {
            **r["params"],
            "objective": r["objective"],
            "brier_overall": r["brier_overall"],
            "logloss_overall": r["logloss_overall"],
            "accuracy_overall": r["accuracy_overall"],
            "n_obs": r["n_obs"],

            "brier_first_game": r["brier_by_slice"].get("first_game", {}).get("brier", np.nan),
            "brier_sparse_1_5": r["brier_by_slice"].get("sparse_1_5", {}).get("brier", np.nan),
            "brier_moderate_6_25": r["brier_by_slice"].get("moderate_6_25", {}).get("brier", np.nan),
            "brier_established_25plus": r["brier_by_slice"].get("established_25plus", {}).get("brier", np.nan),

            "logloss_first_game": r["brier_by_slice"].get("first_game", {}).get("logloss", np.nan),
            "logloss_sparse_1_5": r["brier_by_slice"].get("sparse_1_5", {}).get("logloss", np.nan),
            "logloss_moderate_6_25": r["brier_by_slice"].get("moderate_6_25", {}).get("logloss", np.nan),
            "logloss_established_25plus": r["brier_by_slice"].get("established_25plus", {}).get("logloss", np.nan),

            "accuracy_first_game": r["brier_by_slice"].get("first_game", {}).get("accuracy", np.nan),
            "accuracy_sparse_1_5": r["brier_by_slice"].get("sparse_1_5", {}).get("accuracy", np.nan),
            "accuracy_moderate_6_25": r["brier_by_slice"].get("moderate_6_25", {}).get("accuracy", np.nan),
            "accuracy_established_25plus": r["brier_by_slice"].get("established_25plus", {}).get("accuracy", np.nan),

            "n_first_game": r["brier_by_slice"].get("first_game", {}).get("n", 0),
            "n_sparse_1_5": r["brier_by_slice"].get("sparse_1_5", {}).get("n", 0),
            "n_moderate_6_25": r["brier_by_slice"].get("moderate_6_25", {}).get("n", 0),
            "n_established_25plus": r["brier_by_slice"].get("established_25plus", {}).get("n", 0),
        }
        for r in all_results
    ]).sort_values(
        ["objective", "logloss_overall", "accuracy_overall"],
        ascending=[True, True, False]
    ).reset_index(drop=True)

    return res_df, all_results

def print_rr_best_summary(best_row, label="FINAL BEST RACE PARAMETERS"):
    print(f"\n{'='*96}")
    print(label)
    print(f"{'='*96}")
    print(f"prior_sigma_g   = {best_row['prior_sigma_g']:.6f}")
    print(f"prior_sigma_d   = {best_row['prior_sigma_d']:.6f}")
    print(f"sigma2_obs      = {best_row['sigma2_obs']:.6f}")
    print(f"q_global        = {best_row['q_global']:.6f}")
    print(f"q_race          = {best_row['q_race']:.6f}")
    print("")
    print(f"objective       = {best_row['objective']:.6f}")
    print(f"brier_overall   = {best_row['brier_overall']:.6f}")
    print(f"logloss_overall = {best_row['logloss_overall']:.6f}")
    print(f"accuracy_overall= {best_row['accuracy_overall']:.6f}")
    print(f"n_obs           = {int(best_row['n_obs'])}")
    print("")
    print(f"brier_first     = {best_row['brier_first_game']:.6f}")
    print(f"brier_sparse    = {best_row['brier_sparse_1_5']:.6f}")
    print(f"brier_moderate  = {best_row['brier_moderate_6_25']:.6f}")
    print(f"brier_estab     = {best_row['brier_established_25plus']:.6f}")

# ---------------------------------------------------------------------------
# 7) Run tuning
# ---------------------------------------------------------------------------
# --- coarse pass ---
RUN_TUNING_RESULTS = False

if RUN_TUNING_RESULTS:
    rr_coarse_df, rr_coarse_raw = run_rr_grid(
        RR_COARSE_GRID,
        TUNE_VAL_START,
        TUNE_VAL_END,
        label="RACE COARSE GRID"
    )
    display(rr_coarse_df.head(20))

    best_coarse = rr_coarse_df.iloc[0].to_dict()

    print("\nBest coarse params:")
    print({
        k: best_coarse[k]
        for k in ["prior_sigma_g", "prior_sigma_d", "sigma2_obs", "q_global", "q_race"]
    })

    # --- fine pass ---
    RR_FINE_GRID = make_rr_fine_grid(best_coarse)

    print("\nFine grid:")
    for k, v in RR_FINE_GRID.items():
        print(f"  {k}: {v}")

    rr_fine_df, rr_fine_raw = run_rr_grid(
        RR_FINE_GRID,
        TUNE_VAL_START,
        TUNE_VAL_END,
        label="RACE FINE GRID"
    )
    display(rr_fine_df.head(20))

    best_fine = rr_fine_df.iloc[0]
    print_rr_best_summary(best_fine, label="FINAL BEST RACE PARAMETERS (validation objective)")

# ---------------------------------------------------------------------------
# 8) Optional: save tuning results for later inspection
# ---------------------------------------------------------------------------
SAVE_RR_TUNING_RESULTS = False

if SAVE_RR_TUNING_RESULTS:
    run_timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")

    coarse_save = rr_coarse_df.copy()
    coarse_save["search_stage"] = "coarse"
    coarse_save["run_timestamp"] = run_timestamp
    coarse_save["val_start"] = TUNE_VAL_START
    coarse_save["val_end"] = TUNE_VAL_END

    fine_save = rr_fine_df.copy()
    fine_save["search_stage"] = "fine"
    fine_save["run_timestamp"] = run_timestamp
    fine_save["val_start"] = TUNE_VAL_START
    fine_save["val_end"] = TUNE_VAL_END

    rr_tuning_save_df = pd.concat([coarse_save, fine_save], ignore_index=True)
    rr_tuning_spark_df = spark.createDataFrame(rr_tuning_save_df)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS naf_catalog.gold_summary.race_rating_tuning_results (
            prior_sigma_g DOUBLE,
            prior_sigma_d DOUBLE,
            sigma2_obs DOUBLE,
            q_global DOUBLE,
            q_race DOUBLE,
            objective DOUBLE,
            brier_overall DOUBLE,
            logloss_overall DOUBLE,
            accuracy_overall DOUBLE,
            n_obs BIGINT,
            brier_first_game DOUBLE,
            brier_sparse_1_5 DOUBLE,
            brier_moderate_6_25 DOUBLE,
            brier_established_25plus DOUBLE,
            logloss_first_game DOUBLE,
            logloss_sparse_1_5 DOUBLE,
            logloss_moderate_6_25 DOUBLE,
            logloss_established_25plus DOUBLE,
            accuracy_first_game DOUBLE,
            accuracy_sparse_1_5 DOUBLE,
            accuracy_moderate_6_25 DOUBLE,
            accuracy_established_25plus DOUBLE,
            n_first_game BIGINT,
            n_sparse_1_5 BIGINT,
            n_moderate_6_25 BIGINT,
            n_established_25plus BIGINT,
            search_stage STRING,
            run_timestamp STRING,
            val_start INT,
            val_end INT
        )
        USING DELTA
    """)

    (
        rr_tuning_spark_df
        .write
        .mode("append")
        .saveAsTable("naf_catalog.gold_summary.race_rating_tuning_results")
    )

    print("✓ Saved tuning results to naf_catalog.gold_summary.race_rating_tuning_results")

# COMMAND ----------

# DBTITLE 1,Stage 2 Coach Diagnostics Plot

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

COACH_ID = 9524
ROLLING_WINDOW = 50
RR2_TABLE = "naf_catalog.gold_fact.race_rating_corr_history_fact"

# Set to a race_id for one race only, or None for:
#   - one global plot
#   - one plot per race played by the coach
TARGET_RACE_ID = None

# ---------------------------------------------------------------------------
# 0) Check table exists
# ---------------------------------------------------------------------------
if not spark.catalog.tableExists(RR2_TABLE):
    raise ValueError(f"Table not found: {RR2_TABLE}")

# ---------------------------------------------------------------------------
# 1) Inspect schema and choose columns
# ---------------------------------------------------------------------------
rr2_schema = spark.table(RR2_TABLE).columns
print("Stage 2 columns:")
print(rr2_schema)

required_candidates = {
    "game_id": ["game_id"],
    "coach_id": ["coach_id"],
    "coach_game_number": ["coach_game_number"],
    "game_index": ["game_index"],
    "date_id": ["date_id"],
    "race_id": ["race_id"],
    "score_expected": ["score_expected"],
    "result_numeric": ["result_numeric"],

    # likely state columns
    "g_after": ["g_after"],
    "g_sigma_after": ["g_sigma_after", "g_sigma_before"],
    "theta_after": ["theta_after"],
    "theta_sigma_after": [
        "theta_sigma_after", "theta_sigma_before",
        "played_theta_sigma_after", "played_theta_sigma_before"
    ],
    "d_played_after": ["d_played_after", "d_after", "played_race_d_after"],
    "d_played_sigma_after": [
        "d_played_sigma_after", "d_sigma_after", "played_race_sigma_after",
        "d_played_sigma_before", "d_sigma_before"
    ],
}

chosen = {}
missing_core = []

for key, candidates in required_candidates.items():
    found = None
    for c in candidates:
        if c in rr2_schema:
            found = c
            break
    chosen[key] = found

for k in ["game_id", "coach_id", "coach_game_number", "game_index", "race_id"]:
    if chosen[k] is None:
        missing_core.append(k)

if missing_core:
    raise ValueError(
        f"Stage 2 diagnostics: missing core columns {missing_core}. "
        f"Available columns: {rr2_schema}"
    )

print("\nChosen columns:")
for k, v in chosen.items():
    print(f"  {k}: {v}")

# ---------------------------------------------------------------------------
# 2) Load Stage 2 trajectory for one coach
# ---------------------------------------------------------------------------
select_cols = [v for v in chosen.values() if v is not None]
select_sql = ",\n       ".join(sorted(set(select_cols)))

rr2_df = spark.sql(f"""
    SELECT {select_sql}
    FROM {RR2_TABLE}
    WHERE coach_id = {COACH_ID}
    ORDER BY coach_game_number
""").toPandas()

if len(rr2_df) == 0:
    raise ValueError(f"No Stage 2 data for coach_id={COACH_ID}")

# Rename to canonical names
rename_map = {v: k for k, v in chosen.items() if v is not None}
rr2_df = rr2_df.rename(columns=rename_map)

# ---------------------------------------------------------------------------
# 3) Load Elo comparison (global only)
# ---------------------------------------------------------------------------
elo_df = spark.sql(f"""
    SELECT
        ROW_NUMBER() OVER (ORDER BY game_index, game_id) AS coach_game_number,
        rating_after AS elo_rating
    FROM naf_catalog.gold_fact.rating_history_fact
    WHERE coach_id = {COACH_ID}
      AND scope = 'GLOBAL'
    ORDER BY game_index, game_id
""").toPandas()

if len(elo_df) > 0:
    elo_df["elo_rolling_median"] = (
        elo_df["elo_rating"]
        .rolling(window=ROLLING_WINDOW, min_periods=1)
        .median()
    )

# ---------------------------------------------------------------------------
# 4) Coach / race names
# ---------------------------------------------------------------------------
coach_name_row = spark.sql(f"""
    SELECT coach_name
    FROM naf_catalog.gold_dim.coach_dim
    WHERE coach_id = {COACH_ID}
""").first()

coach_name = coach_name_row["coach_name"] if coach_name_row else str(COACH_ID)

race_dim_df = spark.sql("""
    SELECT race_id, race_name
    FROM naf_catalog.gold_dim.race_dim
""").toPandas()

race_name_map = dict(zip(race_dim_df["race_id"], race_dim_df["race_name"]))
rr2_df["race_name"] = rr2_df["race_id"].map(race_name_map).fillna(rr2_df["race_id"].astype(str))

# ---------------------------------------------------------------------------
# 5) Build plotting helper
# ---------------------------------------------------------------------------
def make_stage2_plot(df_plot, plot_kind="global", race_id=None, race_name=None):
    """
    plot_kind: 'global' or 'race'
    """
    if len(df_plot) == 0:
        print(f"Skipping empty plot: {plot_kind}, race_id={race_id}")
        return

    game_num = df_plot["coach_game_number"].values

    # top-panel mean series
    if plot_kind == "global":
        if "g_after" in df_plot.columns:
            mu = df_plot["g_after"].values
            mu_label = "RR2 global g"
        elif "theta_after" in df_plot.columns:
            mu = df_plot["theta_after"].values
            mu_label = "RR2 θ"
        else:
            raise ValueError("Need either g_after or theta_after to plot global trajectory")

        sigma = None
        sigma_label = None
        if "g_sigma_after" in df_plot.columns:
            sigma = df_plot["g_sigma_after"].values
            sigma_label = "global σ"
        elif "theta_sigma_after" in df_plot.columns:
            sigma = df_plot["theta_sigma_after"].values
            sigma_label = "σ"

    else:  # race-specific
        if "theta_after" in df_plot.columns:
            mu = df_plot["theta_after"].values
            mu_label = f"RR2 θ — {race_name}"
        elif "d_played_after" in df_plot.columns:
            mu = df_plot["d_played_after"].values
            mu_label = f"RR2 played-race d — {race_name}"
        else:
            raise ValueError("Need theta_after or d_played_after to plot race trajectory")

        sigma = None
        sigma_label = None
        if "theta_sigma_after" in df_plot.columns:
            sigma = df_plot["theta_sigma_after"].values
            sigma_label = "played-race σ"
        elif "d_played_sigma_after" in df_plot.columns:
            sigma = df_plot["d_played_sigma_after"].values
            sigma_label = "played-race σ"

    # proxy volatility
    volatility = np.r_[np.nan, np.abs(np.diff(mu))]

    # days gap
    if "date_id" in df_plot.columns:
        dt_series = pd.to_datetime(
            df_plot["date_id"].astype(str),
            format="%Y%m%d",
            errors="coerce"
        )
        days_since_prev = dt_series.diff().dt.days.fillna(0).values
    else:
        days_since_prev = np.zeros(len(df_plot))

    has_d = "d_played_after" in df_plot.columns

    fig, axes = plt.subplots(
        4, 1,
        figsize=(14, 11),
        sharex=True,
        gridspec_kw={"height_ratios": [3, 1, 1, 1]}
    )

    ax1, ax2, ax3, ax4 = axes

    # --- Panel 1: trajectory + uncertainty (+ Elo if global) ---
    if sigma is not None:
        ax1.fill_between(
            game_num,
            mu - 2 * sigma,
            mu + 2 * sigma,
            alpha=0.20,
            color="steelblue",
            label=f"{mu_label} ± 2σ"
        )

    ax1.plot(
        game_num,
        mu,
        color="steelblue",
        linewidth=1.2,
        label=mu_label
    )

    if plot_kind == "global" and len(elo_df) > 0:
        elo_merge = pd.merge(
            df_plot[["coach_game_number"]],
            elo_df,
            on="coach_game_number",
            how="left"
        )

        if elo_merge["elo_rating"].notna().any():
            ax1.plot(
                game_num,
                elo_merge["elo_rating"].values,
                color="coral",
                linewidth=0.6,
                alpha=0.4,
                linestyle="--",
                label="Elo (raw)"
            )
            ax1.plot(
                game_num,
                elo_merge["elo_rolling_median"].values,
                color="coral",
                linewidth=1.2,
                alpha=0.9,
                label=f"Elo (rolling {ROLLING_WINDOW}-game median)"
            )

    ax1.axhline(
        y=150,
        color="gray",
        linestyle=":",
        linewidth=0.7,
        alpha=0.6,
        label="Initial (150)"
    )

    hp_text = (
        f"RR2: σ²_obs={float(_cfg['rr2_sigma2_obs'])}  "
        f"q_global={float(_cfg['rr2_q_global'])}  "
        f"q_race={float(_cfg['rr2_q_race'])}  "
        f"λ={float(_cfg['rr2_cov_shrinkage_lambda'])}"
    )
    ax1.plot([], [], " ", label=hp_text)

    ax1.set_ylabel("Rating")

    if plot_kind == "global":
        title = (
            f"Race Rating S2 Diagnostics — {coach_name} "
            f"(coach {COACH_ID}, global, {len(df_plot)} games)"
        )
    else:
        title = (
            f"Race Rating S2 Diagnostics — {coach_name} "
            f"(coach {COACH_ID}, race: {race_name}, {len(df_plot)} games)"
        )

    ax1.set_title(title)
    ax1.legend(loc="lower right", fontsize=7)
    ax1.grid(True, alpha=0.3)

    # --- Panel 2: uncertainty ---
    if sigma is not None:
        ax2.plot(game_num, sigma, color="steelblue", linewidth=1.0)
        ax2.set_ylabel(sigma_label)
    else:
        ax2.plot(game_num, np.zeros(len(game_num)), color="steelblue", linewidth=1.0)
        ax2.set_ylabel("σ unavailable")
    ax2.set_ylim(bottom=0)
    ax2.grid(True, alpha=0.3)

    # --- Panel 3: volatility proxy ---
    ax3.plot(game_num, volatility, color="darkorange", linewidth=1.0, label="|Δ state|")
    if plot_kind == "race" and has_d:
        ax3.plot(
            game_num,
            df_plot["d_played_after"].values,
            linewidth=0.9,
            alpha=0.8,
            label=f"played-race d — {race_name}"
        )
    ax3.set_ylabel("Movement")
    ymin = np.nanmin(volatility) if np.isfinite(np.nanmin(volatility)) else 0
    ax3.set_ylim(bottom=min(0, ymin))
    ax3.grid(True, alpha=0.3)
    if plot_kind == "race" and has_d:
        ax3.legend(fontsize=7, loc="upper right")

    # --- Panel 4: days gap ---
    ax4.plot(game_num, days_since_prev, color="seagreen", linewidth=1.0)
    ax4.set_ylabel("Days gap")
    ax4.set_xlabel("Game number")
    ax4.set_ylim(bottom=0)
    ax4.grid(True, alpha=0.3)

    fig.tight_layout()
    plt.show()

# ---------------------------------------------------------------------------
# 6) Decide what to plot
# ---------------------------------------------------------------------------
coach_races = (
    rr2_df[["race_id", "race_name"]]
    .drop_duplicates()
    .sort_values(["race_name", "race_id"])
    .reset_index(drop=True)
)

print("\nRaces played by coach:")
display(coach_races)

if TARGET_RACE_ID is not None:
    race_df = rr2_df[rr2_df["race_id"] == TARGET_RACE_ID].copy()
    if len(race_df) == 0:
        available = coach_races["race_id"].tolist()
        raise ValueError(
            f"Coach {COACH_ID} has no Stage 2 rows for race_id={TARGET_RACE_ID}. "
            f"Available race_ids: {available}"
        )

    race_name = race_df["race_name"].iloc[0]
    make_stage2_plot(
        race_df,
        plot_kind="race",
        race_id=TARGET_RACE_ID,
        race_name=race_name
    )

else:
    # Global plot first
    make_stage2_plot(
        rr2_df.copy(),
        plot_kind="global",
        race_name="GLOBAL"
    )

    # Then one plot per race
    for _, row in coach_races.iterrows():
        race_id = row["race_id"]
        race_name = row["race_name"]
        race_df = rr2_df[rr2_df["race_id"] == race_id].copy()

        make_stage2_plot(
            race_df,
            plot_kind="race",
            race_id=race_id,
            race_name=race_name
        )

# COMMAND ----------

# DBTITLE 1,Ridgeline Plot — Coach race rating distributions over time

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import cm, colors

# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------
COACH_ID = 9524
USE_STAGE2 = True

MIN_GAMES_PER_RACE = 1          # 0 allowed
LOOKBACK_GAMES = 50           # None = all history, else last N games for this coach

BINS = 220
SMOOTH_BW = 10.0                # smoothing bandwidth in rating units
HEIGHT_SCALE = 38.0
X_PAD = 15.0

# Outlier handling
TRIM_MODE = "iqr"              # "none", "quantile", "iqr"
TRIM_Q_LOW = 0.01               # used if TRIM_MODE == "quantile"
TRIM_Q_HIGH = 0.99
IQR_MULTIPLIER = 1.5            # used if TRIM_MODE == "iqr"

# Median markers
SHOW_MEDIAN_LINE = True
SHOW_MEDIAN_TEXT = True

# Mean text
SHOW_MEAN_TEXT = True

# ---------------------------------------------------------------------------
# 0) Pick source table
# ---------------------------------------------------------------------------
candidate_tables = []
if USE_STAGE2:
    candidate_tables.append("naf_catalog.gold_fact.race_rating_corr_history_fact")
candidate_tables.append("naf_catalog.gold_fact.race_rating_history_fact")

source_table = None
for t in candidate_tables:
    if spark.catalog.tableExists(t):
        source_table = t
        break

if source_table is None:
    raise ValueError("No race rating history table found.")

print(f"Using source table: {source_table}")

# ---------------------------------------------------------------------------
# 1) Inspect schema
# ---------------------------------------------------------------------------
schema_cols = set(spark.table(source_table).columns)

def pick_col(candidates, required=False, label=None):
    for c in candidates:
        if c in schema_cols:
            return c
    if required:
        raise ValueError(f"Missing required column for {label}: tried {candidates}")
    return None

col_game   = pick_col(["game_id"], required=True, label="game_id")
col_coach  = pick_col(["coach_id"], required=True, label="coach_id")
col_race   = pick_col(["race_id"], required=True, label="race_id")
col_g      = pick_col(["g_after"], required=True, label="g_after")
col_theta  = pick_col(["theta_after"], required=False, label="theta_after")
col_date   = pick_col(["date_id"], required=False, label="date_id")
col_index  = pick_col(["game_index"], required=False, label="game_index")

if col_theta is None:
    print("WARNING: theta_after not found. Race rows will use g_after instead.")
    col_theta = col_g

# ---------------------------------------------------------------------------
# 2) Load full coach history and selected plotting window
# ---------------------------------------------------------------------------
order_clause = []
if col_index is not None:
    order_clause.append(col_index)
order_clause.append(col_game)
order_sql = ", ".join(order_clause)

full_df = spark.sql(f"""
    SELECT
        {col_game}  AS game_id,
        {col_coach} AS coach_id,
        {col_race}  AS race_id,
        {col_g}     AS g_after,
        {col_theta} AS theta_after
        {"," if col_date is not None else ""} {f"{col_date} AS date_id" if col_date is not None else ""}
        {"," if col_index is not None else ""} {f"{col_index} AS game_index" if col_index is not None else ""}
    FROM {source_table}
    WHERE coach_id = {COACH_ID}
    ORDER BY {order_sql}
""").toPandas()

if len(full_df) == 0:
    raise ValueError(f"No data found for coach_id={COACH_ID} in {source_table}")

plot_df = full_df.copy()

if LOOKBACK_GAMES is not None:
    LOOKBACK_GAMES = int(LOOKBACK_GAMES)
    if LOOKBACK_GAMES <= 0:
        raise ValueError("LOOKBACK_GAMES must be positive or None.")
    plot_df = plot_df.tail(LOOKBACK_GAMES).copy()

# ---------------------------------------------------------------------------
# 3) Coach + race names
# ---------------------------------------------------------------------------
coach_name_row = spark.sql(f"""
    SELECT coach_name
    FROM naf_catalog.gold_dim.coach_dim
    WHERE coach_id = {COACH_ID}
""").first()

coach_name = coach_name_row["coach_name"] if coach_name_row else str(COACH_ID)

race_dim_df = spark.sql("""
    SELECT race_id, race_name
    FROM naf_catalog.gold_dim.race_dim
""").toPandas()

race_name_map = dict(zip(race_dim_df["race_id"], race_dim_df["race_name"]))

full_df["race_name"] = full_df["race_id"].map(race_name_map).fillna(full_df["race_id"].astype(str))
plot_df["race_name"] = plot_df["race_id"].map(race_name_map).fillna(plot_df["race_id"].astype(str))

# ---------------------------------------------------------------------------
# 4) Outlier handling helper
# ---------------------------------------------------------------------------
def trim_values(values, mode="none", q_low=0.01, q_high=0.99, iqr_multiplier=1.5):
    values = np.asarray(values, dtype=float)
    values = values[np.isfinite(values)]

    if len(values) == 0:
        return values

    if mode == "none":
        return values

    if mode == "quantile":
        lo = np.quantile(values, q_low)
        hi = np.quantile(values, q_high)
        return values[(values >= lo) & (values <= hi)]

    if mode == "iqr":
        q1 = np.quantile(values, 0.25)
        q3 = np.quantile(values, 0.75)
        iqr = q3 - q1
        lo = q1 - iqr_multiplier * iqr
        hi = q3 + iqr_multiplier * iqr
        return values[(values >= lo) & (values <= hi)]

    raise ValueError("TRIM_MODE must be one of: 'none', 'quantile', 'iqr'.")

# ---------------------------------------------------------------------------
# 5) Build distributions
# ---------------------------------------------------------------------------
series_rows = []

# Global row: g_after over selected time window
g_vals_raw = plot_df["g_after"].dropna().to_numpy(dtype=float)
g_vals = trim_values(
    g_vals_raw,
    mode=TRIM_MODE,
    q_low=TRIM_Q_LOW,
    q_high=TRIM_Q_HIGH,
    iqr_multiplier=IQR_MULTIPLIER
)

if len(g_vals) > 0:
    series_rows.append({
        "label": "GLOBAL",
        "display_name": "Global",
        "values": g_vals,
        "mean": float(np.mean(g_vals)),
        "median": float(np.median(g_vals)),
        "n": int(len(g_vals)),
        "n_raw": int(len(g_vals_raw)),
        "kind": "global",
        "has_window_data": True,
        "historical_n": int(len(full_df["g_after"].dropna())),
    })

# All races ever played by this coach
all_races_df = (
    full_df[["race_id", "race_name"]]
    .drop_duplicates()
    .sort_values(["race_name", "race_id"])
    .reset_index(drop=True)
)

if len(all_races_df) == 0:
    raise ValueError(f"No race history found for coach_id={COACH_ID}")

# Full-history counts per race
hist_counts_df = (
    full_df.groupby(["race_id", "race_name"], as_index=False)
    .agg(historical_n=("theta_after", "count"))
)

hist_counts = {
    int(row["race_id"]): int(row["historical_n"])
    for _, row in hist_counts_df.iterrows()
}

for _, row in all_races_df.iterrows():
    race_id = int(row["race_id"])
    race_name = row["race_name"]

    # values in selected plotting window
    vals_raw = plot_df.loc[
        plot_df["race_id"] == race_id,
        "theta_after"
    ].dropna().to_numpy(dtype=float)

    hist_count = hist_counts.get(race_id, 0)

    # Respect threshold only when > 0, using full-history count
    if MIN_GAMES_PER_RACE > 0 and hist_count < MIN_GAMES_PER_RACE:
        continue

    vals = trim_values(
        vals_raw,
        mode=TRIM_MODE,
        q_low=TRIM_Q_LOW,
        q_high=TRIM_Q_HIGH,
        iqr_multiplier=IQR_MULTIPLIER
    )

    # Keep empty rows when MIN_GAMES_PER_RACE == 0
    if len(vals) == 0:
        if MIN_GAMES_PER_RACE == 0:
            series_rows.append({
                "label": race_name,
                "display_name": race_name,
                "values": np.array([], dtype=float),
                "mean": np.nan,
                "median": np.nan,
                "n": 0,
                "n_raw": 0,
                "kind": "race",
                "has_window_data": False,
                "historical_n": hist_count,
            })
        continue

    series_rows.append({
        "label": race_name,
        "display_name": race_name,
        "values": vals,
        "mean": float(np.mean(vals)),
        "median": float(np.median(vals)),
        "n": int(len(vals)),
        "n_raw": int(len(vals_raw)),
        "kind": "race",
        "has_window_data": True,
        "historical_n": hist_count,
    })

if len(series_rows) == 0:
    raise ValueError("No distributions available to plot.")

# Global first, then races with data, then empty races
global_rows = [r for r in series_rows if r["kind"] == "global"]
race_rows = [r for r in series_rows if r["kind"] == "race"]

race_rows_with_data = [r for r in race_rows if r["has_window_data"]]
race_rows_empty = [r for r in race_rows if not r["has_window_data"]]

race_rows_with_data = sorted(race_rows_with_data, key=lambda r: (r["mean"], r["n_raw"]))
race_rows_empty = sorted(race_rows_empty, key=lambda r: r["display_name"])

series_rows = global_rows + race_rows_with_data + race_rows_empty

# ---------------------------------------------------------------------------
# 6) Density helper
# ---------------------------------------------------------------------------
nonempty_vals = [r["values"] for r in series_rows if len(r["values"]) > 0]
if len(nonempty_vals) == 0:
    raise ValueError("All plotted series are empty in the selected window.")

all_vals = np.concatenate(nonempty_vals)
x_min = float(np.floor(all_vals.min() / 10.0) * 10.0 - X_PAD)
x_max = float(np.ceil(all_vals.max() / 10.0) * 10.0 + X_PAD)
x_grid = np.linspace(x_min, x_max, BINS)

def smooth_density(values, x_grid, bw=10.0):
    values = np.asarray(values, dtype=float)
    if len(values) == 0:
        return np.zeros_like(x_grid)

    hist, edges = np.histogram(
        values,
        bins=len(x_grid) - 1,
        range=(x_grid.min(), x_grid.max()),
        density=True
    )
    mids = 0.5 * (edges[:-1] + edges[1:])
    dx = mids[1] - mids[0]
    sigma_bins = max(bw / dx, 1e-6)

    half_width = int(max(3, np.ceil(4 * sigma_bins)))
    kernel_x = np.arange(-half_width, half_width + 1)
    kernel = np.exp(-0.5 * (kernel_x / sigma_bins) ** 2)
    kernel = kernel / kernel.sum()

    smoothed = np.convolve(hist, kernel, mode="same")

    if len(smoothed) != len(mids):
        smoothed = smoothed[:len(mids)]

    return np.interp(x_grid, mids, smoothed)

for r in series_rows:
    r["density"] = smooth_density(r["values"], x_grid, bw=SMOOTH_BW)

# ---------------------------------------------------------------------------
# 7) Ridgeline plot
# ---------------------------------------------------------------------------
finite_means = np.array([r["mean"] for r in series_rows if np.isfinite(r["mean"])], dtype=float)
norm = colors.Normalize(vmin=np.min(finite_means), vmax=np.max(finite_means))
cmap = cm.get_cmap("RdYlGn")

fig_h = max(8, 0.45 * len(series_rows) + 2.4)
fig, ax = plt.subplots(figsize=(13, fig_h))

y_positions = np.arange(len(series_rows))[::-1]
ridge_max = max(HEIGHT_SCALE * np.max(r["density"]) for r in series_rows)

for y0, r in zip(y_positions, series_rows):
    dens = r["density"]
    ridge = y0 + HEIGHT_SCALE * dens

    if r["has_window_data"] and np.isfinite(r["mean"]):
        colour = cmap(norm(r["mean"]))
        ax.fill_between(
            x_grid, y0, ridge,
            color=colour, alpha=0.62, linewidth=0
        )
        ax.plot(x_grid, ridge, color="black", linewidth=0.8, alpha=0.9)
    else:
        ax.plot(x_grid, np.full_like(x_grid, y0), color="gray", linewidth=0.8, alpha=0.6)

    # Median marker only if data exists
    if r["has_window_data"] and SHOW_MEDIAN_LINE and np.isfinite(r["median"]):
        median_x = r["median"]
        ridge_y = np.interp(median_x, x_grid, ridge)
        ax.plot([median_x, median_x], [y0, ridge_y], color="black", linewidth=1.0, alpha=0.95)

        if SHOW_MEDIAN_TEXT:
            ax.text(
                median_x,
                ridge_y + 0.06,
                f"{median_x:.1f}",
                ha="center",
                va="bottom",
                fontsize=8.5,
                color="black"
            )

    # Left labels
    ax.text(
        x_min - 0.11 * (x_max - x_min),
        y0 + 0.18,
        r["display_name"],
        ha="right", va="center", fontsize=11
    )

    if SHOW_MEAN_TEXT:
        mean_text = f"{r['mean']:.1f}" if np.isfinite(r["mean"]) else "—"
        ax.text(
            x_min - 0.02 * (x_max - x_min),
            y0 + 0.18,
            mean_text,
            ha="right", va="center", fontsize=10, color="dimgray"
        )

    # Right side counts
    if r["kind"] == "race" and not r["has_window_data"]:
        count_text = f"n=0 / {r.get('historical_n', 0)}"
    else:
        count_text = f"n={r['n']}"
        if r["n"] != r["n_raw"]:
            count_text += f" / {r['n_raw']}"

    ax.text(
        x_max + 0.02 * (x_max - x_min),
        y0 + 0.18,
        count_text,
        ha="left", va="center", fontsize=9, color="dimgray"
    )

# Vertical guides
for xv in np.arange(np.floor(x_min / 10) * 10, np.ceil(x_max / 10) * 10 + 1, 10):
    ax.axvline(xv, color="lightgray", linewidth=0.7, alpha=0.6, zorder=0)

# Titles
lookback_txt = "all games" if LOOKBACK_GAMES is None else f"last {LOOKBACK_GAMES} games"
trim_txt = {
    "none": "no outlier trimming",
    "quantile": f"quantile trim [{TRIM_Q_LOW:.2f}, {TRIM_Q_HIGH:.2f}]",
    "iqr": f"IQR trim × {IQR_MULTIPLIER:.2f}"
}[TRIM_MODE]

ax.set_title(
    f"Race rating distributions — {coach_name} (coach {COACH_ID})",
    fontsize=17, pad=16
)
subtitle = (
    f"Window: {lookback_txt}. Global uses g_after. Race rows use theta_after. "
    f"Filter: min games per race = {MIN_GAMES_PER_RACE}. Outliers: {trim_txt}."
)
fig.text(0.125, 0.94, subtitle, fontsize=10.5)

# Axes styling
ax.set_xlim(x_min - 0.15 * (x_max - x_min), x_max + 0.12 * (x_max - x_min))
ax.set_ylim(-0.8, len(series_rows) - 0.2 + ridge_max + 0.5)
ax.set_yticks([])
ax.set_xlabel("Rating")
ax.spines["left"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.spines["top"].set_visible(False)

# Headers
ax.text(
    x_min - 0.11 * (x_max - x_min),
    y_positions[0] + 0.95,
    "Race",
    ha="right", va="bottom", fontsize=10, color="dimgray", fontweight="bold"
)
if SHOW_MEAN_TEXT:
    ax.text(
        x_min - 0.02 * (x_max - x_min),
        y_positions[0] + 0.95,
        "Avg",
        ha="right", va="bottom", fontsize=10, color="dimgray", fontweight="bold"
    )

ax.text(
    x_max + 0.02 * (x_max - x_min),
    y_positions[0] + 0.95,
    "Count",
    ha="left", va="bottom", fontsize=10, color="dimgray", fontweight="bold"
)

plt.tight_layout(rect=[0.06, 0.03, 0.98, 0.93])
plt.show()
