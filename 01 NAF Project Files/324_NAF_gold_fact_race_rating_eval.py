# Databricks notebook source
# MAGIC %md
# MAGIC # 324 — Race Rating Evaluation & Tuning
# MAGIC
# MAGIC **Layer:** GOLD_FACT (research / analysis)
# MAGIC **Pipeline position:** Runs after 310 (config), 320 (Elo + race Elo), 323 (race rating engine)
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC Evaluation, tuning, and diagnostics for the race-aware rating model.
# MAGIC Not part of the production pipeline — run on demand.
# MAGIC
# MAGIC ## Contents
# MAGIC
# MAGIC 1. **Load config and data** — analytical_config + game feed
# MAGIC 2. **Benchmark assembly** — 0.5 baseline, global Elo, race Elo, race-aware model
# MAGIC 3. **Evaluation** — chronological test set, Brier score, log-loss, accuracy
# MAGIC 4. **Sliced evaluation** — by race-history depth (sparse, first-game, established)
# MAGIC 5. **Calibration** — calibration plots for all models
# MAGIC 6. **Export** — results tables and plots
# MAGIC
# MAGIC ## Required model comparisons (from plan)
# MAGIC
# MAGIC 1. `0.5` baseline
# MAGIC 2. Global Elo (best global-only from 320)
# MAGIC 3. Race Elo (independent per-race Elo from 320)
# MAGIC 4. Race-aware model (323: global + independent race deviations)
# MAGIC 5. *(Future: global + correlated race deviations)*
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC Must run 310 (config), 320 (Elo tables), 323 (race rating engine).

# COMMAND ----------

# DBTITLE 1,Load Config and Data
# =============================================================================
# COMPONENT: Load analytical config, game feed, and model predictions
# =============================================================================

import math
import datetime
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

# --- 1. Race-aware model (323) ---
rr_df = spark.sql(f"""
    SELECT game_id, coach_id, race_id, date_id, game_index,
           coach_game_number,
           score_expected AS rr_pred,
           result_numeric,
           opponent_coach_id, opponent_race_id
    FROM naf_catalog.gold_fact.race_rating_history_fact
    WHERE date_id >= {eval_cutoff}
""").toPandas()

print(f"Race-aware model: {len(rr_df)} observations in test window")

# --- 2. Global Elo (320, GLOBAL scope) ---
elo_global_df = spark.sql(f"""
    SELECT game_id, coach_id,
           score_expected AS elo_global_pred
    FROM naf_catalog.gold_fact.rating_history_fact
    WHERE scope = 'GLOBAL'
      AND date_id >= {eval_cutoff}
""").toPandas()

# --- 3. Race Elo (320, RACE scope) ---
elo_race_df = spark.sql(f"""
    SELECT game_id, coach_id,
           score_expected AS elo_race_pred
    FROM naf_catalog.gold_fact.rating_history_fact
    WHERE scope = 'RACE'
      AND date_id >= {eval_cutoff}
""").toPandas()

# --- 4. Merge ---
eval_df = rr_df.copy()
eval_df = eval_df.merge(elo_global_df, on=["game_id", "coach_id"], how="left")
eval_df = eval_df.merge(elo_race_df, on=["game_id", "coach_id"], how="left")

# --- 5. Add 0.5 baseline ---
eval_df["baseline_pred"] = 0.5

# --- 6. Count prior games per (coach, race) for slicing ---
# coach_game_number is overall; we need race-specific game count
race_game_counts = spark.sql(f"""
    SELECT coach_id, race_id, coach_game_number AS overall_game_num,
           ROW_NUMBER() OVER (
               PARTITION BY coach_id, race_id
               ORDER BY game_index
           ) AS race_game_number,
           game_id
    FROM naf_catalog.gold_fact.race_rating_history_fact
""").toPandas()

# Merge race_game_number
eval_df = eval_df.merge(
    race_game_counts[["game_id", "coach_id", "race_game_number"]],
    on=["game_id", "coach_id"],
    how="left"
)

# Slice categories based on prior games with this race (before this game)
eval_df["prior_race_games"] = eval_df["race_game_number"] - 1
eval_df["race_experience"] = pd.cut(
    eval_df["prior_race_games"],
    bins=[-1, 0, 5, 25, float("inf")],
    labels=["first_game", "sparse_1_5", "moderate_6_25", "established_25plus"]
)

print(f"\nEvaluation DF: {len(eval_df)} rows")
print(f"  Models: baseline, elo_global, elo_race, race_rating")
print(f"\nRace experience distribution:")
print(eval_df["race_experience"].value_counts().sort_index())

n_missing_race_elo = eval_df["elo_race_pred"].isna().sum()
if n_missing_race_elo > 0:
    print(f"\n  WARNING: {n_missing_race_elo} rows missing race Elo predictions")

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
    "Race Rating":    eval_df["rr_pred"].values,
}

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
# This tests whether the model helps for sparse/unseen races.
# =============================================================================

print(f"{'='*78}")
print(f"BRIER SCORE BY RACE EXPERIENCE SLICE")
print(f"{'='*78}")

slices = ["first_game", "sparse_1_5", "moderate_6_25", "established_25plus"]
model_names = list(models.keys())

# Header
hdr = f"{'Slice':<22} {'n':>7}"
for name in model_names:
    hdr += f"  {name:>14}"
print(hdr)
print("-" * len(hdr))

for sl in slices:
    mask = eval_df["race_experience"] == sl
    n_sl = mask.sum()
    if n_sl == 0:
        continue
    y_sl = y[mask]
    line = f"{sl:<22} {n_sl:>7}"
    for name, preds in models.items():
        p_sl = preds[mask]
        valid = ~np.isnan(p_sl)
        if valid.sum() > 0:
            br = brier_score(y_sl[valid], p_sl[valid])
            line += f"  {br:>14.5f}"
        else:
            line += f"  {'N/A':>14}"
    print(line)

# Key comparison: race-aware vs race Elo in sparse/first-game slices
print(f"\n--- Key comparisons ---")
for sl in ["first_game", "sparse_1_5"]:
    mask = eval_df["race_experience"] == sl
    if mask.sum() == 0:
        continue
    y_sl = y[mask]
    rr = eval_df["rr_pred"].values[mask]
    re = eval_df["elo_race_pred"].values[mask]
    valid = ~(np.isnan(rr) | np.isnan(re))
    if valid.sum() > 0:
        d = brier_score(y_sl[valid], re[valid]) - brier_score(y_sl[valid], rr[valid])
        better = "Race Rating" if d > 0 else "Race Elo"
        print(f"  {sl}: ΔBrier (Race Elo - Race Rating) = {d:+.5f}  → {better} wins")

# COMMAND ----------

# DBTITLE 1,Calibration Plots
# =============================================================================
# Calibration comparison: 4 panels
# =============================================================================

cal_models = [
    ("Global Elo", eval_df["elo_global_pred"].values),
    ("Race Elo", eval_df["elo_race_pred"].values),
    ("Race Rating", eval_df["rr_pred"].values),
    ("0.5 baseline", eval_df["baseline_pred"].values),
]

fig, axes = plt.subplots(1, 4, figsize=(20, 5))

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
            bn.append(m.sum())

    ax.plot([0, 1], [0, 1], "k--", alpha=0.5)
    ax.bar(bc, ba, width=0.08, alpha=0.6, color="steelblue")
    ax.scatter(bc, ba, color="coral", zorder=5, s=40)
    ax.set_xlabel("Predicted")
    ax.set_ylabel("Actual")
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

run_timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

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
metrics_spark_df.write.mode("append").saveAsTable(
    "naf_catalog.gold_summary.race_rating_evaluation_metrics"
)
print(f"✓ Metrics stored in naf_catalog.gold_summary.race_rating_evaluation_metrics")

# --- CSV + report ---
spark.sql("CREATE VOLUME IF NOT EXISTS naf_catalog.gold_summary.exports")
model_tests_dir = "/Volumes/naf_catalog/gold_summary/exports/model_tests"
run_dir = f"{model_tests_dir}/race_eval_{run_timestamp}"
os.makedirs(run_dir, exist_ok=True)

# CSV
metrics_spark_df.coalesce(1).write.mode("overwrite").option(
    "header", "true"
).csv(f"{run_dir}/_csv_tmp")
part_files = [f.path for f in dbutils.fs.ls(f"{run_dir}/_csv_tmp")
              if f.name.startswith("part-") and f.name.endswith(".csv")]
if part_files:
    dbutils.fs.cp(part_files[0], f"{run_dir}/metrics.csv", True)
dbutils.fs.rm(f"{run_dir}/_csv_tmp", recurse=True)
print(f"✓ CSV: {run_dir}/metrics.csv")

# Text report
report_lines = []
report_lines.append(f"RACE RATING MODEL EVALUATION REPORT")
report_lines.append(f"Run: {run_timestamp}")
report_lines.append(f"Test: date_id >= {eval_cutoff} ({EVAL_TEST_MONTHS} months)")
report_lines.append(f"Observations: {len(eval_df)}")
report_lines.append("")
report_lines.append(f"{'Model':<18} {'Brier':>10} {'Log-loss':>10} {'dBr vs Elo':>11}")
for name, m in metrics.items():
    br_d = elo_br - m["brier"]
    dbr = f"{br_d:+11.5f}" if name != "Global Elo" else f"{'---':>11}"
    report_lines.append(f"{name:<18} {m['brier']:10.5f} {m['log_loss']:10.5f} {dbr}")

report_path = f"{run_dir}/evaluation_report.txt"
with open(report_path, "w") as f:
    f.write("\n".join(report_lines))
print(f"✓ Report: {report_path}")

# Calibration plot
fig.savefig(f"{run_dir}/calibration_plots.png", dpi=150, bbox_inches="tight")
print(f"✓ Plot: {run_dir}/calibration_plots.png")

print(f"\n{'='*78}")
print(f"EXPORT COMPLETE — {run_dir}")
print(f"{'='*78}")
