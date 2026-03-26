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

# --- Overall comparison ---
report_lines.append(f"{'='*78}")
report_lines.append(f"OVERALL COMPARISON")
report_lines.append(f"{'='*78}")
report_lines.append(f"{'Model':<18} {'Brier':>10} {'Log-loss':>10} {'Accuracy':>10}  "
                    f"{'dBr vs Elo':>11}")
for name, m in metrics.items():
    br_d = elo_br - m["brier"]
    dbr = f"{br_d:+11.5f}" if name != "Global Elo" else f"{'---':>11}"
    report_lines.append(f"{name:<18} {m['brier']:10.5f} {m['log_loss']:10.5f} "
                        f"{m['accuracy']:10.3f}  {dbr}")
report_lines.append("")

# --- Sliced evaluation ---
report_lines.append(f"{'='*78}")
report_lines.append(f"BRIER SCORE BY RACE EXPERIENCE")
report_lines.append(f"{'='*78}")
slice_hdr = f"{'Slice':<22} {'n':>7}"
for name in models:
    slice_hdr += f"  {name:>14}"
report_lines.append(slice_hdr)

for sl in ["first_game", "sparse_1_5", "moderate_6_25", "established_25plus"]:
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
    report_lines.append(line)
report_lines.append("")

# --- Key comparisons ---
report_lines.append(f"KEY COMPARISONS (Race Rating vs Race Elo)")
for sl in ["first_game", "sparse_1_5", "moderate_6_25", "established_25plus"]:
    mask = eval_df["race_experience"] == sl
    if mask.sum() == 0:
        continue
    y_sl = y[mask]
    rr = eval_df["rr_pred"].values[mask]
    re = eval_df["elo_race_pred"].values[mask]
    eg = eval_df["elo_global_pred"].values[mask]
    valid = ~(np.isnan(rr) | np.isnan(re) | np.isnan(eg))
    if valid.sum() > 0:
        d_re = brier_score(y_sl[valid], re[valid]) - brier_score(y_sl[valid], rr[valid])
        d_eg = brier_score(y_sl[valid], eg[valid]) - brier_score(y_sl[valid], rr[valid])
        report_lines.append(
            f"  {sl:<22}  vs Race Elo: {d_re:+.5f}  "
            f"vs Global Elo: {d_eg:+.5f}")
report_lines.append("")

# --- Parameters ---
report_lines.append(f"PARAMETERS")
report_lines.append(f"  prior_sigma_g = {float(_cfg['rr_prior_sigma_g'])}")
report_lines.append(f"  prior_sigma_d = {float(_cfg['rr_prior_sigma_d'])}")
report_lines.append(f"  sigma2_obs    = {float(_cfg['rr_sigma2_obs'])}")
report_lines.append(f"  q_global      = {float(_cfg['rr_q_global'])}")
report_lines.append(f"  q_race        = {float(_cfg['rr_q_race'])}")

report_text = "\n".join(report_lines)

report_path = f"{run_dir}/evaluation_report.txt"
with open(report_path, "w") as f:
    f.write(report_text)
print(f"✓ Report: {report_path}")

# Calibration plot
fig.savefig(f"{run_dir}/calibration_plots.png", dpi=150, bbox_inches="tight")
print(f"✓ Plot: {run_dir}/calibration_plots.png")

print(f"\n{'='*78}")
print(f"EXPORT COMPLETE — {run_dir}")
print(f"{'='*78}")

# COMMAND ----------

# DBTITLE 1,Race Rating Tuner (validation-window, stage 1)
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
#   - This is an in-memory research tuner; it does NOT overwrite production tables.
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

RR_INITIAL_RATING = float(_cfg["elo_initial_rating"])
RR_ELO_SCALE      = float(_cfg["elo_scale"])
RR_LN10_OVER_SCALE = math.log(10.0) / RR_ELO_SCALE

# Load feed once
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

rr_feed_rows = rr_feed_df.collect()
print(f"Race tuning feed loaded: {len(rr_feed_rows)} games")
print(f"Validation window: [{val_cutoff}, {test_cutoff})")

def rr_win_prob(theta_self, theta_opp):
    return 1.0 / (1.0 + 10.0 ** ((theta_opp - theta_self) / RR_ELO_SCALE))

def rr_brier(y_true, y_pred):
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    return float(np.mean((y_pred - y_true) ** 2)) if len(y_true) else np.nan

def rr_log_loss(y_true, y_pred, eps=1e-12):
    y_true = np.asarray(y_true, dtype=float)
    p = np.clip(np.asarray(y_pred, dtype=float), eps, 1.0 - eps)
    return float(-np.mean(y_true * np.log(p) + (1.0 - y_true) * np.log(1.0 - p))) if len(y_true) else np.nan

def rr_exp_slice(prior_race_games):
    if prior_race_games == 0:
        return "first_game"
    if 1 <= prior_race_games <= 5:
        return "sparse_1_5"
    if 6 <= prior_race_games <= 25:
        return "moderate_6_25"
    return "established_25plus"

# ---------------------------------------------------------------------------
# 1) Reusable race-engine runner
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
    Run stage-1 race-aware EKF in memory for a parameter variant.

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

    g_state = {}          # coach_id -> [g, P_g]
    d_state = {}          # (coach_id, race_id) -> [d, P_d]
    coach_game_counts = {}    # coach_id -> total games seen so far
    coach_race_counts = {}    # (coach_id, race_id) -> games with that race seen so far

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

        # --- prediction step ---
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
        S_h  = h_val * h_val * (Pg_h_pred + Pd_h_pred) + sigma2_obs
        Kg_h = h_val * Pg_h_pred / S_h
        Kd_h = h_val * Pd_h_pred / S_h

        g_h_post  = g_h + Kg_h * innov_h
        d_h_post  = d_h + Kd_h * innov_h
        Pg_h_post = max(1e-6, (1.0 - Kg_h * h_val) * Pg_h_pred)
        Pd_h_post = max(1e-6, (1.0 - Kd_h * h_val) * Pd_h_pred)

        # --- away update ---
        S_a  = h_val * h_val * (Pg_a_pred + Pd_a_pred) + sigma2_obs
        Kg_a = h_val * Pg_a_pred / S_a
        Kd_a = h_val * Pd_a_pred / S_a

        g_a_post  = g_a + Kg_a * innov_a
        d_a_post  = d_a + Kd_a * innov_a
        Pg_a_post = max(1e-6, (1.0 - Kg_a * h_val) * Pg_a_pred)
        Pd_a_post = max(1e-6, (1.0 - Kd_a * h_val) * Pd_a_pred)

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

        # --- persist states ---
        g_state[c_h] = [g_h_post, Pg_h_post]
        g_state[c_a] = [g_a_post, Pg_a_post]
        d_state[(c_h, r_h)] = [d_h_post, Pd_h_post]
        d_state[(c_a, r_a)] = [d_a_post, Pd_a_post]

        coach_game_counts[c_h] = coach_game_number_h
        coach_game_counts[c_a] = coach_game_number_a
        coach_race_counts[(c_h, r_h)] = prior_race_games_h + 1
        coach_race_counts[(c_a, r_a)] = prior_race_games_a + 1

    return pd.DataFrame(
        out_rows,
        columns=[
            "game_id", "coach_id", "date_id", "game_index", "race_id",
            "coach_game_number", "prior_race_games", "race_experience",
            "rr_pred", "result_numeric"
        ],
    )

# ---------------------------------------------------------------------------
# 2) Validation scorer
# ---------------------------------------------------------------------------
def compute_rr_objective(
    engine_out,
    val_start,
    val_end,
):
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
            "n_obs": 0,
            "brier_by_slice": {},
        }

    y = df["result_numeric"].to_numpy(dtype=float)
    p = df["rr_pred"].to_numpy(dtype=float)

    brier_overall = rr_brier(y, p)
    logloss_overall = rr_log_loss(y, p)

    brier_by_slice = {}
    for sl in ["first_game", "sparse_1_5", "moderate_6_25", "established_25plus"]:
        m = df["race_experience"] == sl
        if m.sum() > 0:
            brier_by_slice[sl] = {
                "brier": rr_brier(df.loc[m, "result_numeric"], df.loc[m, "rr_pred"]),
                "n": int(m.sum()),
            }

    return {
        "objective": float(brier_overall),
        "brier_overall": brier_overall,
        "logloss_overall": logloss_overall,
        "n_obs": int(len(df)),
        "brier_by_slice": brier_by_slice,
    }

# ---------------------------------------------------------------------------
# 3) Grid definitions
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
        if idx == 0:
            step = (candidates[1] - candidates[0]) / 2
        elif idx == len(candidates) - 1:
            step = (candidates[-1] - candidates[-2]) / 2
        else:
            step = min(candidates[idx] - candidates[idx - 1],
                       candidates[idx + 1] - candidates[idx]) / 2
        lo = max(val - step, min(candidates) * 0.5 if min(candidates) > 0 else 0.0)
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
# 4) Grid search runner
# ---------------------------------------------------------------------------
def run_rr_grid(grid, val_start, val_end, label="Race Grid"):
    keys = list(grid.keys())
    combos = list(itertools.product(*[grid[k] for k in keys]))

    print(f"\n{'='*86}")
    print(f"{label}: {len(combos)} parameter combinations")
    print(f"Objective: overall validation Brier")
    print(f"Validation window: date_id ∈ [{val_start}, {val_end})")
    print(f"{'='*86}")

    all_results = []
    t0 = time.time()

    for i, vals in enumerate(combos):
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
        avg_per = total_elapsed / (i + 1)
        remaining = avg_per * (len(combos) - i - 1)

        if (i + 1) % 5 == 0 or (i + 1) == len(combos):
            sl = scores.get("brier_by_slice", {})
            fg = sl.get("first_game", {}).get("brier", np.nan)
            sp = sl.get("sparse_1_5", {}).get("brier", np.nan)

            print(
                f"  [{i+1:>3}/{len(combos)}] "
                f"obj={scores['objective']:.5f}  "
                f"all={scores['brier_overall']:.5f}  "
                f"fg={fg:.5f}  sp={sp:.5f}  "
                f"σg={params['prior_sigma_g']:.1f} "
                f"σd={params['prior_sigma_d']:.1f} "
                f"σ²={params['sigma2_obs']:.3f} "
                f"qg={params['q_global']:.3f} "
                f"qr={params['q_race']:.3f}  "
                f"({elapsed:.0f}s, ~{remaining/60:.0f}m left)"
            )

    res_df = pd.DataFrame([
        {
            **r["params"],
            "objective": r["objective"],
            "brier_overall": r["brier_overall"],
            "logloss_overall": r["logloss_overall"],
            "n_obs": r["n_obs"],
            "brier_first_game": r["brier_by_slice"].get("first_game", {}).get("brier", np.nan),
            "brier_sparse_1_5": r["brier_by_slice"].get("sparse_1_5", {}).get("brier", np.nan),
            "brier_moderate_6_25": r["brier_by_slice"].get("moderate_6_25", {}).get("brier", np.nan),
            "brier_established_25plus": r["brier_by_slice"].get("established_25plus", {}).get("brier", np.nan),
            "n_first_game": r["brier_by_slice"].get("first_game", {}).get("n", 0),
            "n_sparse_1_5": r["brier_by_slice"].get("sparse_1_5", {}).get("n", 0),
            "n_moderate_6_25": r["brier_by_slice"].get("moderate_6_25", {}).get("n", 0),
            "n_established_25plus": r["brier_by_slice"].get("established_25plus", {}).get("n", 0),
        }
        for r in all_results
    ]).sort_values(["objective", "brier_overall"], ascending=[True, True]).reset_index(drop=True)

    return res_df, all_results

# # ---------------------------------------------------------------------------
# # 5) Run tuning (uncomment to execute)
# # ---------------------------------------------------------------------------
# TUNE_VAL_START = val_cutoff
# TUNE_VAL_END   = test_cutoff

# # --- coarse pass ---
# rr_coarse_df, rr_coarse_raw = run_rr_grid(
#     RR_COARSE_GRID,
#     TUNE_VAL_START,
#     TUNE_VAL_END,
#     label="RACE COARSE GRID"
# )
# display(rr_coarse_df.head(20))

# best_coarse = rr_coarse_df.iloc[0].to_dict()
# print("\nBest coarse params:")
# print({k: best_coarse[k] for k in ["prior_sigma_g", "prior_sigma_d", "sigma2_obs", "q_global", "q_race"]})

# # --- fine pass ---
# RR_FINE_GRID = make_rr_fine_grid(best_coarse)
# print("\nFine grid:")
# for k, v in RR_FINE_GRID.items():
#     print(f"  {k}: {v}")

# rr_fine_df, rr_fine_raw = run_rr_grid(
#     RR_FINE_GRID,
#     TUNE_VAL_START,
#     TUNE_VAL_END,
#     label="RACE FINE GRID"
# )
# display(rr_fine_df.head(20))

# best_fine = rr_fine_df.iloc[0]
# print(f"\n{'='*86}")
# print("FINAL BEST RACE PARAMETERS (validation objective)")
# print(f"{'='*86}")
# print(f"prior_sigma_g  = {best_fine['prior_sigma_g']:.4f}")
# print(f"prior_sigma_d  = {best_fine['prior_sigma_d']:.4f}")
# print(f"sigma2_obs     = {best_fine['sigma2_obs']:.4f}")
# print(f"q_global       = {best_fine['q_global']:.4f}")
# print(f"q_race         = {best_fine['q_race']:.4f}")
# print("")
# print(f"objective      = {best_fine['objective']:.5f}")
# print(f"brier_overall  = {best_fine['brier_overall']:.5f}")
# print(f"logloss_overall= {best_fine['logloss_overall']:.5f}")
# print(f"brier_first    = {best_fine['brier_first_game']:.5f}")
# print(f"brier_sparse   = {best_fine['brier_sparse_1_5']:.5f}")
# print(f"brier_moderate = {best_fine['brier_moderate_6_25']:.5f}")
# print(f"brier_estab    = {best_fine['brier_established_25plus']:.5f}")
