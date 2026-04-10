# Databricks notebook source
# MAGIC %md
# MAGIC # 322 — SSM Evaluation & Tuning
# MAGIC
# MAGIC **Layer:** GOLD_FACT  |  **Status:** Research (not production)
# MAGIC **Pipeline position:** After 321 (SSM engines)
# MAGIC
# MAGIC Evaluation, tuning, and diagnostics for SSM models. Run on demand for model comparison and development.
# MAGIC Includes coach plot export, calibration coverage, hyperparameter tuning, ablation tests, and bootstrap CIs.
# MAGIC
# MAGIC ## Dependencies
# MAGIC - `gold_dim.analytical_config` (310)
# MAGIC - `gold_fact.ssm_rating_history_fact`, `gold_fact.ssm2_rating_history_fact` (321)
# MAGIC - `gold_fact.game_feed_for_ratings_fact` (320)
# MAGIC
# MAGIC ## Outputs
# MAGIC - No persistent outputs; produces evaluation metrics, plots, and comparison tables
# MAGIC
# MAGIC **Design authority:** `NAF_Design_Specification.md`, `style_guides.md`
# MAGIC **Design reference:** `00 NAF Project Design/archive/SSM_Design.md`

# COMMAND ----------

# DBTITLE 1,Analytical Config
# =============================================================================
# COMPONENT: Load analytical parameters from central config
# =============================================================================
# PURPOSE      : Read SSM v2 (and shared) parameters from analytical_config
#                so this notebook is self-contained. Same params as 321.
# =============================================================================

import math
from collections import defaultdict

_cfg = spark.table("naf_catalog.gold_dim.analytical_config").first()

# Shared Elo/SSM constants
SSM2_INITIAL_RATING  = float(_cfg["elo_initial_rating"])    # 150.0
SSM2_ELO_SCALE       = float(_cfg["elo_scale"])             # 150.0
SSM2_LN10_OVER_SCALE = math.log(10.0) / SSM2_ELO_SCALE

# SSM v2 production hyperparameters
SSM2_PRIOR_SIGMA = float(_cfg["ssm2_prior_sigma"])          # 50.0
SSM2_SIGMA2_OBS  = float(_cfg["ssm2_sigma2_obs"])           # 0.10
SSM2_Q_TIME      = float(_cfg["ssm2_q_time"])               # 2.00
SSM2_Q_GAME      = float(_cfg["ssm2_q_game"])               # 0.025
SSM2_MAX_DAYS    = float(_cfg["ssm2_max_days"])             # 180.0
SSM2_V_BASE      = float(_cfg["ssm2_v_base"])               # 0.25
SSM2_V_SCALE     = float(_cfg["ssm2_v_scale"])              # 24.0
SSM2_V_DECAY     = float(_cfg["ssm2_v_decay"])              # 0.90
SSM2_V_MIN       = float(_cfg["ssm2_v_min"])                # 0.00
SSM2_V_MAX       = float(_cfg["ssm2_v_max"])                # 16.0

print(f"Config loaded — SSM v2 production: σ²_obs={SSM2_SIGMA2_OBS}  "
      f"q_time={SSM2_Q_TIME}  q_game={SSM2_Q_GAME}  v_scale={SSM2_V_SCALE}")

# ---------------------------------------------------------------------------
# Load game feed (same source as 321, needed by run_ssm2_variant)
# ---------------------------------------------------------------------------
from pyspark.sql import functions as F

ssm2_feed_df = (
    spark.table("naf_catalog.gold_fact.game_feed_for_ratings_fact")
    .withColumn("game_index", F.col("game_index").cast("int"))
    .orderBy(F.col("game_index").asc(), F.col("game_id").asc())
    .select(
        "game_id", "game_index", "event_timestamp", "game_date", "date_id",
        "tournament_id", "variant_id",
        "home_coach_id", "away_coach_id", "result_home", "result_away",
    )
)
ssm2_feed_rows = ssm2_feed_df.collect()
print(f"Game feed loaded: {len(ssm2_feed_rows)} games")

# COMMAND ----------

# DBTITLE 1,Coach Plot Export Function

# =============================================================================
# COMPONENT: Coach Plot Export Function
# =============================================================================
# PURPOSE      : Generate and save SSM v2 diagnostic plots for a list of coaches.
#                Each coach gets a 4-panel PNG: rating+Elo, σ, volatility, days gap.
# USAGE        : export_coach_plots([9524, 34738, 35505])
#                export_coach_plots([9524], output_dir="/Volumes/naf_catalog/gold_summary/exports/my_plots")
# OUTPUT       : One PNG per coach in the output directory.
# =============================================================================

import os
import matplotlib.pyplot as plt
import numpy as np

def export_coach_plots(coach_ids, run_name,
                       base_dir="/Volumes/naf_catalog/gold_summary/exports/ssm_coach_plots"):
    """Generate and save SSM v2-UA diagnostic plots for specified coaches.

    Plots the SSM v2 (weighted-tuned) model trajectories, which is the
    best-performing model when combined with uncertainty-aware (UA)
    predictions. The UA adjustment affects prediction probabilities but
    not the underlying μ, σ, or volatility shown here.

    Each invocation creates a subfolder under base_dir named after
    run_name, so multiple runs are kept separate.

    Args:
        coach_ids: List of coach_id integers to plot.
        run_name: Name for this run (used as subfolder name).
        base_dir: Parent directory for all coach plot exports.

    Returns:
        List of saved file paths.
    """
    output_dir = os.path.join(base_dir, run_name)
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
        ax1.set_title(f"SSM v2-UA Rating Diagnostics — {coach_name} "
                      f"(coach {cid}, {len(cdf)} games)")
        hp_text = (
            f"SSM v2 (weighted): σ²_obs={SSM2_SIGMA2_OBS}  "
            f"q_time={SSM2_Q_TIME}  q_game={SSM2_Q_GAME}  "
            f"v_scale={SSM2_V_SCALE}  |  Predictions use UA probit"
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

# DBTITLE 1,SSM2 Hyperparameter Tuner — Brier-UA Objective

# =============================================================================
# COMPONENT: SSM2 Hyperparameter Tuner — Brier-UA Objective
# =============================================================================
# PURPOSE      : Find optimal (sigma2_obs, q_time, q_game, v_scale) by grid
#                search, optimising Brier score of UA predictions on a
#                validation window (separate from the evaluation test set).
# OBJECTIVE    : Minimise Brier score of uncertainty-aware (probit-adjusted)
#                predictions on the validation window.
# STRATEGY     : Two-pass grid search — coarse (4D, wide) then fine (4D,
#                narrow neighbourhood around coarse best).
# VALIDATION   : Uses games in the validation window only (defined in the
#                evaluation cell: val_cutoff to test_cutoff). The engine
#                processes ALL historical games to build state, but only
#                validation-window predictions count toward the objective.
# FIXED PARAMS : prior_sigma=50, max_days=180, v_decay=0.90, v_base=0.25,
#                v_min=0.0, v_max=16.0
# PREREQS      : Requires ssm2_feed_rows from the engine cell,
#                run_ssm2_variant from the reusable engine cell,
#                and val_cutoff / test_cutoff from the evaluation cell.
# RUNTIME NOTE : Each grid point runs the full SSM2 loop over all games.
#                Coarse grid (~256 points) + fine grid (~81 points) ≈ 337 runs.
#                At ~30-60s per run on Community Edition, expect 3-6 hours.
# =============================================================================

import math
import time
import itertools
import numpy as np

# ---------------------------------------------------------------------------
# 1) UA coefficient (same as evaluation cell)
# ---------------------------------------------------------------------------
_TUNE_ELO_SCALE = SSM2_ELO_SCALE
_TUNE_LN10_S = math.log(10.0) / _TUNE_ELO_SCALE
_TUNE_UA_COEFF = (math.pi / 8.0) * _TUNE_LN10_S ** 2

# ---------------------------------------------------------------------------
# 2) Objective function: Brier score of UA predictions on validation window
# ---------------------------------------------------------------------------
def compute_brier_ua(engine_results, val_start, val_end):
    """Compute Brier score of UA predictions on the validation window.

    Args:
        engine_results: Output of run_ssm2_variant — dict[coach_id] ->
                        list of dicts with keys: game_id, date_id,
                        score_expected, sigma_before, opponent_sigma_before,
                        result_numeric.
        val_start: Start of validation window (inclusive, date_id int).
        val_end: End of validation window (exclusive, date_id int).

    Returns:
        dict with keys: brier_ua, brier_raw, n_obs, brier_by_tier
    """
    preds_raw = []
    sigmas_self = []
    sigmas_opp = []
    actuals = []
    game_numbers = []

    for cid, games in engine_results.items():
        for g in games:
            did = g["date_id"]
            if did < val_start or did >= val_end:
                continue
            preds_raw.append(g["score_expected"])
            sigmas_self.append(g["sigma_before"])
            sigmas_opp.append(g["opponent_sigma_before"])
            actuals.append(g["result_numeric"])
            game_numbers.append(g["coach_game_number"])

    if len(preds_raw) == 0:
        return {"brier_ua": 999.0, "brier_raw": 999.0, "n_obs": 0}

    preds_raw = np.array(preds_raw)
    sigmas_self = np.array(sigmas_self)
    sigmas_opp = np.array(sigmas_opp)
    actuals = np.array(actuals)
    game_numbers = np.array(game_numbers)

    # UA predictions
    sigma2_comb = sigmas_self ** 2 + sigmas_opp ** 2
    scale = np.sqrt(1.0 + _TUNE_UA_COEFF * sigma2_comb)
    eps = 1e-10
    p_clip = np.clip(preds_raw, eps, 1.0 - eps)
    log_odds = np.log10(p_clip / (1.0 - p_clip))
    preds_ua = 1.0 / (1.0 + 10.0 ** (-log_odds / scale))

    brier_ua = float(np.mean((preds_ua - actuals) ** 2))
    brier_raw = float(np.mean((preds_raw - actuals) ** 2))

    # Per-tier breakdown
    brier_by_tier = {}
    for tier_name, lo, hi in [("burnin", 1, 30), ("developing", 31, 100),
                               ("established", 101, 300), ("veteran", 301, 999999)]:
        mask = (game_numbers >= lo) & (game_numbers <= hi)
        if mask.sum() > 0:
            brier_by_tier[tier_name] = {
                "brier_ua": float(np.mean((preds_ua[mask] - actuals[mask]) ** 2)),
                "n": int(mask.sum()),
            }

    return {
        "brier_ua": brier_ua,
        "brier_raw": brier_raw,
        "n_obs": len(actuals),
        "brier_by_tier": brier_by_tier,
    }

# ---------------------------------------------------------------------------
# 3) Grid definitions
# ---------------------------------------------------------------------------
COARSE_GRID = {
    "sigma2_obs": [0.01, 0.05, 0.10, 0.20],
    "q_time":     [0.50, 1.00, 2.00, 4.00],
    "q_game":     [0.01, 0.025, 0.05, 0.15],
    "v_scale":    [4.0,  8.0,  16.0, 32.0],
}

def make_fine_grid(best_params):
    """Create a fine grid centred on best coarse params.

    3 values per param (below, at, above) = 3^4 = 81 points.
    """
    def _neighbourhood(val, candidates):
        idx = min(range(len(candidates)), key=lambda i: abs(candidates[i] - val))
        if idx == 0:
            step = (candidates[1] - candidates[0]) / 2
        elif idx == len(candidates) - 1:
            step = (candidates[-1] - candidates[-2]) / 2
        else:
            step = min(candidates[idx] - candidates[idx - 1],
                       candidates[idx + 1] - candidates[idx]) / 2
        return [max(val - step, candidates[0] * 0.5), val, val + step]

    return {
        "sigma2_obs": _neighbourhood(best_params["sigma2_obs"],
                                     COARSE_GRID["sigma2_obs"]),
        "q_time":     _neighbourhood(best_params["q_time"],
                                     COARSE_GRID["q_time"]),
        "q_game":     _neighbourhood(best_params["q_game"],
                                     COARSE_GRID["q_game"]),
        "v_scale":    _neighbourhood(best_params["v_scale"],
                                     COARSE_GRID["v_scale"]),
    }

# ---------------------------------------------------------------------------
# 4) Grid search runner
# ---------------------------------------------------------------------------
def run_brier_grid(grid, val_start, val_end, label="Grid"):
    """Run all parameter combinations, score by Brier-UA on validation window.

    Uses run_ssm2_variant() (defined in the reusable engine cell) to run
    the full SSM2 loop. Only games with date_id >= val_start are returned
    by the engine, then further filtered to [val_start, val_end) for scoring.

    Args:
        grid: dict of param_name -> list of values.
        val_start: Validation window start (inclusive, date_id int).
        val_end: Validation window end (exclusive, date_id int).
        label: Label for progress output.

    Returns:
        List of result dicts, each with keys: params, brier_ua, brier_raw,
        n_obs, brier_by_tier.
    """
    keys = list(grid.keys())
    combos = list(itertools.product(*[grid[k] for k in keys]))
    print(f"\n{'='*70}")
    print(f"{label}: {len(combos)} parameter combinations")
    print(f"Objective: Brier score of UA predictions")
    print(f"Validation window: date_id ∈ [{val_start}, {val_end})")
    print(f"{'='*70}")

    all_results = []
    t0 = time.time()

    for i, vals in enumerate(combos):
        params = dict(zip(keys, vals))
        t_start = time.time()

        engine_out = run_ssm2_variant(
            sigma2_obs=params["sigma2_obs"],
            q_time=params["q_time"],
            q_game=params["q_game"],
            v_scale=params["v_scale"],
            eval_cutoff_date_id=val_start,
        )
        scores = compute_brier_ua(engine_out, val_start, val_end)
        scores["params"] = params
        all_results.append(scores)

        elapsed = time.time() - t_start
        total_elapsed = time.time() - t0
        avg_per = total_elapsed / (i + 1)
        remaining = avg_per * (len(combos) - i - 1)

        if (i + 1) % 10 == 0 or (i + 1) == len(combos):
            tier_str = "  ".join(
                f"{t[:3]}={s['brier_ua']:.5f}"
                for t, s in sorted(scores.get("brier_by_tier", {}).items())
            )
            print(
                f"  [{i+1:>3}/{len(combos)}] "
                f"Brier-UA={scores['brier_ua']:.5f}  "
                f"raw={scores['brier_raw']:.5f}  "
                f"σ²={params['sigma2_obs']:.3f} "
                f"qt={params['q_time']:.2f} "
                f"qg={params['q_game']:.3f} "
                f"vs={params['v_scale']:.1f}  "
                f"({elapsed:.0f}s, ~{remaining/60:.0f}m left)"
            )

    return all_results

# ---------------------------------------------------------------------------
# 5) Run tuning — set RUN_TUNER = True to execute the grid search.
#    WARNING: Expect 3-6 hours on Community Edition (~337 engine runs).
# ---------------------------------------------------------------------------
RUN_TUNER = False

if RUN_TUNER:
    # Validation window (must match evaluation cell split)
    TUNE_VAL_START = val_cutoff   # e.g. 20250325
    TUNE_VAL_END   = test_cutoff  # e.g. 20250925

    # --- Coarse pass ---
    coarse_results = run_brier_grid(COARSE_GRID, TUNE_VAL_START, TUNE_VAL_END,
                                    "COARSE GRID (Brier-UA)")
    coarse_best = min(coarse_results, key=lambda x: x["brier_ua"])

    print(f"\nCoarse best: Brier-UA = {coarse_best['brier_ua']:.5f} "
          f"(raw = {coarse_best['brier_raw']:.5f}, n = {coarse_best['n_obs']})")
    print(f"  Params: {coarse_best['params']}")
    for tier, s in sorted(coarse_best.get("brier_by_tier", {}).items()):
        print(f"  {tier}: Brier-UA = {s['brier_ua']:.5f} (n={s['n']})")

    # --- Fine pass ---
    fine_grid = make_fine_grid(coarse_best["params"])
    print(f"\nFine grid neighbourhood:")
    for k, v in fine_grid.items():
        print(f"  {k}: {[round(x, 4) for x in v]}")

    fine_results = run_brier_grid(fine_grid, TUNE_VAL_START, TUNE_VAL_END,
                                  "FINE GRID (Brier-UA)")
    fine_best = min(fine_results, key=lambda x: x["brier_ua"])

    print(f"\n{'='*70}")
    print(f"FINAL BEST PARAMETERS (Brier-UA objective)")
    print(f"{'='*70}")
    print(f"  sigma2_obs = {fine_best['params']['sigma2_obs']:.4f}")
    print(f"  q_time     = {fine_best['params']['q_time']:.4f}")
    print(f"  q_game     = {fine_best['params']['q_game']:.4f}")
    print(f"  v_scale    = {fine_best['params']['v_scale']:.4f}")
    print(f"\n  Brier-UA   = {fine_best['brier_ua']:.5f}")
    print(f"  Brier-raw  = {fine_best['brier_raw']:.5f}")
    print(f"  n_obs      = {fine_best['n_obs']}")
    for tier, s in sorted(fine_best.get("brier_by_tier", {}).items()):
        print(f"  {tier}: Brier-UA = {s['brier_ua']:.5f} (n={s['n']})")

    # --- Top 10 ---
    fine_sorted = sorted(fine_results, key=lambda x: x["brier_ua"])
    print(f"\nTop 10 fine-grid candidates:")
    print(f"{'Brier-UA':>10}  {'raw':>10}  "
          f"{'σ²_obs':>7}  {'q_time':>7}  {'q_game':>7}  {'v_scale':>7}")
    for r in fine_sorted[:10]:
        p = r["params"]
        print(f"{r['brier_ua']:10.5f}  {r['brier_raw']:10.5f}  "
              f"{p['sigma2_obs']:7.4f}  {p['q_time']:7.3f}  "
              f"{p['q_game']:7.3f}  {p['v_scale']:7.2f}")

    print(f"\n{'='*70}")
    print("To apply: update SSM2_SIGMA2_OBS, SSM2_Q_TIME, SSM2_Q_GAME, "
          "SSM2_V_SCALE above, re-run engine + evaluation cells.")
    print("Then evaluate on the TEST window (date_id >= test_cutoff) to "
          "confirm improvement on unseen data.")
    print(f"{'='*70}")
else:
    print("Tuner skipped (RUN_TUNER = False). Set to True and re-run to execute.")


# COMMAND ----------

# DBTITLE 1,SSM2 Engine Function

# =============================================================================
# COMPONENT: Re-usable SSM2 Engine Function
# =============================================================================
# PURPOSE      : Run the SSM2 engine with arbitrary hyperparameters.
#                Used by the evaluation cell to generate predictions for
#                parameter variants (e.g. unweighted tuning) without needing
#                a separate pipeline table.
# PREREQS      : Requires ssm2_feed_rows from the SSM2 engine cell above.
# =============================================================================

import math
import datetime as dt
from collections import defaultdict

def run_ssm2_variant(sigma2_obs, q_time, q_game, v_scale,
                     feed_rows=None, eval_cutoff_date_id=None):
    """Run the full SSM2 engine with given hyperparameters.

    Structural parameters (prior, max_days, volatility bounds) are read
    from SSM2_* globals loaded from analytical_config in the first cell.
    Only the four tunable hyperparameters are passed as arguments.

    Args:
        sigma2_obs, q_time, q_game, v_scale: Tunable hyperparameters.
        feed_rows: Game feed rows (defaults to ssm2_feed_rows from engine cell).
        eval_cutoff_date_id: If set, only return results for games with
                             date_id >= this value (saves memory).

    Returns:
        dict[coach_id] -> list of dicts with keys:
            game_id, coach_game_number, date_id, score_expected,
            sigma_before, opponent_sigma_before, result_numeric
    """
    if feed_rows is None:
        feed_rows = ssm2_feed_rows

    # Structural parameters from analytical_config
    PRIOR_P = SSM2_PRIOR_SIGMA ** 2
    INITIAL = SSM2_INITIAL_RATING
    SCALE = SSM2_ELO_SCALE
    LN10S = SSM2_LN10_OVER_SCALE
    MAX_DAYS = SSM2_MAX_DAYS
    V_DECAY = SSM2_V_DECAY
    V_BASE = SSM2_V_BASE
    V_MIN = SSM2_V_MIN
    V_MAX = SSM2_V_MAX
    MIN_P = 1e-6

    # State: [mu, P, volatility, shock_ewma, last_dt]
    state = {}
    game_counts = {}
    results = defaultdict(list)

    def _default():
        return [INITIAL, PRIOR_P, V_BASE, 0.0, None]

    def _dt(row):
        ets = row["event_timestamp"]
        gd = row["game_date"]
        did = int(row["date_id"])
        if ets is not None:
            return ets.replace(tzinfo=None) if getattr(ets, "tzinfo", None) else ets
        if gd is not None:
            return dt.datetime.combine(gd, dt.time(0, 0))
        return dt.datetime.strptime(str(did), "%Y%m%d")

    for row in feed_rows:
        home_id = int(row["home_coach_id"])
        away_id = int(row["away_coach_id"])
        res_h = float(row["result_home"])
        res_a = float(row["result_away"])
        cur_dt = _dt(row)
        date_id = int(row["date_id"])
        game_id = int(row["game_id"])

        sh = state.get(home_id)
        if sh is None:
            sh = _default()
            state[home_id] = sh
        sa = state.get(away_id)
        if sa is None:
            sa = _default()
            state[away_id] = sa

        # Predict home
        days_h = max((cur_dt - sh[4]).total_seconds() / 86400.0, 0.0) if sh[4] else 0.0
        ts_h = math.sqrt(min(days_h, MAX_DAYS))
        P_h_pred = sh[1] + q_time * ts_h + q_game + sh[2]
        mu_h_pred = sh[0]

        # Predict away
        days_a = max((cur_dt - sa[4]).total_seconds() / 86400.0, 0.0) if sa[4] else 0.0
        ts_a = math.sqrt(min(days_a, MAX_DAYS))
        P_a_pred = sa[1] + q_time * ts_a + q_game + sa[2]
        mu_a_pred = sa[0]

        # Observe home
        p_exp_h = 1.0 / (1.0 + 10.0 ** ((mu_a_pred - mu_h_pred) / SCALE))
        H_h = p_exp_h * (1.0 - p_exp_h) * LN10S
        S_h = H_h * H_h * P_h_pred + H_h * H_h * P_a_pred + sigma2_obs
        innov_h = res_h - p_exp_h
        if S_h < 1e-12:
            mu_h_post, P_h_post = mu_h_pred, max(P_h_pred, MIN_P)
        else:
            K_h = H_h * P_h_pred / S_h
            mu_h_post = mu_h_pred + K_h * innov_h
            P_h_post = max((1.0 - K_h * H_h) * P_h_pred, MIN_P)

        # Observe away
        p_exp_a = 1.0 / (1.0 + 10.0 ** ((mu_h_pred - mu_a_pred) / SCALE))
        H_a = p_exp_a * (1.0 - p_exp_a) * LN10S
        S_a = H_a * H_a * P_a_pred + H_a * H_a * P_h_pred + sigma2_obs
        innov_a = res_a - p_exp_a
        if S_a < 1e-12:
            mu_a_post, P_a_post = mu_a_pred, max(P_a_pred, MIN_P)
        else:
            K_a = H_a * P_a_pred / S_a
            mu_a_post = mu_a_pred + K_a * innov_a
            P_a_post = max((1.0 - K_a * H_a) * P_a_pred, MIN_P)

        # Volatility
        shock_h = V_DECAY * sh[3] + (1.0 - V_DECAY) * (innov_h ** 2)
        vol_h = max(V_MIN, min(V_BASE + v_scale * shock_h, V_MAX))
        shock_a = V_DECAY * sa[3] + (1.0 - V_DECAY) * (innov_a ** 2)
        vol_a = max(V_MIN, min(V_BASE + v_scale * shock_a, V_MAX))

        # Game counts
        gn_h = game_counts.get(home_id, 0) + 1
        game_counts[home_id] = gn_h
        gn_a = game_counts.get(away_id, 0) + 1
        game_counts[away_id] = gn_a

        # Record results (only for test window if cutoff specified)
        if eval_cutoff_date_id is None or date_id >= eval_cutoff_date_id:
            results[home_id].append({
                "game_id": game_id, "coach_game_number": gn_h,
                "date_id": date_id, "score_expected": p_exp_h,
                "sigma_before": math.sqrt(P_h_pred),
                "opponent_sigma_before": math.sqrt(P_a_pred),
                "result_numeric": res_h,
            })
            results[away_id].append({
                "game_id": game_id, "coach_game_number": gn_a,
                "date_id": date_id, "score_expected": p_exp_a,
                "sigma_before": math.sqrt(P_a_pred),
                "opponent_sigma_before": math.sqrt(P_h_pred),
                "result_numeric": res_a,
            })

        # Persist state
        sh[0], sh[1], sh[2], sh[3], sh[4] = mu_h_post, P_h_post, vol_h, shock_h, cur_dt
        sa[0], sa[1], sa[2], sa[3], sa[4] = mu_a_post, P_a_post, vol_a, shock_a, cur_dt

    total_rows = sum(len(v) for v in results.values())
    print(f"  Engine variant: {total_rows} rows for {len(results)} coaches "
          f"(σ²_obs={sigma2_obs}, q_time={q_time}, q_game={q_game}, v_scale={v_scale})")
    return results


# COMMAND ----------

# DBTITLE 1,Model Comparison — SSM v1, SSM v2, Elo

# =============================================================================
# COMPONENT: Structured Model Comparison — SSM v1, SSM v2, Elo
# =============================================================================
# PURPOSE      : Compare predictive models on a held-out test window.
#
# CORE MODELS:
#   Elo              — baseline, logistic(Δrating)
#   SSM v2           — weighted-tuned, logistic(Δμ), ignores σ
#   SSM v2-UA        — weighted-tuned, uncertainty-aware
#   SSM v2-UW        — unweighted-tuned, logistic(Δμ), ignores σ
#   SSM v2-UW-UA     — unweighted-tuned, uncertainty-aware
#   Naive            — always predict 0.5
#
# ABLATION (does time-awareness matter?):
#   SSM v2-NT        — UW params with q_time=0 (no time), raw prediction
#   SSM v2-NT-UA     — UW params with q_time=0 (no time), UA prediction
#
# DIAGNOSTIC (reference only):
#   SSM v1 / v1-UA   — legacy random-walk SSM
#   Elo-UA           — Elo + SSM σ (tests σ value without SSM μ)
#   Ensemble         — 0.5 × Elo + 0.5 × SSM v2-UW-UA
#
# SPLIT        : 6-month validation window (tuner) + 6-month test window
#                (evaluation). All systems trained on all prior games.
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
import datetime as dt

# ---------------------------------------------------------------------------
# 1) Define validation/test split
#    - Validation: 12→6 months ago (tuner optimises on this)
#    - Test: last 6 months (reported metrics — never seen by tuner)
# ---------------------------------------------------------------------------
EVAL_TEST_MONTHS = 6
EVAL_VAL_MONTHS = 6
EVAL_TOTAL_MONTHS = EVAL_TEST_MONTHS + EVAL_VAL_MONTHS

_cutoffs = spark.sql(f"""
    SELECT
        CAST(DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -{EVAL_TOTAL_MONTHS}), 'yyyyMMdd') AS INT) AS val_cutoff,
        CAST(DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -{EVAL_TEST_MONTHS}), 'yyyyMMdd') AS INT) AS test_cutoff
""").first()
val_cutoff = _cutoffs["val_cutoff"]
test_cutoff = _cutoffs["test_cutoff"]

# Evaluation uses test window only; tuner uses validation window
eval_cutoff = test_cutoff
print(f"Validation window: {val_cutoff} to {test_cutoff} ({EVAL_VAL_MONTHS} months)")
print(f"Test window:       date_id >= {test_cutoff} ({EVAL_TEST_MONTHS} months)")

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
# 2b) Run SSM v2 variants via run_ssm2_variant()
# ---------------------------------------------------------------------------
import pandas as pd

def _variant_to_df(results, prefix):
    """Convert run_ssm2_variant output to a DataFrame for merging."""
    rows = []
    for cid, games in results.items():
        for g in games:
            rows.append({
                "game_id": g["game_id"],
                "coach_id": cid,
                f"{prefix}_pred": g["score_expected"],
                f"{prefix}_sigma_before": g["sigma_before"],
                f"{prefix}_opp_sigma_before": g["opponent_sigma_before"],
            })
    return pd.DataFrame(rows)

# --- SSM v2-UW: unweighted-tuned params ---
print("\nRunning SSM v2-UW (σ²_obs=0.125, q_time=2.0, q_game=0.025, v_scale=16.0)...")
ssm2_uw_results = run_ssm2_variant(
    sigma2_obs=0.125, q_time=2.0, q_game=0.025, v_scale=16.0,
    eval_cutoff_date_id=eval_cutoff,
)
uw_df = _variant_to_df(ssm2_uw_results, "ssm2_uw")
eval_df = eval_df.merge(uw_df, on=["game_id", "coach_id"], how="left")
print(f"  Merged: {eval_df['ssm2_uw_pred'].notna().sum()} / {len(eval_df)} rows")

# --- SSM v2-NT: no-time ablation (q_time=0, otherwise same as UW) ---
print("\nRunning SSM v2-NT (ablation: q_time=0, σ²_obs=0.125, q_game=0.025, v_scale=16.0)...")
ssm2_nt_results = run_ssm2_variant(
    sigma2_obs=0.125, q_time=0.0, q_game=0.025, v_scale=16.0,
    eval_cutoff_date_id=eval_cutoff,
)
nt_df = _variant_to_df(ssm2_nt_results, "ssm2_nt")
eval_df = eval_df.merge(nt_df, on=["game_id", "coach_id"], how="left")
print(f"  Merged: {eval_df['ssm2_nt_pred'].notna().sum()} / {len(eval_df)} rows")

# ---------------------------------------------------------------------------
# 3) Compute uncertainty-aware predictions (probit approximation)
# ---------------------------------------------------------------------------
# ELO_SCALE from analytical_config (loaded in first cell)
ELO_SCALE = SSM2_ELO_SCALE
LN10_OVER_S = math.log(10.0) / ELO_SCALE
PI_OVER_8 = math.pi / 8.0
UA_COEFF = PI_OVER_8 * LN10_OVER_S ** 2

def compute_ua_predictions(pred_raw, sigma_self, sigma_opp):
    sigma2_combined = sigma_self ** 2 + sigma_opp ** 2
    scale_factor = np.sqrt(1.0 + UA_COEFF * sigma2_combined)
    eps = 1e-10
    p_clipped = np.clip(pred_raw, eps, 1.0 - eps)
    log_odds = np.log10(p_clipped / (1.0 - p_clipped))
    log_odds_ua = log_odds / scale_factor
    return 1.0 / (1.0 + 10.0 ** (-log_odds_ua))

eval_df["ssm1_ua_pred"] = compute_ua_predictions(
    eval_df["ssm1_pred"].values,
    eval_df["ssm1_sigma_before"].values,
    eval_df["ssm1_opp_sigma_before"].values,
)

eval_df["ssm2_ua_pred"] = compute_ua_predictions(
    eval_df["ssm2_pred"].values,
    eval_df["ssm2_sigma_before"].values,
    eval_df["ssm2_opp_sigma_before"].values,
)

eval_df["ssm2_uw_ua_pred"] = compute_ua_predictions(
    eval_df["ssm2_uw_pred"].values,
    eval_df["ssm2_uw_sigma_before"].values,
    eval_df["ssm2_uw_opp_sigma_before"].values,
)

# No-time ablation UA
eval_df["ssm2_nt_ua_pred"] = compute_ua_predictions(
    eval_df["ssm2_nt_pred"].values,
    eval_df["ssm2_nt_sigma_before"].values,
    eval_df["ssm2_nt_opp_sigma_before"].values,
)

eval_df["ssm2_combined_sigma"] = np.sqrt(
    eval_df["ssm2_sigma_before"].values ** 2
    + eval_df["ssm2_opp_sigma_before"].values ** 2
)
eval_df["ssm1_combined_sigma"] = np.sqrt(
    eval_df["ssm1_sigma_before"].values ** 2
    + eval_df["ssm1_opp_sigma_before"].values ** 2
)
eval_df["ssm2_uw_combined_sigma"] = np.sqrt(
    eval_df["ssm2_uw_sigma_before"].values ** 2
    + eval_df["ssm2_uw_opp_sigma_before"].values ** 2
)

# Elo-UA: Elo predictions adjusted by SSM v2-UW uncertainty estimates
eval_df["elo_ua_pred"] = compute_ua_predictions(
    eval_df["elo_pred"].values,
    eval_df["ssm2_uw_sigma_before"].values,
    eval_df["ssm2_uw_opp_sigma_before"].values,
)

# Ensemble: simple average of Elo and SSM v2-UW-UA
eval_df["ensemble_pred"] = (
    eval_df["elo_pred"].values + eval_df["ssm2_uw_ua_pred"].values
) / 2.0

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
    p = np.clip(y_pred, EPS, 1.0 - EPS)
    return -np.mean(y_true * np.log(p) + (1 - y_true) * np.log(1 - p))

def brier_score(y_true, y_pred):
    return np.mean((y_pred - y_true) ** 2)

def accuracy(y_true, y_pred):
    mask = y_true != 0.5
    if mask.sum() == 0:
        return float("nan")
    correct = ((y_pred[mask] > 0.5) & (y_true[mask] == 1.0)) | \
              ((y_pred[mask] < 0.5) & (y_true[mask] == 0.0))
    return correct.mean()

# ---------------------------------------------------------------------------
# 5) Compute metrics for all models
# ---------------------------------------------------------------------------
y = eval_df["result_numeric"].values

models = {
    # --- Baselines ---
    "Elo":           np.clip(eval_df["elo_pred"].values, EPS, 1 - EPS),
    "Naive":         np.full_like(y, 0.5),
    # --- Core SSM v2 models ---
    "SSM v2":        np.clip(eval_df["ssm2_pred"].values, EPS, 1 - EPS),
    "SSM v2-UA":     np.clip(eval_df["ssm2_ua_pred"].values, EPS, 1 - EPS),
    "SSM v2-UW":     np.clip(eval_df["ssm2_uw_pred"].values, EPS, 1 - EPS),
    "SSM v2-UW-UA":  np.clip(eval_df["ssm2_uw_ua_pred"].values, EPS, 1 - EPS),
    # --- Ablation: no time-awareness (q_time=0) ---
    "SSM v2-NT":     np.clip(eval_df["ssm2_nt_pred"].values, EPS, 1 - EPS),
    "SSM v2-NT-UA":  np.clip(eval_df["ssm2_nt_ua_pred"].values, EPS, 1 - EPS),
    # --- Diagnostic (kept for reference, not production candidates) ---
    "SSM v1":        np.clip(eval_df["ssm1_pred"].values, EPS, 1 - EPS),
    "SSM v1-UA":     np.clip(eval_df["ssm1_ua_pred"].values, EPS, 1 - EPS),
    "Elo-UA":        np.clip(eval_df["elo_ua_pred"].values, EPS, 1 - EPS),
    "Ensemble":      np.clip(eval_df["ensemble_pred"].values, EPS, 1 - EPS),
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

for cmp_name in ["SSM v2-UW-UA", "SSM v2-NT-UA", "Ensemble"]:
    ll_imp = (elo_ll - metrics[cmp_name]["log_loss"]) / elo_ll * 100
    br_imp = (elo_br - metrics[cmp_name]["brier"]) / elo_br * 100
    print(f"{cmp_name} vs Elo: log-loss {ll_imp:+.3f}%, Brier {br_imp:+.3f}%")

# Ablation: time-awareness contribution
uw_br = metrics["SSM v2-UW-UA"]["brier"]
nt_br = metrics["SSM v2-NT-UA"]["brier"]
print(f"\nAblation: time-awareness contribution:")
print(f"  SSM v2-UW-UA Brier = {uw_br:.5f}")
print(f"  SSM v2-NT-UA Brier = {nt_br:.5f}  (q_time=0)")
print(f"  Δ = {nt_br - uw_br:+.5f}  ({'time helps' if uw_br < nt_br else 'time does not help'})")

# ---------------------------------------------------------------------------
# 5b) Bootstrap confidence intervals (game-level resampling)
# ---------------------------------------------------------------------------
# Resample at game level (each game produces 2 coach-rows) to avoid
# overstating effective sample size from correlated rows.
N_BOOTSTRAP = 2000
rng = np.random.default_rng(42)

game_ids = eval_df["game_id"].values
unique_games = np.unique(game_ids)
n_games = len(unique_games)

# Pre-build game->row index mapping for fast resampling
game_to_rows = {}
for idx, gid in enumerate(game_ids):
    if gid not in game_to_rows:
        game_to_rows[gid] = []
    game_to_rows[gid].append(idx)

# Key comparisons: Brier differences
bootstrap_pairs = [
    ("SSM v2-UW-UA", "Elo"),
    ("SSM v2-NT-UA", "Elo"),
    ("Ensemble", "Elo"),
    ("SSM v2-UW-UA", "SSM v2-NT-UA"),
]

print(f"\n{'='*78}")
print(f"BOOTSTRAP CONFIDENCE INTERVALS (Brier score differences)")
print(f"  {N_BOOTSTRAP} bootstrap resamples, game-level resampling "
      f"({n_games} games)")
print(f"{'='*78}")

boot_results = {}
for model_a, model_b in bootstrap_pairs:
    preds_a = models[model_a]
    preds_b = models[model_b]

    # Observed difference (positive = model_a better)
    obs_diff = brier_score(y, preds_b) - brier_score(y, preds_a)

    boot_diffs = np.empty(N_BOOTSTRAP)
    for b in range(N_BOOTSTRAP):
        sampled_games = rng.choice(unique_games, size=n_games, replace=True)
        row_indices = []
        for gid in sampled_games:
            row_indices.extend(game_to_rows[gid])
        row_indices = np.array(row_indices)

        y_b = y[row_indices]
        br_a = np.mean((preds_a[row_indices] - y_b) ** 2)
        br_b = np.mean((preds_b[row_indices] - y_b) ** 2)
        boot_diffs[b] = br_b - br_a

    ci_lo = np.percentile(boot_diffs, 2.5)
    ci_hi = np.percentile(boot_diffs, 97.5)
    p_positive = np.mean(boot_diffs > 0)
    boot_results[(model_a, model_b)] = {
        "obs": obs_diff, "ci_lo": ci_lo, "ci_hi": ci_hi, "p_pos": p_positive,
    }

    sig_marker = "*" if (ci_lo > 0 or ci_hi < 0) else ""
    print(f"\n  {model_a} vs {model_b}:")
    print(f"    ΔBrier = {obs_diff:+.5f}  "
          f"95% CI [{ci_lo:+.5f}, {ci_hi:+.5f}] {sig_marker}")
    print(f"    P({model_a} better) = {p_positive:.1%}")

print(f"\n  * = 95% CI excludes zero (significant at α=0.05)")

# ---------------------------------------------------------------------------
# 6) Calibration plots
# ---------------------------------------------------------------------------
cal_models = [
    ("Elo", models["Elo"]),
    ("SSM v2-UW-UA", models["SSM v2-UW-UA"]),
    ("SSM v2-NT-UA", models["SSM v2-NT-UA"]),
    ("SSM v2-UW", models["SSM v2-UW"]),
    ("SSM v2-NT", models["SSM v2-NT"]),
    ("Ensemble", models["Ensemble"]),
]

n_cal = len(cal_models)
n_cols = 4
n_rows = (n_cal + n_cols - 1) // n_cols
fig, axes = plt.subplots(n_rows, n_cols, figsize=(20, 5 * n_rows))
axes = axes.flatten()

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
brier_by_model = {name: [] for name in [
    "Elo", "SSM v2-UW-UA", "SSM v2-NT-UA", "Ensemble"
]}

for i in range(len(sigma_bins) - 1):
    mask = sigma_bin_idx == i
    if mask.sum() >= 50:
        sigma_labels.append(f"σ∈[{sigma_bins[i]:.0f},{sigma_bins[i+1]:.0f})")
        for name in brier_by_model:
            brier_by_model[name].append(brier_score(y[mask], models[name][mask]))

x_pos = np.arange(len(sigma_labels))
n_models = len(brier_by_model)
width = 0.8 / n_models
colors = {
    "Elo": "coral", "SSM v2-UW-UA": "seagreen",
    "SSM v2-NT-UA": "steelblue", "Ensemble": "mediumpurple",
}

for j, (name, briers) in enumerate(brier_by_model.items()):
    offset = (j - (n_models - 1) / 2) * width
    ax.bar(x_pos + offset, briers, width, alpha=0.7,
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
# 7b) σ constancy diagnostic — is uncertainty genuinely varying?
# ---------------------------------------------------------------------------
# Individual σ (not combined) by experience tier
eval_df["_diag_tier"] = eval_df["ssm2_game_number"].apply(
    lambda g: "01: 1-30" if g <= 30
    else "02: 31-100" if g <= 100
    else "03: 101-300" if g <= 300
    else "04: 301+"
)

print(f"\n{'='*100}")
print("σ CONSTANCY DIAGNOSTIC — SSM v2-UW (unweighted-tuned)")
print(f"{'='*100}")

# Individual σ_before stats by tier
print(f"\n--- Individual σ_before by experience tier ---")
print(f"{'Tier':<15} {'N':>8}  {'Mean':>8} {'Std':>8} {'P10':>8} {'P25':>8} "
      f"{'P50':>8} {'P75':>8} {'P90':>8} {'Min':>8} {'Max':>8}")
print(f"{'-'*15} {'-'*8}  {'-'*8} {'-'*8} {'-'*8} {'-'*8} "
      f"{'-'*8} {'-'*8} {'-'*8} {'-'*8} {'-'*8}")

for tier in sorted(eval_df["_diag_tier"].unique()):
    tmask = eval_df["_diag_tier"] == tier
    s = eval_df.loc[tmask, "ssm2_uw_sigma_before"].values
    pcts = np.percentile(s, [10, 25, 50, 75, 90])
    print(f"{tier:<15} {tmask.sum():>8}  {s.mean():8.2f} {s.std():8.2f} "
          f"{pcts[0]:8.2f} {pcts[1]:8.2f} {pcts[2]:8.2f} "
          f"{pcts[3]:8.2f} {pcts[4]:8.2f} {s.min():8.2f} {s.max():8.2f}")

# σ for veterans specifically — how tight is the distribution?
vet_mask = eval_df["ssm2_game_number"] > 300
vet_sigma = eval_df.loc[vet_mask, "ssm2_uw_sigma_before"].values
print(f"\nVeterans (301+ games): n={vet_mask.sum()}, "
      f"σ mean={vet_sigma.mean():.2f}, std={vet_sigma.std():.2f}, "
      f"CV={vet_sigma.std()/vet_sigma.mean()*100:.1f}%")

# UA shrinkage factor distribution — how much does UA actually change predictions?
sigma2_comb_uw = (eval_df["ssm2_uw_sigma_before"].values ** 2
                  + eval_df["ssm2_uw_opp_sigma_before"].values ** 2)
scale_factors = np.sqrt(1.0 + UA_COEFF * sigma2_comb_uw)

print(f"\n--- UA scale factor distribution (higher = more shrinkage toward 0.5) ---")
print(f"{'Tier':<15} {'Mean':>8} {'Std':>8} {'P10':>8} {'P50':>8} {'P90':>8}")
print(f"{'-'*15} {'-'*8} {'-'*8} {'-'*8} {'-'*8} {'-'*8}")

for tier in sorted(eval_df["_diag_tier"].unique()):
    tmask = eval_df["_diag_tier"] == tier
    sf = scale_factors[tmask.values]
    pcts = np.percentile(sf, [10, 50, 90])
    print(f"{tier:<15} {sf.mean():8.4f} {sf.std():8.4f} "
          f"{pcts[0]:8.4f} {pcts[1]:8.4f} {pcts[2]:8.4f}")

print(f"\nOverall: scale factor mean={scale_factors.mean():.4f}, "
      f"std={scale_factors.std():.4f}, "
      f"range=[{scale_factors.min():.4f}, {scale_factors.max():.4f}]")

# UA Brier improvement by σ quintile — where is UA actually helping?
print(f"\n--- UA Brier improvement by σ_combined quintile ---")
print(f"  (Positive = UA is better than raw)")
print(f"{'σ quintile':<25} {'N':>8}  {'Brier raw':>10} {'Brier UA':>10} {'Δ':>10}")
print(f"{'-'*25} {'-'*8}  {'-'*10} {'-'*10} {'-'*10}")

sigma_comb_vals = eval_df["ssm2_uw_combined_sigma"].values
for lo_pct, hi_pct in [(0, 20), (20, 40), (40, 60), (60, 80), (80, 100)]:
    lo = np.percentile(sigma_comb_vals, lo_pct)
    hi = np.percentile(sigma_comb_vals, hi_pct)
    if hi_pct == 100:
        mask = (sigma_comb_vals >= lo)
    else:
        mask = (sigma_comb_vals >= lo) & (sigma_comb_vals < hi)
    if mask.sum() < 50:
        continue
    br_raw = brier_score(y[mask], models["SSM v2-UW"][mask])
    br_ua = brier_score(y[mask], models["SSM v2-UW-UA"][mask])
    label = f"Q{lo_pct//20+1}: σ∈[{lo:.1f},{hi:.1f})"
    print(f"{label:<25} {mask.sum():>8}  {br_raw:10.5f} {br_ua:10.5f} "
          f"{br_raw - br_ua:+10.5f}")

# Histogram of individual σ_before
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

ax1.hist(eval_df["ssm2_uw_sigma_before"].values, bins=50, alpha=0.7,
         color="steelblue", edgecolor="white")
ax1.set_xlabel("σ_before (individual coach)")
ax1.set_ylabel("Count")
ax1.set_title("Distribution of SSM v2-UW σ_before in test set")
ax1.axvline(x=eval_df["ssm2_uw_sigma_before"].median(), color="coral",
            linestyle="--", label=f"Median={eval_df['ssm2_uw_sigma_before'].median():.1f}")
ax1.legend()
ax1.grid(True, alpha=0.3)

# Scale factor by σ_combined
ax2.scatter(sigma_comb_vals, scale_factors, alpha=0.05, s=2, color="steelblue")
ax2.set_xlabel("σ_combined (self + opponent)")
ax2.set_ylabel("UA scale factor")
ax2.set_title("UA shrinkage factor vs combined uncertainty")
ax2.grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

eval_df.drop(columns=["_diag_tier"], inplace=True)

# ---------------------------------------------------------------------------
# 8) Per-tier breakdown
# ---------------------------------------------------------------------------
eval_df["tier"] = eval_df["ssm2_game_number"].apply(
    lambda g: "01: 1-30" if g <= 30
    else "02: 31-100" if g <= 100
    else "03: 101-300" if g <= 300
    else "04: 301+"
)

tier_model_names = ["Elo", "SSM v2-UW-UA", "SSM v2-NT-UA", "Ensemble"]

print(f"\n{'='*140}")
print("METRICS BY EXPERIENCE TIER (unweighted evaluation)")
print(f"{'='*140}")

# Log-loss table
print(f"\n--- Log-loss by tier ---")
header = f"{'Tier':<15} {'N':>8}"
for mn in tier_model_names:
    header += f"  {mn:>12}"
print(header)
print(f"{'-'*15} {'-'*8}" + f"  {'-'*12}" * len(tier_model_names))

for tier in sorted(eval_df["tier"].unique()):
    tmask = eval_df["tier"] == tier
    y_t = eval_df.loc[tmask, "result_numeric"].values
    n_t = tmask.sum()
    row = f"{tier:<15} {n_t:>8}"
    for name in tier_model_names:
        p_t = np.clip(models[name][tmask.values], EPS, 1 - EPS)
        row += f"  {log_loss(y_t, p_t):12.5f}"
    print(row)

# Brier table
print(f"\n--- Brier score by tier ---")
header = f"{'Tier':<15} {'N':>8}"
for mn in tier_model_names:
    header += f"  {mn:>12}"
print(header)
print(f"{'-'*15} {'-'*8}" + f"  {'-'*12}" * len(tier_model_names))

for tier in sorted(eval_df["tier"].unique()):
    tmask = eval_df["tier"] == tier
    y_t = eval_df.loc[tmask, "result_numeric"].values
    n_t = tmask.sum()
    row = f"{tier:<15} {n_t:>8}"
    for name in tier_model_names:
        p_t = np.clip(models[name][tmask.values], EPS, 1 - EPS)
        row += f"  {brier_score(y_t, p_t):12.5f}"
    print(row)

# ---------------------------------------------------------------------------
# 9) In-sample / out-of-sample check
# ---------------------------------------------------------------------------
coach_first_game_in_test = (
    eval_df.groupby("coach_id")["ssm2_game_number"].min().reset_index()
)
coach_first_game_in_test.columns = ["coach_id", "first_test_game_number"]

eval_df = eval_df.merge(coach_first_game_in_test, on="coach_id", how="left")
eval_df["tuning_population"] = eval_df["first_test_game_number"].apply(
    lambda g: "In-sample (301+ before test)" if g > 301
    else "Out-of-sample (≤301 before test)"
)

oos_model_names = ["Elo", "SSM v2-UW-UA", "SSM v2-NT-UA", "Ensemble"]

print(f"\n{'='*120}")
print("IN-SAMPLE vs OUT-OF-SAMPLE (tuner targeted 301+ coaches)")
print(f"{'='*120}")

y_all = eval_df["result_numeric"].values
models_aligned = {
    "Elo":          np.clip(eval_df["elo_pred"].values, EPS, 1 - EPS),
    "SSM v2-UW-UA": np.clip(eval_df["ssm2_uw_ua_pred"].values, EPS, 1 - EPS),
    "SSM v2-NT-UA": np.clip(eval_df["ssm2_nt_ua_pred"].values, EPS, 1 - EPS),
    "Ensemble":     np.clip(eval_df["ensemble_pred"].values, EPS, 1 - EPS),
}

# Log-loss
header = f"\n{'Population':<35} {'N':>8}"
for mn in oos_model_names:
    header += f"  {mn + ' LL':>14}"
print(header)
print(f"{'-'*35} {'-'*8}" + f"  {'-'*14}" * len(oos_model_names))

for pop in sorted(eval_df["tuning_population"].unique()):
    pmask = (eval_df["tuning_population"] == pop).values
    y_p = y_all[pmask]
    n_p = pmask.sum()
    row = f"{pop:<35} {n_p:>8}"
    for name in oos_model_names:
        row += f"  {log_loss(y_p, models_aligned[name][pmask]):14.5f}"
    print(row)

# Brier
header = f"\n{'Population':<35} {'N':>8}"
for mn in oos_model_names:
    header += f"  {mn + ' Br':>14}"
print(header)
print(f"{'-'*35} {'-'*8}" + f"  {'-'*14}" * len(oos_model_names))

for pop in sorted(eval_df["tuning_population"].unique()):
    pmask = (eval_df["tuning_population"] == pop).values
    y_p = y_all[pmask]
    n_p = pmask.sum()
    row = f"{pop:<35} {n_p:>8}"
    for name in oos_model_names:
        row += f"  {brier_score(y_p, models_aligned[name][pmask]):14.5f}"
    print(row)

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

SSM v2 params (weighted):   σ²_obs=0.10,   q_time=2.0, q_game=0.025, v_scale=24.0
SSM v2-UW params (unwtd):  σ²_obs=0.125,  q_time=2.0, q_game=0.025, v_scale=16.0

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
  5. Does unweighted tuning improve calibration vs weighted tuning?
     → Compare SSM v2-UA vs SSM v2-UW-UA calibration and tier breakdown.
""")
print(f"{'='*78}")

# ---------------------------------------------------------------------------
# 11) Store results — Delta table, CSV, text report, plots
# ---------------------------------------------------------------------------

run_timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")

# 11.1) Store metrics in Delta table
metrics_rows = []
for name, m in metrics.items():
    metrics_rows.append({
        "run_timestamp": run_timestamp,
        "model_name": name,
        "log_loss": float(m["log_loss"]),
        "brier_score": float(m["brier"]),
        "accuracy": float(m["accuracy"]),
        "test_cutoff_date_id": eval_cutoff,
        "test_months": EVAL_TEST_MONTHS,
        "n_observations": len(eval_df),
        "n_games": int(eval_df["game_id"].nunique()),
        "n_coaches": int(eval_df["coach_id"].nunique()),
    })

metrics_spark_df = spark.createDataFrame(metrics_rows)
# Ensure target schema exists (this notebook can be run before 331 creates it)
spark.sql("CREATE SCHEMA IF NOT EXISTS naf_catalog.gold_summary")
metrics_spark_df.write.mode("append").saveAsTable("naf_catalog.gold_summary.ssm_evaluation_metrics")
print(f"\n✓ Metrics stored in naf_catalog.gold_summary.ssm_evaluation_metrics")

# 11.2) Create Volume and run output directory
spark.sql("CREATE VOLUME IF NOT EXISTS naf_catalog.gold_summary.exports")
model_tests_dir = "/Volumes/naf_catalog/gold_summary/exports/model_tests"
run_dir = f"{model_tests_dir}/ssm_eval_{run_timestamp}"
os.makedirs(run_dir, exist_ok=True)

# 11.3) Export metrics CSV
metrics_spark_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{run_dir}/_csv_tmp")
part_files = [f.path for f in dbutils.fs.ls(f"{run_dir}/_csv_tmp")
              if f.name.startswith("part-") and f.name.endswith(".csv")]
if part_files:
    dbutils.fs.cp(part_files[0], f"{run_dir}/metrics.csv", True)
dbutils.fs.rm(f"{run_dir}/_csv_tmp", recurse=True)
print(f"✓ CSV: {run_dir}/metrics.csv")

# 11.4) Export text report with full evaluation results
report_lines = []
report_lines.append(f"SSM MODEL EVALUATION REPORT")
report_lines.append(f"Run: {run_timestamp}")
report_lines.append(f"Validation: {val_cutoff} to {test_cutoff} ({EVAL_VAL_MONTHS} months)")
report_lines.append(f"Test:       date_id >= {test_cutoff} ({EVAL_TEST_MONTHS} months)")
report_lines.append(f"Observations: {len(eval_df)} ({eval_df['game_id'].nunique()} games, "
                    f"{eval_df['coach_id'].nunique()} coaches)")
report_lines.append(f"UA coefficient: {UA_COEFF:.6f}")
report_lines.append("")

# Overall comparison
report_lines.append(f"{'='*78}")
report_lines.append(f"OVERALL COMPARISON (test set)")
report_lines.append(f"{'='*78}")
report_lines.append(f"{'Model':<15} {'Log-loss':>10} {'Brier':>10} {'Accuracy':>10}  "
                    f"{'dLL vs Elo':>11} {'dBr vs Elo':>11}")
for name, m in metrics.items():
    ll_d = elo_ll - m["log_loss"]
    br_d = elo_br - m["brier"]
    dll = f"{ll_d:+11.5f}" if name != "Elo" else f"{'---':>11}"
    dbr = f"{br_d:+11.5f}" if name != "Elo" else f"{'---':>11}"
    report_lines.append(f"{name:<15} {m['log_loss']:10.5f} {m['brier']:10.5f} "
                        f"{m['accuracy']:10.3f}  {dll} {dbr}")
report_lines.append("")

# Ablation
report_lines.append(f"ABLATION: time-awareness (q_time)")
report_lines.append(f"  SSM v2-UW-UA Brier = {metrics['SSM v2-UW-UA']['brier']:.5f}")
report_lines.append(f"  SSM v2-NT-UA Brier = {metrics['SSM v2-NT-UA']['brier']:.5f}  (q_time=0)")
uw_b = metrics["SSM v2-UW-UA"]["brier"]
nt_b = metrics["SSM v2-NT-UA"]["brier"]
report_lines.append(f"  Delta = {nt_b - uw_b:+.5f}  "
                    f"({'time helps' if uw_b < nt_b else 'time does not help'})")
report_lines.append("")

# Bootstrap CIs
report_lines.append(f"BOOTSTRAP CONFIDENCE INTERVALS ({N_BOOTSTRAP} game-level resamples)")
for (model_a, model_b), br in boot_results.items():
    sig = "*" if (br["ci_lo"] > 0 or br["ci_hi"] < 0) else ""
    report_lines.append(f"  {model_a} vs {model_b}:")
    report_lines.append(f"    dBrier = {br['obs']:+.5f}  "
                        f"95% CI [{br['ci_lo']:+.5f}, {br['ci_hi']:+.5f}] {sig}")
    report_lines.append(f"    P({model_a} better) = {br['p_pos']:.1%}")
report_lines.append("  * = significant at alpha=0.05")
report_lines.append("")

# Parameters
report_lines.append(f"PARAMETERS")
report_lines.append(f"  SSM v2 (weighted):   s2_obs=0.10   q_time=2.0  q_game=0.025  v_scale=24.0")
report_lines.append(f"  SSM v2-UW (unwtd):   s2_obs=0.125  q_time=2.0  q_game=0.025  v_scale=16.0")
report_lines.append(f"  SSM v2-NT (ablation): s2_obs=0.125  q_time=0.0  q_game=0.025  v_scale=16.0")

report_text = "\n".join(report_lines)

report_path = f"{run_dir}/evaluation_report.txt"
with open(report_path, "w") as f:
    f.write(report_text)
print(f"✓ Report: {report_path}")

# 11.5) Save plots as PNGs
# Re-generate calibration plot for saving
fig_cal, axes_cal = plt.subplots(n_rows, n_cols, figsize=(20, 5 * n_rows))
axes_cal = axes_cal.flatten()
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
    ax.plot([0, 1], [0, 1], "k--", alpha=0.5)
    ax.bar(bc, ba, width=0.08, alpha=0.6, color="steelblue")
    ax.scatter(bc, ba, color="coral", zorder=5, s=40)
    ax.set_xlabel("Predicted")
    ax.set_ylabel("Actual")
    ax.set_title(f"{name}")
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.grid(True, alpha=0.3)
for j in range(len(cal_models), len(axes_cal)):
    axes_cal[j].set_visible(False)
fig_cal.suptitle("Calibration Comparison", fontsize=14, y=1.02)
fig_cal.tight_layout()
fig_cal.savefig(f"{run_dir}/calibration_plots.png", dpi=150, bbox_inches="tight")
plt.close(fig_cal)
print(f"✓ Plot: {run_dir}/calibration_plots.png")

print(f"\n{'='*78}")
print(f"EXPORT COMPLETE — {run_dir}")
print(f"{'='*78}")
print(f"  Delta table : naf_catalog.gold_summary.ssm_evaluation_metrics")
print(f"  metrics.csv           — overall model comparison")
print(f"  evaluation_report.txt — full results with ablation + bootstrap CIs")
print(f"  calibration_plots.png — calibration comparison chart")
print(f"{'='*78}")


# COMMAND ----------

# DBTITLE 1,Export Coach Plots

#DenmarkEB2026:
DenmarkEB2026 = [11501, 11499, 29078, 17846, 24222, 29046, 34738, 9524]

#DenmarkEO2026:
DenmarkEO2026 = [30735, 33476, 21295, 7957]

#Chimera:
Chimera =       [2785, 	14113, 25971, 13583, 12319, 20685, 9524]

export_coach_plots(DenmarkEB2026, "DenmarkEB2026")
export_coach_plots(DenmarkEO2026, "DenmarkEO2026")
export_coach_plots(Chimera, "Chimera")
