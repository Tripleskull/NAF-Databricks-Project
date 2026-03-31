# NAF Project — Status

> Last updated: 2026-03-31

---

## Pipeline Maturity

| Domain | Summary | Presentation | Dashboard | Status |
|---|---|---|---|---|
| Coach | 331 (mature) | 340, 341 (mature) | Coach Dashboard (polished) | **Near-final** |
| Nation | 332 (functional) | 342 (functional) | Nation Dashboard (in progress) | **In progress** |
| Tournament | 333 (placeholder) | 343 (placeholder) | — | **Placeholder** |
| Race | 334 (empty) | 344 (empty) | — | **Not started** |
| SSM Ratings | 321 (v1 + v2 complete) | — | — | **Complete** |
| Race-Aware Ratings | 323 (Stage 1 complete) | — | — | **Research / evolving** |

Core layers (Bronze 100, Silver 200, Gold dim 310, Gold fact 320) are stable and shared across all domains.

---

## Nation Dashboard Expansion Plan Progress

| Phase | Description | Status |
|---|---|---|
| 0 | Cleanup & consolidation | ✅ Complete |
| 1 | Opponent strength + form score | ✅ Complete |
| 2 | Nation cumulative WDL series | ✅ Complete |
| 3 | Race strength comparisons | ✅ Complete |
| 4 | Home/abroad + specialist badges | ✅ Complete |
| 5 | Opponent-adjusted win rates | ✅ Complete (not yet run in Databricks) |
| 6 | Team selector + power ranking | ⏸️ Pending |
| 7 | Global status report | ⏸️ Pending |
| 8 | Coach dashboard updates | ⏸️ Pending |
| 9 | Testing + documentation | ⏸️ Pending |

See `Expansion_Plan.md` for full phase specifications and implementation order.

---

## Infrastructure

### Centralised Parameter Table

All tuneable analytical parameters are stored in `naf_catalog.gold_dim.analytical_config` — a singleton table defined in 310. Notebooks 320, 331, and 332 read from this table via a `cfg` CTE (SQL) or `spark.table().first()` (Python). No hardcoded magic numbers remain in the pipeline code.

See `Analytical_Parameters.md` for the full parameter reference and change protocol.

---

## SSM Rating Engine (321)

**Status: v1 and v2 implemented and tuned**

Two SSM variants implemented as EKF-based rating engines in `321_NAF_gold_fact_ssm.py`:

- **SSM v1** (`ssm_rating_history_fact`): Game-indexed random walk (φ=1.0, no mean reversion). Tuned params: σ²_process=2.0, σ²_obs=0.02, prior_σ=50.
- **SSM v2** (`ssm2_rating_history_fact`): Time-aware uncertainty growth + adaptive volatility via EWMA of squared innovations. Tuned params: σ²_obs=0.10, q_time=2.0, q_game=0.025, v_scale=24.0.

Tuning used a two-pass grid search (coarse 256 + fine 81 combinations) against a coverage metric targeting ~95% of games where 50-game rolling median Elo falls inside SSM ±2σ.

**Design doc**: `archive/SSM_Design.md`.

---

## Race-Aware Rating Engine (323)

**Status: Stage 1 implemented, research ongoing**

Decomposes coach strength as `θ = g + d` where `g` is global skill and `d` is a race-specific deviation. Stage 1 uses independent race deviations (no cross-race covariance), updated jointly via a 2D EKF step. Evaluated in 324 (research notebook, not part of production pipeline).

Future directions under consideration: correlated prior / independent update hybrid, low-rank coach × race factor model, hierarchical shrinkage with race main effects, selection diagnostics for g.

**Design doc**: `archive/Race_Rating_Design.md`.

---

## Design Documentation Status

| Document | Status | Notes |
|---|---|---|
| `NAF_Design_Specification.md` | **Current** | Consolidated architecture spec. Single source of truth for modelling rules. |
| `Analytical_Parameters.md` | **Current** | All tuneable parameters + config column mapping + change protocol. |
| `Pipeline_Object_Index.md` | **Current** | Logical ordering and dependency map for all summary/presentation objects. |
| `style_guides.md` | **Current** | SQL/PySpark formatting rules. |
| `Expansion_Plan.md` | **Current** | 9-phase nation dashboard + feature roadmap. |
| `Nation_Dashboard_Plan.md` | **Current** | Widget-by-widget nation dashboard design spec. |
| `archive/SSM_Design.md` | **Reference** | SSM design document (options analysis, v1 + v2 specs, tuning lessons). |
| `archive/Race_Rating_Design.md` | **Reference** | Race-aware rating design (Stage 1 implemented, future directions). |

---

## Open Issues

1. **Nation dashboard parameter inconsistency**: Uses `:nation_name_display`, `:nation_name`, and `:nation_clean` inconsistently — should settle on one.
2. **Nation dashboard metric_type values**: Near-duplicate queries use `'glo_peak'` vs `'PEAK'` — needs alignment.
3. **Nation rivalry_summary duplicate**: 332 creates `nation_rivalry_summary` as a TABLE then immediately overwrites it with a VIEW of the same name. Needs resolution.
4. **Tournament/race placeholders**: 333, 334, 343, 344 are placeholder or empty notebooks — not bugs, just unbuilt.
