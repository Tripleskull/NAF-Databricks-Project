# NAF Project — Status and Task Tracker

> Updated: 2026-02-13

---

## Pipeline Maturity

| Domain | Summary | Presentation | Dashboard | Status |
|---|---|---|---|---|
| Coach | 331 (mature) | 340, 341 (mature) | Coach Dashboard (polished) | **Near-final** |
| Nation | 332 (functional) | 342 (functional) | Nation Dashboard (early) | **In progress** |
| Tournament | 333 (placeholder) | 343 (placeholder) | — | **Placeholder** |
| Race | 334 (empty) | 344 (empty) | — | **Not started** |

Core layers (Bronze 100, Silver 200, Gold dim 310, Gold fact 320) are stable and shared across all domains.

---

## Infrastructure

### Centralised Parameter Table

All tuneable analytical parameters are stored in `naf_catalog.gold_dim.analytical_config` — a singleton table defined in 310. Notebooks 320, 331, and 332 read from this table via a `cfg` CTE (SQL) or `spark.table().first()` (Python). No hardcoded magic numbers remain in the pipeline code.

Covered parameters: Elo engine (initial rating, scale), tournament n_eff, burn-in thresholds (global, race), last-n window, minimum-games eligibility (race performance, nation GLO, nation race), rivalry scoring weights and cap. Change protocol documented in `Analytical_Parameters.md`.

---

## Design Documentation Status

| Document | Status | Notes |
|---|---|---|
| `NAF_Design_Specification.md` | **Current** | Consolidated spec replacing 9 individual docs. Validated against code. |
| `Analytical_Parameters.md` | **Current** | All tuneable numbers + config column mapping + change protocol. |
| `Pipeline_Object_Index.md` | **Current** | Logical ordering and dependency map for all 42+ summary/presentation objects. |
| `03_style_guides.md` | Unchanged | SQL/code formatting. Stays separate. |
| `Nation_plan.md` | Unchanged | Nation dashboard build plan. Retire after nation pipeline is finalised. |
| `Rating_System_Spec.md` | **Superseded** | Replaced by `Analytical_Parameters.md`. Can be deleted. |
| `00_principles.md` | **Superseded** | Replaced by `NAF_Design_Specification.md` §1. |
| `00_design_decisions.md` | **Superseded** | Replaced by `NAF_Design_Specification.md` §1–4. |
| `01_variable_naming.md` | **Superseded** | Replaced by `NAF_Design_Specification.md` §2. |
| `02_schema_design.md` | **Superseded** | Replaced by `NAF_Design_Specification.md` §1, §3. |
| `10–14 dimension specs` | **Superseded** | Replaced by `NAF_Design_Specification.md` §6. |
| `NAF Project Code Review.md` | Reference | Initial code review from first session. |

---

## Resolved Issues (this session)

1. **Test suite variant filter** — fixed: `(1, 13)` → `(1, 13, 15)`.
2. **Coach dashboard hardcoded ID** — fixed: `WHERE coach_id = 9524` → `WHERE coach_id = :coach_id`.
3. **coach_profile column count** — addressed: added `distinct_races_played`, `best_race_win_frac`, `best_race_win_pct` (moved aggregate computation out of dashboard into presentation layer). Header reformatted to standard GRAIN/PK/SOURCES pattern.
4. **Presentation `_v` suffix inconsistency** — fixed: `coach_streak_segments_v` → `coach_streak_detail`, `coach_streak_summary_v` → `coach_streak_overview`.
5. **Summary-layer PK tests** — fixed: uncommented and extended PK uniqueness tests for all 8 coach summary tables. Added FK coverage tests (coach_id → coach_dim).
6. **Hardcoded parameters across pipeline** — fixed: all tuneable numbers now read from `analytical_config` in 310, 320, 331, 332.

---

## Open Issues

1. **Nation dashboard parameter inconsistency**: Uses `:nation_name_display`, `:nation_name`, and `:nation_clean` inconsistently — should settle on one.
2. **Nation dashboard metric_type values**: Near-duplicate queries use `'glo_peak'` vs `'PEAK'` — needs alignment.
3. **Nation rivalry_summary duplicate**: 332 creates `nation_rivalry_summary` as a TABLE, then immediately overwrites it with a VIEW of the same name. Needs resolution.
4. **Tournament/race placeholders**: 333, 334, 343, 344 are placeholder or empty notebooks — not bugs, just unbuilt.

---

## Next Steps (suggested priority)

1. Finalise the nation pipeline (summary → presentation → dashboard), using `Nation_plan.md` as the build guide. Fix open issues 1–3 as part of this work.
2. Build tournament summary and presentation properly (replace placeholder 333/343).
3. Build race summary and presentation (334/344).
4. Retire `Nation_plan.md` and superseded design docs once nation is done.
5. Update `CLAUDE.md` with cross-session working instructions.
