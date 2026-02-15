# NAF Data Platform - Code Review

**Reviewed:** February 2026
**Scope:** Design documents (00-14), Bronze through Gold Presentation notebooks, test suite

---

## Overall Assessment

This is an impressively well-architected data engineering project. The medallion architecture is thoughtfully designed, the documentation is thorough and internally consistent, and the code quality is high throughout. The separation of concerns across layers is among the best I've seen for a project of this scale.

That said, I found a handful of issues worth addressing - two critical, a few medium, and several minor polish items. None of them undermine the fundamental architecture.

---

## What's Working Well

**Documentation as source of truth.** The design docs form a coherent, layered system where `00_design_decisions.md` explicitly wins over everything else, and each doc cross-references the others. The "update decisions here first, then align implementations" discipline is exactly right for preventing drift.

**Silver as the contract gatekeeper.** Silver owns dedup, key integrity, NOT NULL enforcement, and semantic exclusions. Gold layers trust Silver and avoid redundant defensive checks - this is clean and prevents the "every layer re-filters everything" anti-pattern.

**Deterministic surrogate keys.** The `PMOD(XXHASH64(group_key), 2147483647)` approach for `nation_id` is clever and reproducible. The hash collision check in the validation suite is a nice safety net.

**Spine/series/summary decomposition in Gold.** The coach summary notebook (331) is particularly well-structured - scaffolding views with no filtering, cumulative series for time-indexed analysis, and materialized summaries that apply thresholds only at the final step.

**The Elo engine.** Computing ratings sequentially on the driver with deterministic ordering (via `game_index`) is the correct approach for this type of stateful computation. The fail-fast validation and the debug diagnostic cell that follows are both practical additions.

**The test suite.** `Tests and admining.py` covers the important invariants - PK uniqueness, FK coverage, NOT NULL keys, self-play exclusion, scope/race_id consistency, and even checks that summary tables don't contain display columns. The ERROR/WARN severity distinction is useful.

**Presentation identity shims.** The `340_core` notebook creates clean, reusable identity views (`coach_identity_v`, `nation_identity_v`, etc.) that every downstream presentation view joins to. This prevents display attribute duplication and keeps the pattern DRY.

---

## Critical Issues

### 1. Display columns leaked into `gold_summary.tournament_winners` (notebook 333)

The `tournament_winners` table includes `tournament_name`, `coach_name`, and `race_name` directly in the summary layer. This violates the core architecture rule that summary tables contain only IDs and metrics, with display attributes added in presentation via joins.

**Why it matters:** If a coach or tournament name changes (SCD1 update), the summary table becomes stale while presentation views would auto-refresh. It also means the test in `Tests and admining.py` that checks for display columns in `gold_summary` should flag this.

**Fix:** Remove name columns from `tournament_winners`. Keep only `tournament_id`, `winner_coach_id`, `winner_race_id`, `winner_date`. Create a corresponding `gold_presentation.tournament_winners` view that joins the identity shims.

### 2. String-based nation join in `gold_summary.tournament_nation_summary` (notebook 333)

This table uses `nation_name` (a string) as its grain and joins to `nation_dim` via `UPPER(TRIM(tns.nation_name)) = UPPER(TRIM(n.nation_name))`. This is fragile - case/whitespace differences, accent marks, or name updates would break the join silently.

**Why it matters:** Every other table in the project uses `nation_id` as the canonical FK, as the design docs require. This is the one place where the pattern breaks, and it cascades into `343_tournament_presentation` which inherits the string-based join.

**Fix:** Rewrite `tournament_nation_summary` to use `nation_id` from `coach_dim` (via the coach-tournament participation data). Replace the string grain with `(tournament_id, nation_id)`.

---

## Medium Issues

### 3. Column proliferation in `coach_profile` (notebook 341)

The `coach_profile` presentation view has 60+ columns, including flattened `race_1/2/3_*` slots and multiple timestamp variants for the same event. This makes the contract hard to maintain and brittle if the top-N race logic changes.

**Suggestion:** Split into focused views - `coach_profile_core` (~15-20 columns for identity, overall stats, rating), `coach_top_races` (one row per coach/race rank), and `coach_rating_snapshot` (current + peak + phase metrics). Dashboards can join what they need.

### 4. Parameterized query in presentation layer (notebook 342)

`nation_glo_metric_quantiles` uses Databricks `:parameter` syntax for dynamic metric selection. While this works in the Databricks dashboard context, it couples the data layer to a specific execution environment. If you later export to Streamlit or static HTML (as the design docs anticipate), this won't work.

**Suggestion:** Either pre-compute all metric variants (PEAK/MEAN/MEDIAN x ALL/POST_THRESHOLD/LAST_50) as separate rows with a `metric_type`/`metric_phase` column, or move the parameterized logic to the dashboard query layer and keep the presentation view static.

### 5. Empty notebooks (334, 344)

`334_NAF_gold_summary_race.py` and `344_NAF_gold_presentation_race.py` are empty placeholders. The `Nation_plan.md` references race-level analytics and a race Elo bin scheme that would presumably live here.

**Suggestion:** Either implement these or remove the files and add a note in the design docs that race presentation is deferred. Empty files create ambiguity about what's implemented vs. planned.

---

## Minor Issues and Polish

### Bronze layer

The Bronze notebook is clean and consistent. Every table gets the same `ingest_timestamp`/`ingest_source`/`file_name` metadata, `_rescued_data` for schema evolution, and explicit schema declarations for CSVs. Two small notes:

- The `coach_export_raw` table doesn't declare an explicit schema (unlike the other CSVs), relying on header inference. Consider adding one for consistency.
- The FIFA country codes ingestion uses PySpark with `windows-1252` decoding and regex parsing - this is more complex than the SQL-based ingestions but justified given the HTML source format.

### Silver layer

The Silver notebook is the strongest in the project. The nation entity resolution pipeline (auto-map, manual-map, resolved, grouped, dedup, final + unknown_row) is sophisticated but well-commented. A few observations:

- **Double dedup in `country_reference`:** The FIFA and ISO sources are already deduped in their own Silver tables (`country_codes_fifa`, `country_codes_iso`), but `country_reference` re-deduplicates them. This is harmless but redundant - the WHERE + QUALIFY pattern could be simplified to a direct join since uniqueness is already guaranteed upstream.
- **`coach_name` cleaning logic** is repeated verbatim between `coaches_clean` and `tournaments_clean` (the "preserve-but-clean" CASE block). Consider extracting this into a SQL function or at least a comment noting the shared pattern.
- **Variant filter `IN (1, 13, 15)`** appears in Bronze, Silver, and Gold. It's documented but could benefit from a named constant or a Silver reference table.

### Gold Dim layer

Straightforward and minimal, as designed. The `tournament_parameters` helper table is a well-documented exception to the "dims are entities only" rule.

- `tournament_stat_dim` uses `SELECT DISTINCT` rather than the dedup pattern used elsewhere. Since Silver already guarantees uniqueness on `stat_id`, this works, but it's an inconsistency in style.

### Gold Fact layer

- The `game_global_order_spine_v` view's `COALESCE(event_timestamp, TO_TIMESTAMP(CAST(date_id AS STRING), 'yyyyMMdd'))` fallback is a pragmatic choice, but worth noting that games on the same date with NULL timestamps will be ordered only by `game_id`, which may not reflect actual play order.
- The Elo engine collects all rows to the driver (`feed_df.collect()`). This is correct for sequential state computation but will eventually hit memory limits as the dataset grows. For now it's fine - NAF game volumes are manageable - but consider documenting the scaling boundary.

### Gold Summary layer

- `nation_overview_comparison_summary` (332) uses inconsistent dedup patterns - some use `QUALIFY ROW_NUMBER()`, others use CTE + WHERE. Standardizing on one pattern across all notebooks would improve readability.
- Shannon entropy and Gini coefficient calculations in `tournament_summary` (333) are correct but uncommented. A brief formula note would help future maintainers.

### Test suite

The test suite is thorough for core integrity but has gaps at the summary layer:

- No PK uniqueness checks for `gold_summary` tables
- No FK coverage checks between summary and dim tables
- No temporal sanity checks (negative date ranges, future dates)
- The `allowed_variant_ids` check hardcodes `(1, 13)` but the pipeline actually uses `(1, 13, 15)` - this may be intentional but should be verified

---

## Structural Observations

**Folder naming:** The `02 NAF Dashbords` folder has a typo ("Dashbords" vs "Dashboards") and is empty. Minor but worth fixing.

**Duplicate folder nesting:** Each top-level folder contains a duplicate-named subfolder (`00 NAF Project Design/00 NAF Project Design/`, `01 NAF Project Notebooks/01 NAF Project Notebooks/`). This looks like an export artifact. Consider flattening.

**File naming with ` (1)` suffix:** All design docs have ` (1)` in their filenames (e.g., `00_principles (1).md`). These appear to be duplicate-download artifacts and should be cleaned up.

**Manifest files:** The `manifest.mf` files contain GUID metadata but aren't referenced anywhere in the code. If they're Databricks workspace artifacts, they can probably be excluded from version control.

---

## Recommended Priority Order

1. Fix `tournament_winners` display column leak (critical, isolated fix)
2. Fix `tournament_nation_summary` string-based join (critical, cascading fix to 343)
3. Resolve empty notebooks 334/344 (clarify scope)
4. Split `coach_profile` into focused views (medium, improves maintainability)
5. Add summary-layer tests to the test suite (medium, improves safety net)
6. Clean up folder structure and file naming (minor, improves professionalism)
