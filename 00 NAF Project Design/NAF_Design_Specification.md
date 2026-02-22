# NAF Data Platform — Design Specification

> **Scope**: Architecture, naming, schema design, dimension contracts, and all modelling policies.
> **Excludes**: SQL/code style guide (`03_style_guides.md`), analytical parameters (`Analytical_Parameters.md`), nation dashboard build plan (`Nation_plan.md`).
> **Precedence**: This document is the single source of truth for modelling rules. If implementation diverges, either update this doc or plan a migration — do not let drift persist silently.

---

## 1. Architecture

### 1.1 Unity Catalog layout

All objects live under a single catalog with layer-specific schemas:

| Schema | Purpose |
|---|---|
| `naf_catalog.bronze` | Raw ingest |
| `naf_catalog.silver` | Canonical cleaning and normalisation |
| `naf_catalog.gold_dim` | Canonical business entities (+ parameter helpers) |
| `naf_catalog.gold_fact` | Event-level truth |
| `naf_catalog.gold_summary` | Aggregates, analytics, windows, trajectories |
| `naf_catalog.gold_presentation` | Thin dashboard contracts |

Always use fully-qualified names (e.g. `naf_catalog.gold_fact.games_fact`) to avoid confusion with legacy `workspace.*` objects.

### 1.2 Layer responsibilities

**Bronze** — Raw ingest for auditability and reproducibility.
Allowed: ingestion metadata (`ingest_timestamp`, `ingest_source`, `file_name`), `_rescued_data`, minimal technical renames to satisfy Spark/Delta identifiers, schema evolution. Not allowed: semantic transforms, canonicalisation, deduplication for meaning, analytics.

**Silver** — Canonical cleaned data model. The single owner of semantic validity filters, key typing, and deduplication.
Allowed: type/unit normalisation, canonical naming, stable IDs, reference mappings, semantic dedup and corrections (documented), key integrity enforcement.
Required: all emitted `*_id` columns NOT NULL (unless explicitly documented as optional), uniqueness at declared primary-key grain, documented semantic exclusions applied here (not downstream).
Not allowed: cross-event aggregates, analytics, rating computations beyond source-provided values.

**Gold_dim** — Canonical business entities, intentionally minimal and normalised. Store intrinsic attributes and FKs; no analytics content (ratings, aggregates, peaks, streaks). SCD policy: SCD Type 1 (current-state overwrite) unless explicitly decided otherwise.

**Gold_fact** — Event-level truth with explicit grain, PK, and FKs. Allowed: per-row derived fields, row-level flags, event-level rating values (before/after). Prohibited: multi-event aggregates, rolling windows, streaks, KPIs.

**Gold_summary** — All aggregates and analytics: totals, rates, distributions, streaks, peaks, rolling windows, rating snapshots and trajectories, entity-level and cross-entity summaries. Metric definitions live here; avoid re-defining metrics elsewhere.

**Gold_presentation** — Thin dashboard-oriented layer. Primarily joins (dims + summaries), light formatting, stable sorts/bucketing, display labels. Not allowed: heavy business logic, recomputation of metrics owned by `gold_summary`, re-canonicalisation.

### 1.3 Build dependency rule

Gold objects are built from Silver (and Gold dims where relevant), not directly from Bronze. Keep transformations close to their correct responsibility layer.

### 1.4 Data contracts and publishing strategy

Dashboards read from `gold_presentation` contracts. Contracts have stable grain, column names, and meanings so they can power Databricks dashboards now and be exported later for Streamlit or static HTML reporting.

### 1.5 Documentation as source of truth

This document is the modelling source of truth. If implementation diverges from docs, either update docs or plan a migration. Notebook/view headers should include lightweight contract notes (purpose, grain, inputs/outputs).

---

## 2. Naming Conventions

### 2.1 General rules

All table and column names use `snake_case`. Avoid abbreviations unless domain-standard (`elo`, `glo`, `iso`, `fifa`).

### 2.2 Table naming by layer

| Layer | Convention | Examples |
|---|---|---|
| Bronze | `*_raw` for direct ingest feeds | `coach_export_raw`, `game_raw`, `tournament_raw` |
| Silver | `*_clean` for cleaned datasets; entity/reference tables as needed | `games_clean`, `coaches_clean`, `nation_manual_map` |
| Gold dim | `*_dim` for dimensions; no `_dim` suffix on helpers | `coach_dim`, `nation_dim`, `tournament_parameters` |
| Gold fact | `*_fact` | `games_fact`, `coach_games_fact`, `rating_history_fact` |
| Gold summary | `*_summary` where natural | `coach_race_summary`, `nation_overview_summary` |
| Gold presentation | No suffix requirement; use semantic prefixes for dashboard groups | `coach_profile`, `nation_snapshot` |

### 2.3 Key naming

Primary keys: `<entity>_id` (never bare `id`). Foreign keys match the referenced PK name, or use a clearly documented role-based variant.

The suffix `_id` is **reserved for modelled entity keys**. Source/passthrough numeric fields must not end with `_id` — use e.g. `source_season_num`.

Canonical entity keys: `coach_id`, `nation_id`, `race_id`, `tournament_id`, `date_id`.

### 2.4 Role-specific keys

When two roles exist for the same entity in one row:

Games (two-role): `home_coach_id`, `away_coach_id`, `home_race_id`, `away_race_id`, `td_home`, `td_away`.

Per-coach perspective (coach vs opponent): `coach_id`, `opponent_coach_id`, `race_id` (coach's race), `opponent_race_id`.

Do not introduce `coach_race_id` unless explicitly decided and documented.

### 2.5 Date and timestamp naming

Date dimension key: `date_id` (INT, `YYYYMMDD` format). Introduced in Silver; NOT NULL wherever emitted as a FK.

Domain event time: `*_date` for DATE (e.g. `game_date`), `*_timestamp` for TIMESTAMP (e.g. `event_timestamp`).

Audit timestamps (canonical): `ingest_timestamp` (raw ingest time), `load_timestamp` (table write/load time).

**Do not introduce `*_datetime` columns** in Silver or downstream. If upstream input uses `*_datetime`, alias to `*_timestamp`.

### 2.6 Boolean naming

Use `is_` prefix: `is_major_tournament`, `is_valid_glo`, `is_valid_elo`, `is_valid_min_games_race_performance`. Avoid boolean suffix naming (e.g. `major_boolean`).

### 2.7 Rating field naming

Silver may contain source-provided rating fields (typed/normalised): e.g. `elo_home_before`, `elo_away_before`, `elo_home_after`, `elo_away_after`.

Gold fact rating tables use standardised generic columns: `rating_before`, `rating_after`, `rating_delta`, plus `scope` and `rating_system`.

---

## 3. Modelling Discipline

### 3.1 Object documentation requirement

Every Gold object must be explicit about: grain (one sentence), primary key or uniqueness rule, foreign keys (to Gold dimensions), what is allowed vs prohibited content for the layer.

### 3.2 Silver key integrity (Gold trust contract)

Silver guarantees key integrity. Gold layers (dim/fact/summary/presentation) assume this guarantee and should avoid redundant `IS NOT NULL` checks and `ROW_NUMBER()`-based dedup unless a real risk is demonstrated.

For games specifically, Silver must exclude: invalid/self-play games (`home_coach_id = away_coach_id`), test coach records (`coach_id = 9`), non-standard variant games (only `variant_id IN (1, 13, 15)` pass through), and any other documented exclusions.

### 3.3 Gold_dim helper parameter tables (exception)

Deterministic "parameter lookup" tables may live in `gold_dim` as an explicit exception when: grain is 1 row per `<entity>_id` (lookup-like), outputs are not KPIs for reporting and contain no display attributes, logic is deterministic and idempotent from canonical inputs, and purpose is to provide stable parameters/config used by downstream pipelines.

Example: `naf_catalog.gold_dim.tournament_parameters` — tournament-level rating parameters derived from games participation + `is_major_tournament`.

---

## 4. Semantic Policies (Data Rules)

### 4.1 Variant filter

Only NAF standard variant games are included in the analytical pipeline. The canonical filter is `variant_id IN (1, 13, 15)`, applied in Silver (`games_clean`). All downstream layers inherit this filter.

### 4.2 Coach exclusions

`coach_id = 9` is a known test/admin account and is excluded from analytical outputs. Exclusion is applied in Silver.

### 4.3 Bretonnians exception

`race_id = 26` (Bretonnians) is explicitly included in race-level analytics despite its `race_is_counted` flag being false in source data. This is an intentional override: Bretonnians are a legitimate legacy race that should appear in race-level analysis. The exception is coded in Silver.

### 4.4 Self-play exclusion

Games where `home_coach_id = away_coach_id` are excluded in Silver. This is a data quality filter, not an edge case to handle downstream.

### 4.5 Unknown nation policy

Unmapped or invalid source nations are collapsed to a single canonical nation: `nation_id = 0`, `nation_name = 'Unknown'`, `nation_name_display = 'Unknown'`. All code fields are NULL, `flag_code` is NULL. Presentation layers render a globe icon (🌐) when `flag_code` is NULL.

### 4.6 REST_OF_WORLD (ROW) definition

Given a selected nation N, ROW = opponent coaches where `opponent_nation_id != N`. Whether `nation_id = 0` (Unknown) is included in ROW is a configurable option (see Nation_plan.md Section 3.1 for dashboard controls).

---

## 5. Gold Summary Sub-patterns

The coach summary pipeline (the most mature implementation) establishes canonical sub-patterns that other summary domains should follow.

### 5.1 Spines

Spines are minimal scaffolding views (suffix `_spine_v`) that define the grain of an analytical object without computing metrics. They establish which combinations of keys exist for downstream joins and windows.

Materialisation: **views** (suffix `_v`). Spines are lightweight and do not need to be persisted.

### 5.2 Series

Series views (suffix `_series_v`) add cumulative or sequential computation on top of spines — typically `ROW_NUMBER()`, running totals, or sequential game numbering used for burn-in thresholds and window definitions.

Materialisation: **views** (suffix `_v`).

### 5.3 Summaries

Summary tables (suffix `_summary`) are the final analytical products: aggregated metrics, distribution statistics, validity flags, phase-based breakdowns. These are the objects that `gold_presentation` joins against.

Materialisation: **tables** (persisted, with `load_timestamp`).

### 5.4 The `_v` suffix convention

Objects with suffix `_v` are temporary or intermediate views within a notebook's build flow. They are not intended as stable contracts for cross-notebook consumption. Stable outputs drop the `_v` suffix.

---

## 6. Dimension Specifications

### 6.1 Nation dimension (`naf_catalog.gold_dim.nation_dim`)

Grain: one row per `nation_id`.

| Column | Type | Notes |
|---|---|---|
| `nation_id` | INT | PK (stable). `nation_id = 0` reserved for Unknown. Generated via `PMOD(XXHASH64(group_key), 2147483647)`. |
| `nation_name` | STRING | Canonical name (cleaned) |
| `nation_name_display` | STRING | UI label |
| `iso2_code` | STRING | UPPERCASE when present |
| `iso3_code` | STRING | UPPERCASE when present |
| `fifa_code` | STRING | UPPERCASE when present |
| `ioc_code` | STRING | UPPERCASE when present |
| `flag_code` | STRING | UI flag key (typically ISO2-aligned); NULL = unknown/unsupported |

Special row: `nation_id = 0` — Unknown nation. All code fields NULL, `flag_code` NULL. `nation_dim` is the single owner of nation naming, codes, and display labels. Other dims/facts store only `nation_id` and join for display. Code fields should be trimmed and uppercased; unknown codes are NULL (no placeholder strings like `'XX'`). Built from Silver's canonical nation entity table.

### 6.2 Race dimension (`naf_catalog.gold_dim.race_dim`)

Grain: one row per `race_id`.

| Column | Type | Notes |
|---|---|---|
| `race_id` | INT | PK (stable). `race_id = 0` reserved for GLOBAL scope. |
| `race_name` | STRING | Canonical race name |

Special row: `race_id = 0`, `race_name = 'none'` — represents race-agnostic (GLOBAL) rating scope. Never used as a playable race.

Relationship usage: Games use `home_race_id`, `away_race_id` (non-zero, playable). Coach-perspective facts use `race_id` (coach's race) and optionally `opponent_race_id`. Rating facts use `race_id = 0` for GLOBAL scope, `race_id <> 0` for RACE scope.

### 6.3 Coach dimension (`naf_catalog.gold_dim.coach_dim`)

Grain: one row per `coach_id`.

| Column | Type | Notes |
|---|---|---|
| `coach_id` | INT | PK (NAF number / stable external ID) |
| `coach_name` | STRING | Canonical coach name |
| `nation_id` | INT | FK → `nation_dim`. NOT NULL; `0` = Unknown |

Intentionally minimal. SCD Type 1 (current-state). Nation names/codes/flags not duplicated here.

### 6.4 Tournament dimension (`naf_catalog.gold_dim.tournament_dim`)

Grain: one row per `tournament_id`.

| Column | Type | Notes |
|---|---|---|
| `tournament_id` | INT | PK |
| `tournament_name` | STRING | Canonical name |
| `organizer_coach_id` | INT | FK → `coach_dim` (nullable if unknown) |
| `date_start` | DATE | Tournament start date |
| `date_end` | DATE | Tournament end date |
| `start_date_id` | INT | FK → `date_dim` (YYYYMMDD of `date_start`) |
| `end_date_id` | INT | FK → `date_dim` (YYYYMMDD of `date_end`) |
| `is_major_tournament` | BOOLEAN | NOT NULL. Canonical major flag. |
| `nation_id` | INT | FK → `nation_dim`. NOT NULL; `0` = Unknown |

Contract: `tournament_dim` must cover all `tournament_id` referenced by `silver.games_clean`. `is_major_tournament` is NOT NULL by design; downstream helpers may rely on this without COALESCE. If only a single date is known, set `date_end = date_start` and `end_date_id = start_date_id`.

**Note on date columns**: The spec includes both DATE columns (`date_start`, `date_end`) and their corresponding `date_id` INT keys (`start_date_id`, `end_date_id`). The `date_id` columns enable efficient joins to `date_dim` and are the preferred join keys in downstream layers. The DATE columns are retained for human readability and date arithmetic.

### 6.5 Date dimension (`naf_catalog.gold_dim.date_dim`)

Grain: one row per calendar date.

| Column | Type | Notes |
|---|---|---|
| `date_id` | INT | PK. `YYYYMMDD` surrogate key. |
| `date` | DATE | The date value (unique) |
| `year` | INT | Calendar year |
| `quarter` | INT | 1–4 |
| `month` | INT | 1–12 |
| `day_of_month` | INT | 1–31 |
| `month_name` | STRING | Short name (e.g. Jan) |
| `year_month` | STRING | `YYYY-MM` |
| `day_of_week_name` | STRING | Short name (e.g. Mon) |
| `week_of_year` | INT | Per Spark `weekofyear` |
| `day_of_week_num` | INT | ISO-8601: Mon=1 … Sun=7 |
| `is_weekend` | BOOLEAN | TRUE when `day_of_week_num` IN (6, 7) |

`date_id` is introduced in Silver when canonicalising source dates/timestamps. Any Silver table emitting `date_id` must enforce NOT NULL. Facts/summaries should join to `date_dim` via `date_id`. Intentionally simple and stable — add convenience flags only when there is a clear downstream use.

### 6.6 Identity shims (Gold Presentation pattern)

The presentation layer uses identity views as a DRY pattern for display attributes that are reused across multiple presentation outputs:

| Shim | Purpose | Key columns |
|---|---|---|
| `coach_identity_v` | Coach display name + nation display | `coach_id`, `coach_name`, `nation_name_display` |
| `nation_flag_emoji_v` | Flag emoji generation (ISO2 → Regional Indicator Symbols + GB subdivision special cases) | `nation_id`, `flag_code`, `iso2`, `flag_emoji` |
| `nation_identity_v` | Nation display + flag | `nation_id`, `nation_name_display`, `flag_emoji` |
| `tournament_identity_v` | Tournament display + dates | `tournament_id`, `tournament_name`, dates |
| `race_identity_v` | Race display name | `race_id`, `race_name` |

These are views (not tables) and are internal to the presentation layer. They are not stable contracts for external consumption.

**Flag emoji rendering caveat**: `flag_emoji` uses Unicode Regional Indicator Symbols (e.g. U+1F1E9 U+1F1F0 for 🇩🇰). These render as graphical flags on macOS and Linux, but Windows displays them as two-letter codes (e.g. "DK"). The underlying UTF-8 bytes are correct on all platforms — this is a client-side rendering limitation. For future HTML/Streamlit dashboards targeting Windows users, use CSS flag libraries (`flag-icons`) or image-based flags (`flagcdn.com`) instead of Unicode emoji.

---

## 7. Materialisation Policy

| Object type | Storage | Naming signal |
|---|---|---|
| Spines | View | `*_spine_v` |
| Series | View | `*_series_v` |
| Intermediate views | View | `*_v` |
| Summary analytics | Delta table | `*_summary` (with `load_timestamp`) |
| Presentation contracts | View or table | No `_v` suffix |
| Dimensions | Delta table | `*_dim` |
| Facts | Delta table | `*_fact` |

Summary tables include `load_timestamp` (TIMESTAMP) recording when the table was written. Views do not need `load_timestamp`.

---

## 8. Presentation Layer Contracts

### 8.1 Contract stability

Presentation outputs are stable data contracts. Changes to grain, column names, or metric meanings require intentional versioning or explicit changelog notes.

### 8.2 Dashboard parameter portability

Databricks dashboards use `:parameter` syntax for interactive controls. This syntax is not portable to Streamlit or static HTML. When the project moves to alternative publishing, the presentation layer contracts remain stable — only the parameter binding mechanism changes.

### 8.3 Presentation layer rules

Presentation outputs join dims for display attributes and summaries for metrics. They may add display labels, sort keys, and UI-friendly bucketing. They must not recompute metrics, re-canonicalise data, or add heavy business logic. When a metric appears in a presentation output, it must be traceable to a specific `gold_summary` object.

---

## 9. Consistency and QA Checklist

These rules prevent silent drift across the model:

1. No dashboard dataset recomputes MEAN/MEDIAN/PEAK — it only selects from coach-context summaries.
2. POST_THRESHOLD always uses strict "after burn-in" semantics (games strictly > threshold, not >=).
3. LAST_N respects "min(window, games)" semantics; optional UI gating for "full window only".
4. Nation attributes are never duplicated outside `nation_dim`; always joined via `nation_id`.
5. GLOBAL scope always uses `race_id = 0`.
6. Bins reuse stable scheme IDs + labels; only the bucketed rating value changes with selector.
7. `load_timestamp` (not `load_datetime`) is the canonical audit column for materialised tables.
8. `variant_id IN (1, 13, 15)` is the canonical variant filter — applied in Silver, inherited everywhere.
9. `coach_id = 9` exclusion is applied in Silver — not re-filtered downstream.

---

## 10. Future Extension Points

These are not being built now, but the design should avoid decisions that make them hard later.

### 10.1 Multi-platform awareness

The current platform is NAF (tabletop Blood Bowl). Future platforms may include Blood Bowl 3 (BB3), FUMBBL, and potentially others. Points of attention:

**Coach identity across platforms**: A single person may have different coach IDs on different platforms. The current `coach_id` is a NAF number. If multi-platform support is added, a cross-platform identity layer would sit above the current `coach_dim`, mapping platform-specific IDs to a universal player identity. Design choice: keep `coach_id` as a platform-scoped key, and introduce a separate `player_id` or `universal_coach_id` only when needed.

**Game ID namespacing**: `game_id` is currently implicitly NAF-scoped. If multiple platforms contribute games, either prefix IDs by platform or add a `platform_id` discriminator column to facts. The current single-source design does not conflict with either approach.

**Variant and ruleset abstraction**: `variant_id` is NAF-specific. Other platforms may have their own ruleset identifiers. A future `ruleset_dim` could normalise across platforms. For now, `variant_id` in Silver is sufficient.

**Race mapping**: Race IDs and names differ across platforms (NAF races vs BB3 races vs FUMBBL races). A future `race_mapping` table could map platform-specific race IDs to a canonical set. The current `race_dim` is already platform-neutral in structure.

### 10.2 Incremental ingestion

The current model uses snapshot-based full reload. If data volumes grow, Bronze can add `ingest_batch_id` for append/incremental ingestion. The Silver dedup contract already handles this — uniqueness is enforced at Silver grain regardless of ingestion pattern.

### 10.3 Additional analytical domains

The model is designed for domain-specific summary and presentation pipelines. Currently: coach (mature), nation (in progress), tournament (placeholder), race (placeholder). Each new domain follows the same pattern: summary notebook → presentation notebook → dashboard contract.

---

## 11. Superseded Documents

This specification consolidates and replaces the following individual documents. They are retained in the repository for historical reference but should not be treated as authoritative:

- `00_principles (1).md` — Core principles (now Section 1)
- `00_design_decisions (1).md` — Design decisions (now distributed across Sections 1–4)
- `01_variable_naming (1).md` — Naming conventions (now Section 2)
- `02_schema_design (1).md` — Schema design (now Sections 1, 3)
- `10_nation_dimension (1).md` — Nation dim spec (now Section 6.1)
- `11_race_dimension (1).md` — Race dim spec (now Section 6.2)
- `12_coach_dimension (1).md` — Coach dim spec (now Section 6.3)
- `13_tournament_dimension (1).md` — Tournament dim spec (now Section 6.4)
- `14_date_dimension (1).md` — Date dim spec (now Section 6.5)

## 12. Resolved Contradictions

These issues existed across the previous document set and code. They are resolved as follows:

1. **`load_datetime` vs `load_timestamp`**: Previously documented as a contradiction — the nation summary notebook (332) historically used `load_datetime`. Code has since been corrected to `load_timestamp`. Resolved.

2. **Variant filter scope**: Bronze/Silver code correctly filters `variant_id IN (1, 13, 15)`. The test suite (`Tests and admining.py`) still checks only `(1, 13)`. The canonical filter is `(1, 13, 15)` — **tests need updating**.

3. **Tournament dim date columns**: The original dimension spec listed only `date_start`/`date_end` (DATE), but code also produces `start_date_id`/`end_date_id` (INT). Both are now documented in Section 6.4. The `*_date_id` columns are the preferred join keys. Resolved.
