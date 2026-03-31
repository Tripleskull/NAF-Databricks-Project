> This project uses AI-assisted development with Claude. This file documents project conventions and architecture to maintain consistency across sessions.

# NAF Databricks Project — Developer Reference

## Project Context
This Databricks project analyzes NAF (Blood Bowl) data to provide insights through dashboards and reports. Analysis methods include statistical modeling, ML, and traditional analytics.

### Target Audiences & Use Cases
- **Players**: Ratings, results, narratives
- **Coaches** (especially national teams): Performance analysis, ratings, predictions
- **Tournaments**: Event metrics and characteristics
- **Races/Factions**: Matchup analysis, power rankings, ruleset performance

## Repository Setup
- **GitHub Repository**: https://github.com/Tripleskull/NAF-Databricks-Project
- **Source of Truth**: GitHub is the single source of truth for all code
- **Databricks Integration**: Connected via Databricks Repos (syncs with GitHub)
- **Runtime**: Databricks Community Edition (free tier)

## Architecture & Standards

### Data Architecture
- **Medallion architecture**: Bronze → Silver → Gold
- **Catalog structure**: `naf_catalog.{bronze, silver, gold_dim, gold_fact, gold_summary, gold_presentation}`
- **Singleton config**: `naf_catalog.gold_dim.analytical_config` for centralized parameters

### Code Quality
- Professional-grade code quality
- Google-style docstrings for all functions
- Comprehensive documentation and readability throughout

### Key Conventions
- **Timestamps**: Always `load_timestamp`, never `load_datetime`
- **Flags**: Use `flag_emoji` for coach/nation identity views, `flag_code` only in dimension tables
- **Naming**: Consistent column names across summary/presentation layers
- **Dashboard parameters**: Use `:parameter_name` syntax for Lovelytics dashboard queries

### Quality Standards
- Verify PK uniqueness and FK coverage in tests
- Ensure metric_type values are uppercase
- Check for TABLE/VIEW duplicates
- Validate dashboard queries against actual view schemas

## File Structure

### Notebooks (`01 NAF Project Files/`)
- `100_NAF_bronze.py` — Bronze layer (raw ingestion)
- `200_NAF_silver.py` — Silver layer (cleansed/conformed)
- `310_NAF_gold_dim.py` — Dimension tables
- `320_NAF_gold_fact.py` — Fact tables + Elo engine
- `321_NAF_gold_fact_ssm.py` — SSM rating engine (v1: random walk EKF, v2: time-aware + adaptive volatility)
- `322_NAF_gold_fact_ssm_eval.py` — SSM evaluation & tuning (research, not production)
- `323_NAF_gold_fact_race_rating.py` — Race-aware rating engine (global + race deviations, joint EKF)
- `324_NAF_gold_fact_race_rating_eval.py` — Race rating evaluation (research, not production)
- `330_NAF_gold_summary_core.py` — Core summary objects (nation_coach_glo_metrics, world_glo_metric_quantiles)
- `331_NAF_gold_summary_coach.py` — Coach summaries
- `332_NAF_gold_summary_nation.py` — Nation summaries
- `333_NAF_gold_summary_tournament.py` — Tournament summaries
- `334_NAF_gold_summary_race.py` — Race summaries
- `340_NAF_gold_presentation_core.py` — Core presentation views
- `341_NAF_gold_presentation_coach.py` — Coach presentation views
- `342_NAF_gold_presentation_nation.py` — Nation presentation views
- `343_NAF_gold_presentation_tournament.py` — Tournament presentation views
- `344_NAF_gold_presentation_race.py` — Race presentation views
- `350_Tests_and_admining.py` — Pipeline tests

### Dashboards (`02 NAF Dashboards/`)
- `Coach Dashboard.lvdash.json` — Coach dashboard (polished)
- `Nation Dashboard.lvdash.json` — Nation dashboard (early)

### Design Documentation (`00 NAF Project Design/`)
- `NAF_Design_Specification.md` — Consolidated architecture spec (single source of truth for modelling rules)
- `Analytical_Parameters.md` — All tuneable parameters + config column mapping + change protocol
- `Pipeline_Object_Index.md` — Logical ordering and dependency map for all summary/presentation objects
- `Expansion_Plan.md` — 9-phase roadmap (nation dashboard, team selector, global status report)
- `Nation_Dashboard_Plan.md` — Nation dashboard overhaul plan (widget-by-widget)
- `style_guides.md` — SQL/PySpark formatting rules
- `Project_Status.md` — Pipeline maturity, expansion progress, open issues
- `archive/` — Implemented design specs kept for reference (SSM_Design.md, Race_Rating_Design.md)

## Pipeline Execution Order
Run notebooks in sequence: 100 → 200 → 310 → 320 → 321 → 330 → 331 → 332 → 333 → 334 → 340 → 341 → 342 → 343 → 344 → 350 (tests). Verify dashboards after pipeline changes.

## Technical Notes

### Flag Emoji Rendering (Windows Limitation)
Windows does not render Unicode Regional Indicator Symbol flag emojis (e.g. 🇩🇰), displaying the two-letter code instead. The pipeline produces correct UTF-8 bytes — the issue is purely client-side rendering.

Flag architecture is centralised in `nation_flag_emoji_v` (340) using `decode(unhex(...), 'UTF-8')` with special cases for GB subdivisions. All views consume flags via `nation_identity_v` / `coach_identity_v` — never inline hex conversions.

For future HTML/Streamlit dashboards targeting Windows users, use CSS-based flags (`flag-icons`), image-based flags (`flagcdn.com`), or SVG sprites instead of Unicode emoji.

### Dashboard JSON Gotcha
Databricks AI/BI table widgets have two column arrays: `spec.encodings.columns` (visible) and `spec.invisibleColumns` (hidden). When importing or editing dashboards, columns can silently move to `invisibleColumns`. Always verify flag columns are in `encodings.columns`.
