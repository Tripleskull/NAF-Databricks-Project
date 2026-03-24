# NAF Databricks Project - Instructions for Claude

## Project Context
This Databricks project analyzes NAF (Blood Bowl) data to provide insights through dashboards and reports. Analysis methods include statistical modeling, ML, and traditional analytics.

### Target Audiences & Use Cases
- **Players**: Ratings, results, narratives
- **Coaches** (especially national teams): Performance analysis, ratings, predictions
- **Tournaments**: Event metrics and characteristics
- **Races/Factions**: Matchup analysis, power rankings, ruleset performance

## Version Control & Workflow

### Repository Setup
- **GitHub Repository**: https://github.com/Tripleskull/NAF-Databricks-Project
- **Source of Truth**: GitHub is the single source of truth for all code
- **Databricks Integration**: Connected via Databricks Repos (syncs with GitHub)
- **Local Development**: Cloned to Windows PC for local work with Cowork

### Working Folder
**IMPORTANT**: Always work in the GitHub-connected folder:
- **Path**: `C:\Users\tripl\NAF-Databricks-Project`

### Git Workflow
1. **Work**: Claude edits files in the mounted folder
2. **Commit**: Claude always commits locally after completing a batch of changes (never leave uncommitted work). Claude drafts the commit message and runs `git commit` in the VM.
3. **Push** (user runs on Windows): Claude cannot push to GitHub (no remote credentials). User pushes when ready:
   ```bash
   cd "C:\Users\tripl\NAF-Databricks-Project"
   git push
   ```
4. **Sync Databricks**: Pull changes in Databricks Repos to update notebooks

#### Commit Rules
- **Always commit** after completing a logical batch of changes — never end a task with uncommitted work
- **Commit message style**: Short summary line, then blank line, then bullet details of what changed per file/notebook
- **Co-author tag**: Always include `Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>`
- **Stage specific files** by name (not `git add -A` or `git add .`)
- **Never amend** previous commits — always create new commits
- **Never push** — user handles push

## Initial Setup Task
**Status: Complete (2025-02-15)**
- Git repo verified: connected to `origin` on `main` branch ✓
- Old folder compared: notebooks identical (trivial trailing-newline diffs only). Design docs, dashboards, status files copied to Git repo.
- Old folder (`NAF Project with Claude`) should be deleted from PC manually (Cowork lacks delete permissions on mounted folders).
- Line-ending issue identified: no `.gitattributes` — CRLF/LF mismatch causes phantom diffs (see Known Issues)

## Architecture & Standards

### Data Architecture
- **Medallion architecture**: Bronze → Silver → Gold
- **Catalog structure**: `naf_catalog.{bronze, silver, gold_dim, gold_fact, gold_summary, gold_presentation}`
- **Singleton config**: `naf_catalog.gold_dim.analytical_config` for centralized parameters

### Code Quality
- Professional-grade code quality
- Google-style docstrings for all functions
- Comprehensive documentation and readability throughout
- Running on Databricks Community Edition (free tier)

### Key Conventions
- **Timestamps**: Always `load_timestamp`, never `load_datetime`
- **Flags**: Use `flag_emoji` for coach/nation identity views, `flag_code` only in dimension tables
- **Naming**: Consistent column names across summary/presentation layers
- **Dashboard parameters**: Use `:parameter_name` syntax for Lovelytics dashboard queries

## Code Review Expectations

### Performance & Architecture
- Flag performance issues (inefficient Spark operations, unnecessary shuffles, etc.)
- Suggest architectural improvements and cleaner implementations
- Balance refactoring: aim for clean, professional code that fulfills its purpose, but don't over-engineer or prevent forward progress
- Challenge implementation choices and explain trade-offs

### Quality Standards
- Verify PK uniqueness and FK coverage in tests
- Ensure metric_type values are uppercase
- Check for TABLE/VIEW duplicates
- Validate dashboard queries against actual view schemas

## Working Style Preferences

### Communication Style
- **Direct, matter-of-fact feedback** without excessive positivity
- **Critical and constructive approach**
- directness over politeness
- No excessive apologizing or hedging
- Get to the point efficiently

### Domain Knowledge
- **Ask for Blood Bowl/NAF context** or other context when needed rather than assuming domain knowledge
- Clarify terminology when uncertain
- User has deep domain expertise - leverage it

### Problem Solving
- Present options with trade-offs clearly stated
- Recommend specific approaches with reasoning
- Flag issues immediately, don't wait until asked
- If multiple rounds of fixes are needed, be systematic

## File Structure

### Notebooks (in Git, under `NAF Project Files/`)
- `100_NAF_bronze.py` - Bronze layer (raw ingestion)
- `200_NAF_silver.py` - Silver layer (cleansed/conformed)
- `310_NAF_gold_dim.py` - Dimension tables
- `320_NAF_gold_fact.py` - Fact tables + ELO engine
- `321_NAF_gold_fact_ssm.py` - SSM rating engine (v1: random walk EKF, v2: time-aware + adaptive volatility)
- `330_NAF_gold_summary_core.py` - Core summary objects (nation_coach_glo_metrics, world_glo_metric_quantiles)
- `331_NAF_gold_summary_coach.py` - Coach summaries
- `332_NAF_gold_summary_nation.py` - Nation summaries
- `333_NAF_gold_summary_tournament.py` - Tournament summaries
- `334_NAF_gold_summary_race.py` - Race summaries
- `340_NAF_gold_presentation_core.py` - Core presentation views
- `341_NAF_gold_presentation_coach.py` - Coach presentation views
- `342_NAF_gold_presentation_nation.py` - Nation presentation views
- `343_NAF_gold_presentation_tournament.py` - Tournament presentation views
- `344_NAF_gold_presentation_race.py` - Race presentation views
- `350_Tests_and_admining.py` - Pipeline tests

### Dashboards (in Git, under `02 NAF Dashbords/`)
- `Coach Dashboard.lvdash.json` - Coach dashboard (polished)
- `Nation Dashboard.lvdash.json` - Nation dashboard (early)

### Design Documentation (in Git, under `00 NAF Project Design/`)
- `NAF_Design_Specification.md` - Consolidated architecture spec (single source of truth for modelling rules)
- `Analytical_Parameters.md` - All tuneable parameters + config column mapping + change protocol
- `Pipeline_Object_Index.md` - Logical ordering and dependency map for all summary/presentation objects
- `Expansion_Plan.md` - 9-phase roadmap (nation dashboard, team selector, global status report)
- `Skill_Estimation_Plan.md` - SSM design options analysis (Option 4 selected and implemented)
- `ssm_model_outline_v2_with_suggestions.md` - SSM v2 design spec (time-aware + adaptive volatility)
- `Nation_plan.md` - Nation dashboard content plan (retire after nation pipeline is finalised)
- `style_guides.md` - SQL/PySpark formatting rules

### Other Documentation
- `CLAUDE.md` - This file (project instructions)
- `PROJECT_STATUS.md` - Current status, open issues, next steps
- `NAF Project Code Review.md` - Initial code review (reference until all issues resolved)

## Known Issues & Patterns

### Common Fixes
- Column name mismatches between summary and presentation layers
- Dashboard queries referencing non-existent views
- `load_datetime` vs `load_timestamp` inconsistencies
- `flag_code` vs `flag_emoji` mismatches

### Flag Emoji Rendering (Windows Limitation)
- **Root cause**: Windows does not render Unicode Regional Indicator Symbol flag emojis (e.g. 🇩🇰). Instead it displays the two-letter code (e.g. "DK") in a styled font. macOS and Linux render flags correctly.
- **Data is correct**: The pipeline produces proper UTF-8 bytes (`F09F87A9F09F87B0` for Denmark). The issue is purely client-side rendering.
- **Databricks dashboards**: Flags render on macOS/Linux browsers but NOT on Windows (any browser). Exports and screenshots should be done from macOS/Linux.
- **Flag architecture**: Centralised in `nation_flag_emoji_v` (340) using `decode(unhex(...), 'UTF-8')` with special cases for GB subdivisions. All views consume flags via `nation_identity_v` / `coach_identity_v` — never inline hex conversions.
- **Future HTML/Streamlit dashboards**: Do NOT rely on Unicode flag emojis if Windows users are a target audience. Use one of:
  - CSS-based flags (e.g. `flag-icons` library: `<span class="fi fi-dk"></span>`)
  - Image-based flags (e.g. `flagcdn.com`: `<img src="https://flagcdn.com/16x12/dk.png">`)
  - SVG flag sprites
  - These approaches render identically on all platforms.
- **Dashboard JSON gotcha**: Databricks AI/BI table widgets have two column arrays: `spec.encodings.columns` (visible) and `spec.invisibleColumns` (hidden). When importing/editing dashboards, columns can silently move to `invisibleColumns`. Always verify flag columns are in `encodings.columns`.

### Line Endings
- No `.gitattributes` file exists yet — Windows CRLF vs Linux LF causes all files to appear modified
- Fix: add `.gitattributes` with `* text=auto` and normalize (run `git add --renormalize .`)
- Until fixed, `git diff` will show phantom changes on every file

### Testing
- Run pipeline in order: 100 → 200 → 310 → 320 → 321 → 330 → 331 → 332 → 333 → 334 → 340 → 341 → 342 → 343 → 344 → 350 (tests)
- Verify dashboards after pipeline changes
- Check parameter names match between queries and dashboard config
