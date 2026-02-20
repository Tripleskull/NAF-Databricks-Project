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
- **Do NOT use**: `NAF Project with Claude` (old folder, to be cleaned up)

### Git Workflow
1. **Work**: Claude edits files in the mounted folder
2. **Save**: Changes automatically save locally
3. **Commit** (user runs on Windows):
   ```bash
   cd "C:\Users\tripl\NAF-Databricks-Project"
   git status
   git add .
   git commit -m "Description of changes"
   git push
   ```
4. **Sync Databricks**: Pull changes in Databricks Repos to update notebooks

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
- **Scandinavian directness** over American politeness
- No excessive apologizing or hedging
- Get to the point efficiently

### Domain Knowledge
- **Ask for Blood Bowl/NAF context** when needed rather than assuming domain knowledge
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

### Line Endings
- No `.gitattributes` file exists yet — Windows CRLF vs Linux LF causes all files to appear modified
- Fix: add `.gitattributes` with `* text=auto` and normalize (run `git add --renormalize .`)
- Until fixed, `git diff` will show phantom changes on every file

### Testing
- Run pipeline in order: 100 → 200 → 310 → 320 → 331 → 332 → 333 → 334 → 340 → 341 → 342 → 343 → 344 → 350 (tests)
- Verify dashboards after pipeline changes
- Check parameter names match between queries and dashboard config
