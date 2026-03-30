# Clean-Up Tracker

> Central coordination tool for repository clean-up. Update status as tasks are completed.
> Reference: `Repository_Audit.md` for full context on each item.

---

## Documentation

| # | Task | Files | Pri | Status |
|---|---|---|---|---|
| D1 | Add 323/324 sections to Pipeline_Object_Index; update build order | Pipeline_Object_Index.md | High | ✅ Done |
| D2 | Fix stale references in style_guides.md (retired file names) | style_guides.md | Med | ✅ Done |
| D3 | Add deprecation header to Nation_plan.md | Nation_plan.md | Med | ✅ Done |
| D4 | Enrich README: model descriptions, architecture summary, rating narrative | README.md | High | ✅ Done |
| D5 | Resolve PROJECT_STATUS.md location (User decision G.1) | CLAUDE.md, PROJECT_STATUS.md | Med | Todo (User) |
| D6 | Add modelling overview section (Elo → SSM → race-aware narrative) | README.md (merged into D4) | Med | ✅ Done |
| D7 | Add public-viewer note to CLAUDE.md header (User decision G.6) | CLAUDE.md | Low | ✅ Done |

## Notebooks

| # | Task | Files | Pri | Status |
|---|---|---|---|---|
| N1 | Add DBTITLE to all untitled cells (~176 cells across 17 notebooks) | 100–350 | High | ✅ Done |
| N2 | Add/improve cell header comments where missing or generic | 100–350 | Med | Deferred (low ROI after N1) |
| N3 | Delete 9 empty cells in 323 | 323 | High | ✅ Done |
| N4 | Standardise Cell 1 markdown headers in all notebooks | 100–350 | Med | ✅ Done |
| N5 | Replace commented-out tuner execution in 322 with boolean flag | 322 | Low | ✅ Done |
| N6 | Populate 334/344 stub notebooks with markdown header | 334, 344 | Low | ✅ Done |
| N7 | Flag model maturity (production vs research) in notebook headers | 321–324, 334, 344 | Med | ✅ Done |

## Code Quality / Risk

| # | Task | Files | Pri | Status |
|---|---|---|---|---|
| Q1 | Standardise `datetime` import aliasing (`as dt`) across notebooks | 321–324 | Low | ✅ Done |
| Q2 | Review large commented-out blocks: add intent notes or remove dead code | 322, 331, 332 | Low | Deferred |

## Structure / Organisation

| # | Task | Files | Pri | Status |
|---|---|---|---|---|
| S1 | Rename `02 NAF Dashbords` → `02 NAF Dashboards` | Folder + Databricks Repos | Med | Todo (User) |
| S2 | Decide on folder space convention (spaces in folder names) | All folder names | Low | Todo (User) |
| S3 | Delete external review files (ssm_notebook_review_clean_sharp.md etc.) | 00 NAF Project Design/ | Low | Todo (User) |

## Publication Readiness

| # | Task | Files | Pri | Status |
|---|---|---|---|---|
| P1 | Add example results or screenshots to README or a results doc | README.md | High | Todo (needs pipeline output) |
| P2 | Ensure all design docs have current "last updated" or version note | 00 NAF Project Design/*.md | Low | Todo |
| P3 | Final consistency pass: verify all cross-references between docs | All docs | Low | Todo |
