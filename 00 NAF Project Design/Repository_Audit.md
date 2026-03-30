# Repository Audit — Phase 1

---

## A. Executive Overview

The repository is a well-structured Databricks analytics project with a clean medallion architecture, consistent naming, centralised configuration and thorough design documentation. The core pipeline (bronze through presentation) is production-grade. The rating model track (321–324) is newer and still evolving.

**Current state:** Internal/research-grade — roughly 60–70% of the way to publication-ready. The architecture, data model and analytical logic are strong. The gaps are in presentation: most notebook cells lack titles and header comments, several design documents are stale or overlapping, and the README gives a general overview but lacks concrete examples or results. A focused clean-up pass (primarily documentation and notebook commentary) would close most of the gap.

---

## B. Main Issues

### Documentation issues

1. **Pipeline_Object_Index.md missing 323/324.** Race rating engine outputs (`race_rating_history_fact`) and evaluation objects are undocumented. Build order also omits 323–324.
2. **style_guides.md references retired files.** Line 6 references `01_variable_naming.md` and `00_design_decisions.md` — both superseded by `NAF_Design_Specification.md`.
3. **Nation_plan.md still active.** Marked for retirement in CLAUDE.md but still present and referenced. Should either be explicitly deprecated with a header note or retired.
4. **README.md is generic.** Suitable for public viewing but reads as a template — no concrete examples, results, screenshots or model descriptions. The "Getting Started" section is at the top instead of after the project description.
5. **PROJECT_STATUS.md is gitignored.** Referenced in CLAUDE.md as a project file but lives in the ignored working-documents folder. Either bring it into the repo or remove the CLAUDE.md reference.
6. **Folder name typo:** `02 NAF Dashbords` → should be `Dashboards`.

### Notebook / commentary issues

7. **~85% of cells lack DBTITLE markers.** Only 322–324 and parts of 350 have titles. The summary/presentation notebooks (331, 332, 341, 342) are worst — zero titles across 100+ cells.
8. **SQL-wrapped cells have no header comments.** Cells in 200, 310, 330–334, 340–344 contain only `# MAGIC %sql` blocks with SQL header comments inside the SQL but no Python-level cell header explaining the cell's role.
9. **323 has 9 empty cells.** Placeholder cells from Databricks that clutter the notebook.
10. **Cell 1 (notebook header) quality is inconsistent.** 321–324 have clear, structured headers. Older notebooks (100, 200, 310, 320) have adequate but less structured headers. Summary/presentation notebooks mostly have good SQL block headers but no markdown cell.

### Code quality / bug-risk issues

11. **No critical code bugs identified.** Parameters are centralised, column naming is consistent, validation exists at entry points.
12. **`datetime` aliasing varies** — some notebooks use `import datetime`, others `import datetime as dt`. Cosmetic but inconsistent.
13. **Large commented-out code blocks** in 322 (tuner execution sections). Intentional (expensive to run), but should be distinguished from dead code via a brief comment.

### Structure / organisation issues

14. **Folder spaces in names** (`01 NAF Project Files`, `02 NAF Dashbords`). Not a bug, but unusual for a public repo. Renaming would be a structural change (C5).
15. **`coachGroups.txt` in root** (or working-documents). Contains coach ID lists for plot exports — should either live in a data/ folder or stay gitignored.
16. **334 and 344 are empty stubs.** Placeholder notebooks for race summaries/presentation. Fine for now, but should be noted.

### Publication / portfolio-readiness issues

17. **No results or examples in the repo.** No sample outputs, screenshots, evaluation summaries or model comparison tables visible from the README.
18. **No modelling overview document.** The rating model track (Elo → SSM v1 → SSM v2 → race-aware) is documented across multiple design files but there's no single narrative overview a reviewer could read.
19. **CLAUDE.md is internal.** Contains working instructions for Claude — fine for the repo, but a public viewer would find it confusing without context.

---

## C. Immediate Priorities

Ordered by impact and risk:

1. **Add DBTITLE markers to all cells** — highest visual impact for portfolio reviewers. Start with the large notebooks (331, 332, 341, 342) where the gap is largest. (Opus — requires editing notebook code. C1/C6)
2. **Update Pipeline_Object_Index.md** with 323/324 sections and correct build order. (Sonnet — documentation only.)
3. **Clean up 323 empty cells.** (Opus — requires cell deletion. C1/C6)
4. **Improve README.md.** Add: concrete project description with key metrics, architecture diagram reference, model descriptions, example output or screenshot, link to design docs. (Sonnet — documentation.)
5. **Fix style_guides.md stale references.** (Sonnet — documentation.)
6. **Add deprecation note to Nation_plan.md.** (Sonnet — documentation.)
7. **Resolve PROJECT_STATUS.md location.** (User decision — see Section G.)

---

## D. Documentation Map

| Purpose | Authoritative source | Notes |
|---|---|---|
| Project overview (public) | `README.md` | Needs enrichment (C.4) |
| Working instructions | `CLAUDE.md` | Internal; consider adding a note for public viewers |
| Architecture & data model | `NAF_Design_Specification.md` | Comprehensive; current |
| All tuneable parameters | `Analytical_Parameters.md` | Current; maps to `analytical_config` |
| Pipeline structure & dependencies | `Pipeline_Object_Index.md` | Missing 323/324 (C.2) |
| Development roadmap | `Expansion_Plan.md` | Phases 0–4 done; 5–9 pending |
| SSM model design | `Skill_Estimation_Plan.md` + `ssm_model_outline_v2_with_suggestions.md` | Two files — could consolidate, but not urgent |
| Race model design | `race_rating_model_plan.md` | Current |
| SQL/code style | `style_guides.md` | Minimal; stale references (C.5) |
| Notebook guide | *Does not exist* | Pipeline_Object_Index partially serves this role |
| Results / examples | *Does not exist* | Needed for portfolio (C.17) |
| Update flow | `CLAUDE.md` §Change Policy in `Analytical_Parameters.md` | Adequate for internal use |

---

## E. Notebook Review Principles

Proposed standard for all notebook cells:

### Cell 1: Notebook header (markdown)
- Notebook number and name
- Layer and pipeline position
- Purpose (2–3 sentences)
- Inputs, outputs, dependencies
- Design reference (link to relevant design doc)

### All subsequent cells: title + header

**DBTITLE** (required): Short descriptive label, 3–8 words. Examples:
- `Load Analytical Config`
- `TABLE: coach_rating_global_elo_summary`
- `Calibration Plots`
- `Quick Diagnostics`

**Header comment** (required for code cells): 2–5 line comment block at the top:
```python
# PURPOSE: Compute Brier score for all models on the test window.
# INPUT:   eval_df with model predictions merged.
# OUTPUT:  metrics dict, printed comparison table.
```

For SQL cells (`# MAGIC %sql`), the existing SQL block header pattern (`-- OBJECT / GRAIN / PURPOSE / SOURCES`) is already good. No change needed — just ensure every SQL cell has one.

### Commentary within cells
- Inline comments for non-obvious logic; no comment for self-evident lines.
- Section separators (`# ---`) for cells longer than ~50 lines.
- No commented-out code without an explicit note explaining why it's kept.

---

## F. Changes to Defer to Opus

| Item | Reason | Constraint |
|---|---|---|
| Add DBTITLE markers to all ~200 untitled cells | Requires editing notebook source code | C1, C6 |
| Delete 9 empty cells in 323 | Requires editing notebook source code | C1, C3, C6 |
| Add header comments to cells missing them | Requires editing notebook source code | C1, C6 |
| Clean up commented-out code blocks (distinguish intentional from dead) | Requires code judgment | C1, C6, C11 |
| Fix `datetime` import aliasing inconsistency | Code change | C1, C6 |
| Rename `02 NAF Dashbords` → `02 NAF Dashboards` | Structural repo change, affects Databricks Repos link | C5, C6 |
| Rename folders to remove spaces (if desired) | Structural repo change | C5, C6 |
| Standardise Cell 1 markdown headers across older notebooks | Requires rewriting notebook cells | C2, C6 |

---

## G. Questions / Ambiguities

1. **PROJECT_STATUS.md location.** CLAUDE.md references it as a project file, but it's gitignored. Should it be: (a) moved to root and tracked, (b) kept gitignored and the CLAUDE.md reference removed, or (c) replaced by a Status section in README.md?

2. **Folder name spelling: `02 NAF Dashbords`.** Clearly a typo for "Dashboards". Renaming affects Databricks Repos sync and any hardcoded references. Should this be fixed now, or deferred?

3. **Folder spaces in names.** `01 NAF Project Files` and similar use spaces. This is unconventional for public repos (causes quoting issues in shell). Is this an established convention you want to keep, or an accident from initial setup? (C10)

4. **Nation_plan.md vs Nation_Dashboard_Plan.md.** These overlap in scope (nation pipeline content). Nation_plan.md is marked for retirement. Should it be: (a) retired now with a deprecation header, (b) merged into Nation_Dashboard_Plan.md, or (c) kept as-is until nation pipeline is finalised?

5. **ssm_notebook_review_clean_sharp.md.** This is an external review document. Should it remain in `00 NAF Project Design/` (design docs) or move to the working-documents folder? It's useful reference but not a design spec.

6. **CLAUDE.md visibility.** This file is valuable for Claude-assisted development but may confuse public viewers. Options: (a) keep as-is with a note at the top, (b) rename to something like `.claude-instructions.md`, (c) move to a dotfile location. (C9 — the repo's CLAUDE.md convention is established but may conflict with public presentation goals.)

7. **Empty stub notebooks (334, 344).** These are placeholders for race summary/presentation. Should they be: (a) kept as empty stubs, (b) populated with a markdown header explaining planned scope, or (c) removed until needed?

8. **322 commented-out tuner execution.** Section 5 of the Brier-UA tuner cell is commented out because it's expensive to run (~3–6 hours). This is intentional but looks like dead code. Should it get an explicit `# COMMENTED OUT: expensive grid search, ~3-6h on Community Edition. Uncomment to re-tune.` note, or be restructured differently?

9. **321–324 model maturity framing.** The prompt notes these notebooks are "part of an evolving modelling track, not a finished endpoint." Current documentation doesn't explicitly flag this. Should there be a note in the README, the Pipeline_Object_Index, or the notebook headers themselves about which models are production vs research?
