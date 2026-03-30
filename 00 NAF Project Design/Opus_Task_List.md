# Opus Task List

> Structured task list for Opus sessions. All items are marked **[REVIEW]** — confirm with user before executing.
> Reference: `Cleanup_Tracker.md` for completed work, `Repository_Audit.md` for full context.
> **Last updated:** 2026-03-30

---

## 1. Deletions (requires user confirmation — Cowork cannot delete mounted files)

| # | Item | Location | Rationale | Priority |
|---|---|---|---|---|
| DEL-1 | `ssm_notebook_review_clean_sharp.md` | `00 NAF Project Design/` | External code review from a previous session. Not a design spec. Superseded by `Repository_Audit.md`. | **[REVIEW]** Med |
| DEL-2 | `Nation_plan.md` | `00 NAF Project Design/` | Deprecated plan, superseded by `Nation_Dashboard_Plan.md`. Has deprecation header but adds clutter. | **[REVIEW]** Low |
| DEL-3 | `NAF Project Code Review.md` | `Working documents gitignore/` | Old external review, gitignored. | **[REVIEW]** Low |
| DEL-4 | `NAF_Project_Full_Review_2026-02-16-CHATGPT.md` | `Working documents gitignore/` | Old ChatGPT review, gitignored. | **[REVIEW]** Low |
| DEL-5 | `review_notes/` folder (6 files) | `Working documents gitignore/review_notes/` | Per-notebook review notes from an earlier pass. Content is now absorbed into `Repository_Audit.md`. | **[REVIEW]** Low |
| DEL-6 | `Skill_Estimation_Plan.md` | `00 NAF Project Design/` | Point-in-time design options analysis (Option 4 was chosen and implemented). Useful reference but not an active spec. Consider archiving rather than deleting. | **[REVIEW]** Low |
| DEL-7 | `ssm_model_outline_v2_with_suggestions.md` | `00 NAF Project Design/` | SSM v2 design outline, now implemented. Same reasoning as DEL-6. | **[REVIEW]** Low |
| DEL-8 | `race_rating_model_plan.md` | `00 NAF Project Design/` | Race rating design plan, now implemented in 323/324. Same reasoning as DEL-6. | **[REVIEW]** Low |

> Note on DEL-6/7/8: These are historical design specs for implemented models. They may be worth keeping as reference (e.g. for explaining design choices to collaborators). Consider an `archive/` subfolder rather than deletion.

---

## 2. Code Cleanup — Dead Code

| # | Item | Files | Rationale | Priority |
|---|---|---|---|---|
| CC-1 | Remove two commented-out SQL blocks in 331 | `331_NAF_gold_summary_coach.py` cells "Moved to 332: coach_race_relative_strength" and "Moved to 332: coach_race_nation_rank" | Both objects now live in 332. The entire SQL definitions are commented out in 331 — genuinely dead code. Safe to delete. | **[REVIEW]** High |
| CC-2 | Review commented-out blocks in 332 | `332_NAF_gold_summary_nation.py` | Cells labelled "REMOVED: nation_overview_comparison_summary" and "REMOVED: nation_game_quality_bin_wdl" are stubs with only a comment explaining removal. These are clean — just confirm and close. | **[REVIEW]** Low |

---

## 3. Safety / Risk

| # | Item | Files | Rationale | Priority |
|---|---|---|---|---|
| SAF-1 | Add guard comment to DROP SCHEMA cell in 320 | `320_NAF_gold_fact.py` cell "Drop schema gold_fact" | Cell runs `DROP SCHEMA IF EXISTS naf_catalog.gold_fact CASCADE` — destructive if run accidentally. Needs a prominent `# ADMIN ONLY — drops entire gold_fact schema` comment. | **[REVIEW]** High |
| SAF-2 | Add guard comment to DROP SCHEMA cell in 331 | `331_NAF_gold_summary_coach.py` cell "Drop schema gold_summary" | Same issue — `DROP SCHEMA IF EXISTS naf_catalog.gold_summary CASCADE`. | **[REVIEW]** High |

---

## 4. Structure

| # | Item | Files | Rationale | Priority |
|---|---|---|---|---|
| STR-1 | Rename `02 NAF Dashbords` → `02 NAF Dashboards` | Folder + Databricks Repos | Clear typo. Requires renaming in git and re-pointing Databricks Repos. | **[REVIEW]** Med |
| STR-2 | Move `coachGroups.txt` to a data folder or confirm gitignore | `Working documents gitignore/coachGroups.txt` | Small helper file with coach ID lists. Currently gitignored. Either track it explicitly or document where it lives. | **[REVIEW]** Low |
| STR-3 | Create `archive/` subfolder in `00 NAF Project Design/` | `00 NAF Project Design/` | Home for implemented design specs (DEL-6/7/8) if not deleting them. Keeps the design folder clean without losing history. | **[REVIEW]** Low |

---

## 5. Analytical / Model

| # | Item | Files | Rationale | Priority |
|---|---|---|---|---|
| MOD-1 | Tune `rr_q_global` in race rating model | `310_NAF_gold_dim.py` (analytical_config), `323`, `324` | Evaluation (324) showed overconfidence at high Brier scores. Current `rr_q_global=0.50` is likely too high — causes variance to grow quickly, keeping Kalman gains large and pushing ratings to extremes. Run 324 grid search to find a better value, then update `analytical_config` in 310. | **[REVIEW]** Med |

---

## 6. Documentation

| # | Item | Files | Rationale | Priority |
|---|---|---|---|---|
| DOC-1 | Fix 331 header — stale design decisions section | `331_NAF_gold_summary_coach.py` (markdown header) | Header mentions `coach_race_relative_strength` and `coach_race_nation_rank` as "331-specific design decisions" but both objects now live in 332. Update or remove those bullet points. | **[REVIEW]** Med |
| DOC-2 | Add example results to README (screenshots / metric table) | `README.md` | The biggest remaining gap for portfolio readiness. Requires a pipeline run and screenshot. Agreed as P1 in Cleanup_Tracker. | **[REVIEW]** High |
| DOC-3 | Resolve PROJECT_STATUS.md: track it, retire it, or add Status section to README | `CLAUDE.md`, `Working documents gitignore/PROJECT_STATUS.md` | File exists but is gitignored. CLAUDE.md still references it. User decision required first (D5 in Cleanup_Tracker). | **[REVIEW]** Med |

---

## Notes for Opus

- **Deletions (Section 1):** Cowork cannot delete files in mounted folders. User must confirm and perform file deletions on Windows, or Opus should use bash `rm` if the file system permits.
- **STR-1 (folder rename):** Needs both a `git mv` and a Databricks Repos re-sync. Confirm Databricks side before executing.
- **MOD-1 (parameter tuning):** Not a quick edit — requires running 324 with a parameter grid and interpreting results. Treat as a mini-research task.
- **CC-1 (dead code in 331):** High confidence these are safe to delete — both objects are confirmed in 332. But verify with a quick grep before removing.
