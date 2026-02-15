# 03 – Style Guides (SQL, PySpark, and Layer Rules)

This document provides practical style guidance for writing readable, maintainable Databricks SQL and PySpark notebooks, aligned with the medallion layers.

Specific naming conventions live in `01_variable_naming.md`. Concrete design decisions (e.g. home/away key naming) live in `00_design_decisions.md`.

---

## 1) Databricks SQL style

### 1.1 Formatting
- SQL keywords: **UPPERCASE**
- Table/column names: **lowercase snake_case**
- Prefer explicit aliases and aligned `JOIN` clauses
- Prefer `SELECT` with explicit columns (avoid `SELECT *` for Gold outputs)
- Keep CTEs small and readable; avoid deeply nested subqueries

### 1.2 Fully-qualified names
Use fully-qualified names where it improves clarity (especially in Gold):
- `naf_catalog.gold_fact.games_fact`
- `naf_catalog.gold_dim.coach_dim`

### 1.3 Join readability
- Put join conditions on new lines
- Name aliases consistently (`g` for games, `ch/ca` for home/away coach, etc.)
- Keep join keys visually obvious

### 1.4 Example (preferred home/away naming)
```sql
SELECT
  g.game_id,
  g.game_date,
  g.home_coach_id,
  g.away_coach_id,
  ch.coach_name AS home_coach_name,
  ca.coach_name AS away_coach_name
FROM naf_catalog.gold_fact.games_fact AS g
LEFT JOIN naf_catalog.gold_dim.coach_dim AS ch
  ON g.home_coach_id = ch.coach_id
LEFT JOIN naf_catalog.gold_dim.coach_dim AS ca
  ON g.away_coach_id = ca.coach_id;


## 2) Key integrity checks (recommended)

- Enforce required `*_id` NOT NULL and PK uniqueness in **Silver**.
- Avoid repeating the same `IS NOT NULL` filters and `ROW_NUMBER()` dedup in Gold unless a real duplication risk is demonstrated.
- Prefer explicit QA queries (rowcount, null counts, duplicate counts) as separate notebook cells when hardening contracts.
