# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Presentation — Core (`naf_catalog.gold_presentation`)
# MAGIC
# MAGIC This notebook builds **shared, reusable presentation contracts** that other `gold_presentation_*` notebooks depend on.
# MAGIC
# MAGIC ## Layer
# MAGIC `naf_catalog.gold_presentation` (thin dashboard-oriented shaping)
# MAGIC
# MAGIC ## Build order
# MAGIC Run **after**:
# MAGIC - `naf_catalog.gold_dim` (nation/coach/tournament/race dims)
# MAGIC - `naf_catalog.gold_summary`
# MAGIC
# MAGIC Run **before**:
# MAGIC - domain presentation notebooks (e.g. coach/tournament/race dashboards)
# MAGIC
# MAGIC ## Objects created (core contracts)
# MAGIC - Schema: `naf_catalog.gold_presentation`
# MAGIC - `nation_flag_emoji_v` (emoji resolution)
# MAGIC - `nation_identity_v` (nation_name_display + flag_emoji)
# MAGIC - `coach_identity_v` (coach + nation identity)
# MAGIC - `tournament_identity_v` (tournament + host nation identity)
# MAGIC - `race_identity_v` (race display identity; includes GLOBAL handling if used)
# MAGIC - ~~`global_elo_bin_scheme`~~ (removed — legacy configurable bin framework)
# MAGIC
# MAGIC ## Design authority
# MAGIC Project Design:
# MAGIC - `00_design_decisions.md` (wins)
# MAGIC - `02_schema_design.md`
# MAGIC - `03_style_guides.md`
# MAGIC
# MAGIC ## Contract rules
# MAGIC - Prefer explicit column lists (avoid `SELECT *`) for published contracts.
# MAGIC - No KPI re-definition here: metrics stay in `gold_summary`; this notebook adds **display-only** shaping.
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create schema gold_presentation
# MAGIC %sql
# MAGIC -- SCHEMA: naf_catalog.gold_presentation
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Ensure gold_presentation schema exists for core presentation contracts.
# MAGIC -- LAYER        : GOLD_PRESENTATION
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS naf_catalog.gold_presentation;
# MAGIC

# COMMAND ----------

# DBTITLE 1,gold_presentation.nation_flag_emoji_v
# MAGIC %sql
# MAGIC -- VIEW: naf_catalog.gold_presentation.nation_flag_emoji_v
# MAGIC -- =====================================================================
# MAGIC -- LAYER        : GOLD_PRESENTATION
# MAGIC -- CONTRACT TYPE: Dashboard-facing display enrichment (nation -> flag emoji)
# MAGIC --
# MAGIC -- PURPOSE
# MAGIC --   Map each nation_id to a Unicode flag emoji for consistent dashboard display.
# MAGIC --   All emoji generation logic lives here (not in dashboards).
# MAGIC --
# MAGIC -- OUTPUT GRAIN / KEYS
# MAGIC --   GRAIN       : 1 row per nation_id
# MAGIC --   PRIMARY KEY : nation_id
# MAGIC --
# MAGIC -- OUTPUT COLUMNS
# MAGIC --   - nation_id
# MAGIC --   - flag_code
# MAGIC --   - iso2        (resolved ISO2 used for standard flags)
# MAGIC --   - flag_emoji
# MAGIC --
# MAGIC -- SOURCES
# MAGIC --   - naf_catalog.gold_dim.nation_dim
# MAGIC --
# MAGIC -- RULES / LOGIC
# MAGIC --   1) England/Scotland/Wales: use explicit Unicode tag-sequence flags.
# MAGIC --   2) Northern Ireland: maps to GB flag (existing dashboard behavior).
# MAGIC --   3) All other nations: build flag from ISO2 using Regional Indicator Symbols.
# MAGIC --      (Implemented via decode(unhex(...)) to match the prior “working” approach.)
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.nation_flag_emoji_v AS
# MAGIC WITH flags AS (
# MAGIC   SELECT
# MAGIC     -- England / Scotland / Wales “subdivision” flags (tag sequences)
# MAGIC     decode(unhex('F09F8FB4F3A081A7F3A081A2F3A081A5F3A081AEF3A081A7F3A081BF'), 'UTF-8') AS flag_eng,
# MAGIC     decode(unhex('F09F8FB4F3A081A7F3A081A2F3A081B3F3A081A3F3A081B4F3A081BF'), 'UTF-8') AS flag_sct,
# MAGIC     decode(unhex('F09F8FB4F3A081A7F3A081A2F3A081B7F3A081ACF3A081B3F3A081BF'), 'UTF-8') AS flag_wls,
# MAGIC
# MAGIC     -- GB regional-indicator flag (🇬🇧)
# MAGIC     concat(
# MAGIC       decode(unhex('F09F87AC'),'UTF-8'),
# MAGIC       decode(unhex('F09F87A7'),'UTF-8')
# MAGIC     ) AS flag_gb
# MAGIC ),
# MAGIC
# MAGIC base AS (
# MAGIC   SELECT
# MAGIC     n.nation_id,
# MAGIC     n.nation_name_display,
# MAGIC     n.flag_code,
# MAGIC     n.iso2_code,
# MAGIC
# MAGIC     -- Prefer iso2_code (canonical) when present; otherwise fall back to flag_code if it is ISO2-like.
# MAGIC     CASE
# MAGIC       WHEN upper(trim(coalesce(n.iso2_code, ''))) RLIKE '^[A-Z]{2}$'
# MAGIC         THEN upper(trim(n.iso2_code))
# MAGIC
# MAGIC       WHEN upper(trim(n.flag_code)) = 'UK'
# MAGIC         THEN 'GB'
# MAGIC
# MAGIC       WHEN upper(trim(coalesce(n.flag_code, ''))) RLIKE '^[A-Z]{2}$'
# MAGIC         THEN upper(trim(n.flag_code))
# MAGIC
# MAGIC       ELSE NULL
# MAGIC     END AS iso2
# MAGIC   FROM naf_catalog.gold_dim.nation_dim n
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   b.nation_id,
# MAGIC   b.flag_code,
# MAGIC   b.iso2,
# MAGIC
# MAGIC   CASE
# MAGIC     WHEN upper(trim(b.nation_name_display)) = 'ENGLAND'
# MAGIC       OR upper(trim(b.flag_code)) IN ('GB-ENG','ENG','GBENG')
# MAGIC       THEN f.flag_eng
# MAGIC
# MAGIC     WHEN upper(trim(b.nation_name_display)) = 'SCOTLAND'
# MAGIC       OR upper(trim(b.flag_code)) IN ('GB-SCT','SCT','GBSCT')
# MAGIC       THEN f.flag_sct
# MAGIC
# MAGIC     WHEN upper(trim(b.nation_name_display)) = 'WALES'
# MAGIC       OR upper(trim(b.flag_code)) IN ('GB-WLS','WLS','GBWLS')
# MAGIC       THEN f.flag_wls
# MAGIC
# MAGIC     WHEN upper(trim(b.nation_name_display)) IN ('NORTHERN IRELAND','N. IRELAND','N IRELAND')
# MAGIC       OR upper(trim(b.flag_code)) IN ('GB-NIR','NIR','GBNIR','NI')
# MAGIC       THEN f.flag_gb
# MAGIC
# MAGIC     -- Standard ISO2 country flags using Regional Indicator Symbols (decode/unhex)
# MAGIC     WHEN b.iso2 IS NOT NULL
# MAGIC       THEN concat(
# MAGIC         decode(
# MAGIC           unhex(
# MAGIC             concat(
# MAGIC               'F09F87',
# MAGIC               lpad(hex(166 + (ascii(substr(b.iso2, 1, 1)) - 65)), 2, '0')
# MAGIC             )
# MAGIC           ),
# MAGIC           'UTF-8'
# MAGIC         ),
# MAGIC         decode(
# MAGIC           unhex(
# MAGIC             concat(
# MAGIC               'F09F87',
# MAGIC               lpad(hex(166 + (ascii(substr(b.iso2, 2, 1)) - 65)), 2, '0')
# MAGIC             )
# MAGIC           ),
# MAGIC           'UTF-8'
# MAGIC         )
# MAGIC       )
# MAGIC
# MAGIC     ELSE '🌐'
# MAGIC   END AS flag_emoji
# MAGIC
# MAGIC FROM base b
# MAGIC CROSS JOIN flags f
# MAGIC ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,gold_presentation.nation_identity_v
# MAGIC %sql
# MAGIC -- VIEW: naf_catalog.gold_presentation.nation_identity_v
# MAGIC -- =====================================================================
# MAGIC -- LAYER        : GOLD_PRESENTATION
# MAGIC -- CONTRACT TYPE: Dashboard-facing identity (nation name + flag emoji)
# MAGIC --
# MAGIC -- PURPOSE
# MAGIC --   Provide a single reusable nation identity contract for dashboards:
# MAGIC --     - nation_name_display
# MAGIC --     - flag_emoji (with globe fallback handled in nation_flag_emoji_v)
# MAGIC --
# MAGIC -- OUTPUT GRAIN / KEYS
# MAGIC --   GRAIN       : 1 row per nation_id
# MAGIC --   PRIMARY KEY : nation_id
# MAGIC --
# MAGIC -- SOURCES
# MAGIC --   - naf_catalog.gold_dim.nation_dim
# MAGIC --   - naf_catalog.gold_presentation.nation_flag_emoji_v
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.nation_identity_v AS
# MAGIC SELECT
# MAGIC   n.nation_id,
# MAGIC   n.nation_name_display,
# MAGIC   COALESCE(f.flag_emoji, '🌐') AS flag_emoji
# MAGIC FROM naf_catalog.gold_dim.nation_dim n
# MAGIC LEFT JOIN naf_catalog.gold_presentation.nation_flag_emoji_v f
# MAGIC   ON n.nation_id = f.nation_id
# MAGIC ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,gold_presentation.coach_identity_v
# MAGIC %sql
# MAGIC -- VIEW: naf_catalog.gold_presentation.coach_identity_v
# MAGIC -- =====================================================================
# MAGIC -- LAYER        : GOLD_PRESENTATION
# MAGIC -- CONTRACT TYPE: Reusable identity shim (dashboard/export friendly)
# MAGIC --
# MAGIC -- PURPOSE
# MAGIC --   Provide a single, reusable coach identity contract for dashboards and presentation-layer views:
# MAGIC --     - coach display fields (name)
# MAGIC --     - nation display fields (name + emoji)
# MAGIC --
# MAGIC -- GRAIN / KEYS
# MAGIC --   GRAIN       : (coach_id)
# MAGIC --   PRIMARY KEY : (coach_id)
# MAGIC --
# MAGIC -- SOURCES
# MAGIC --   - naf_catalog.gold_dim.coach_dim                 (coach_id, coach_name, nation_id)
# MAGIC --   - naf_catalog.gold_presentation.nation_identity_v (nation_name_display, flag_emoji)
# MAGIC --
# MAGIC -- JOIN SEMANTICS
# MAGIC --   - LEFT JOIN to nation_identity_v to retain coaches even if nation_id is null/unmapped.
# MAGIC --
# MAGIC -- NOTES
# MAGIC --   - Keep this contract stable: downstream dashboards expect these column names.
# MAGIC --   - No measures/aggregations here; identity only.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.coach_identity_v AS
# MAGIC SELECT
# MAGIC   c.coach_id,
# MAGIC   c.coach_name,
# MAGIC   c.nation_id,
# MAGIC   ni.nation_name_display,
# MAGIC   ni.flag_emoji
# MAGIC FROM naf_catalog.gold_dim.coach_dim AS c
# MAGIC LEFT JOIN naf_catalog.gold_presentation.nation_identity_v AS ni
# MAGIC   ON c.nation_id = ni.nation_id;
# MAGIC

# COMMAND ----------

# DBTITLE 1,gold_presentation.tournament_identity_v
# MAGIC %sql
# MAGIC -- VIEW: naf_catalog.gold_presentation.tournament_identity_v
# MAGIC -- =====================================================================
# MAGIC -- LAYER        : GOLD_PRESENTATION
# MAGIC -- CONTRACT TYPE: Reusable identity shim (tournament + host nation display)
# MAGIC --
# MAGIC -- PURPOSE
# MAGIC --   Provide a single, reusable tournament identity contract for dashboards and presentation-layer views:
# MAGIC --     - tournament display fields (name, major flag)
# MAGIC --     - host nation display fields (name + emoji)
# MAGIC --
# MAGIC -- GRAIN / KEYS
# MAGIC --   GRAIN       : (tournament_id)
# MAGIC --   PRIMARY KEY : (tournament_id)
# MAGIC --
# MAGIC -- SOURCES
# MAGIC --   - naf_catalog.gold_dim.tournament_dim              (tournament attributes)
# MAGIC --   - naf_catalog.gold_presentation.nation_identity_v  (nation_name_display, flag_emoji)
# MAGIC --
# MAGIC -- JOIN SEMANTICS
# MAGIC --   - LEFT JOIN to nation_identity_v to retain tournaments even if nation_id is null/unmapped.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.tournament_identity_v AS
# MAGIC SELECT
# MAGIC   t.tournament_id,
# MAGIC   t.tournament_name,
# MAGIC   t.is_major_tournament,
# MAGIC
# MAGIC   t.nation_id,
# MAGIC   ni.nation_name_display,
# MAGIC   ni.flag_emoji,
# MAGIC
# MAGIC   t.start_date_id,
# MAGIC   t.end_date_id
# MAGIC FROM naf_catalog.gold_dim.tournament_dim AS t
# MAGIC LEFT JOIN naf_catalog.gold_presentation.nation_identity_v AS ni
# MAGIC   ON t.nation_id = ni.nation_id;
# MAGIC

# COMMAND ----------

# DBTITLE 1,gold_presentation.race_identity_v
# MAGIC %sql
# MAGIC -- VIEW: naf_catalog.gold_presentation.race_identity_v
# MAGIC -- =====================================================================
# MAGIC -- LAYER        : GOLD_PRESENTATION
# MAGIC -- CONTRACT TYPE: Dashboard-facing identity (race name display)
# MAGIC --
# MAGIC -- PURPOSE
# MAGIC --   Provide a single reusable race identity contract for dashboards:
# MAGIC --     - race_name_display (UI-friendly; maps GLOBAL sentinel to 'Global')
# MAGIC --
# MAGIC -- OUTPUT GRAIN / KEYS
# MAGIC --   GRAIN       : 1 row per race_id
# MAGIC --   PRIMARY KEY : race_id
# MAGIC --
# MAGIC -- SOURCES
# MAGIC --   - naf_catalog.gold_dim.race_dim
# MAGIC --
# MAGIC -- NOTES
# MAGIC --   - race_id = 0 is the GLOBAL sentinel, guaranteed to exist in race_dim (see 310).
# MAGIC --     This view renders it as 'Global' for UI display.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.race_identity_v AS
# MAGIC SELECT
# MAGIC   r.race_id,
# MAGIC   CASE
# MAGIC     WHEN r.race_id = 0 THEN 'Global'
# MAGIC     ELSE r.race_name
# MAGIC   END AS race_name_display
# MAGIC FROM naf_catalog.gold_dim.race_dim AS r;
# MAGIC
