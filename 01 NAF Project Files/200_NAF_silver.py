# Databricks notebook source
# MAGIC %md
# MAGIC # 200 — Silver Layer
# MAGIC
# MAGIC **Layer:** SILVER  |  **Status:** Production
# MAGIC **Pipeline position:** After 100 (Bronze)
# MAGIC
# MAGIC Canonicalises Bronze into clean, stable, joinable datasets. Standard types, snake_case, deterministic surrogate keys, and explainable dedup. No metrics or aggregates.
# MAGIC
# MAGIC ## Dependencies
# MAGIC - `bronze.*` — all raw ingest tables from 100
# MAGIC
# MAGIC ## Outputs
# MAGIC - `silver.country_codes_iso` — 1 row per ISO country (alpha-2 key)
# MAGIC - `silver.country_codes_fifa` — 1 row per FIFA country key
# MAGIC - `silver.country_reference` — 1 row per country key (FIFA + ISO merged)
# MAGIC - `silver.nation_manual_map` — nation name canonicalisation rules
# MAGIC - `silver.nations_entity` — 1 row per nation_id (deterministic surrogate key)
# MAGIC - `silver.coaches_clean` — 1 row per coach_id
# MAGIC - `silver.coach_rating_variant_clean` — 1 row per (coach_id, variant_id)
# MAGIC - `silver.games_clean` — 1 row per game_id
# MAGIC - `silver.races_clean` — 1 row per race_id
# MAGIC - `silver.tournaments_clean` — 1 row per tournament_id
# MAGIC - `silver.tournament_statistics_group_clean` — 1 row per (tournament_id, coach_id, stat_id)
# MAGIC - `silver.tournament_statistics_list_clean` — 1 row per stat_id
# MAGIC - `silver.tournament_coach_clean` — 1 row per (tournament_id, coach_id)
# MAGIC - `silver.variants_clean` — 1 row per variant_id
# MAGIC
# MAGIC **Design authority:** `NAF_Design_Specification.md`, `style_guides.md`
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create schema silver
# MAGIC %sql
# MAGIC -- DROP SCHEMA IF EXISTS naf_catalog.silver CASCADE;
# MAGIC CREATE SCHEMA IF NOT EXISTS naf_catalog.silver;

# COMMAND ----------

# DBTITLE 1,silver.country_codes_iso
# MAGIC %sql -- TABLE: naf_catalog.silver.country_codes_iso
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Clean ISO-3166 country code reference for nation mapping.
# MAGIC -- LAYER        : SILVER
# MAGIC -- GRAIN        : 1 row per ISO country (alpha-2/alpha-3 unique in source)
# MAGIC -- PRIMARY KEY  : (iso2_code)  -- practical key; iso3_code also unique in ISO list
# MAGIC -- SOURCES      : naf_catalog.bronze.iso_country_codes_raw
# MAGIC -- NOTES        : Keeps normalized join helpers (country_name_norm, country_key).
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.silver.country_codes_iso
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH cleaned AS (
# MAGIC   SELECT
# MAGIC     NULLIF(TRIM(English_short_name_lower_case), '') AS country_name_iso,
# MAGIC
# MAGIC     NULLIF(UPPER(TRIM(Alpha_2_code)), '')          AS iso2_code,
# MAGIC     NULLIF(UPPER(TRIM(Alpha_3_code)), '')          AS iso3_code,
# MAGIC
# MAGIC     -- numeric codes can have leading zeros; keep as string but normalize to 3 digits when possible
# MAGIC     CASE
# MAGIC       WHEN NULLIF(TRIM(Numeric_code), '') IS NULL THEN NULL
# MAGIC       ELSE LPAD(TRIM(Numeric_code), 3, '0')
# MAGIC     END AS iso_numeric_code,
# MAGIC
# MAGIC     NULLIF(TRIM(ISO_3166_2), '')                   AS iso_3166_2_code,
# MAGIC
# MAGIC     -- normalized join helpers
# MAGIC     UPPER(NULLIF(TRIM(English_short_name_lower_case), '')) AS country_name_norm,
# MAGIC     REGEXP_REPLACE(UPPER(NULLIF(TRIM(English_short_name_lower_case), '')), '[^A-Z0-9]', '') AS country_key,
# MAGIC
# MAGIC     -- carry Bronze ingestion metadata through Silver
# MAGIC     ingest_timestamp,
# MAGIC     ingest_source,
# MAGIC     file_name,
# MAGIC     source_url,
# MAGIC
# MAGIC     current_timestamp() AS load_timestamp
# MAGIC   FROM naf_catalog.bronze.iso_country_codes_raw
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM cleaned
# MAGIC WHERE iso2_code IS NOT NULL
# MAGIC   AND iso3_code IS NOT NULL;
# MAGIC

# COMMAND ----------

# DBTITLE 1,silver.country_codes_fifa
# MAGIC %sql -- TABLE: naf_catalog.silver.country_codes_fifa
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Clean FIFA country code reference for nation mapping.
# MAGIC -- LAYER        : SILVER
# MAGIC -- GRAIN        : 1 row per country_key
# MAGIC -- PRIMARY KEY  : country_key
# MAGIC -- SOURCES      : naf_catalog.bronze.fifa_country_codes_raw
# MAGIC -- NOTES        : Dedup keeps latest ingested record per country_key.
# MAGIC --                All codes are uppercase; join helper country_key is A-Z0-9 only.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.silver.country_codes_fifa
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH typed AS (
# MAGIC   SELECT
# MAGIC     -- preserve-but-clean name (trim + collapse whitespace)
# MAGIC     CASE
# MAGIC       WHEN country_name_raw IS NULL OR TRIM(country_name_raw) = '' THEN NULL
# MAGIC       ELSE REGEXP_REPLACE(TRIM(country_name_raw), '\\s+', ' ')
# MAGIC     END AS country_name_fifa,
# MAGIC
# MAGIC     -- normalized name helper
# MAGIC     UPPER(
# MAGIC       CASE
# MAGIC         WHEN country_name_raw IS NULL OR TRIM(country_name_raw) = '' THEN NULL
# MAGIC         ELSE REGEXP_REPLACE(TRIM(country_name_raw), '\\s+', ' ')
# MAGIC       END
# MAGIC     ) AS country_norm,
# MAGIC
# MAGIC     -- normalized join key
# MAGIC     REGEXP_REPLACE(
# MAGIC       UPPER(
# MAGIC         CASE
# MAGIC           WHEN country_name_raw IS NULL OR TRIM(country_name_raw) = '' THEN NULL
# MAGIC           ELSE REGEXP_REPLACE(TRIM(country_name_raw), '\\s+', ' ')
# MAGIC         END
# MAGIC       ),
# MAGIC       '[^A-Z0-9]',
# MAGIC       ''
# MAGIC     ) AS country_key,
# MAGIC
# MAGIC     NULLIF(UPPER(TRIM(fifa_code)), '') AS fifa_code,
# MAGIC
# MAGIC     CASE
# MAGIC       WHEN ioc_code IS NULL THEN NULL
# MAGIC       WHEN TRIM(ioc_code) IN ('', '-----') THEN NULL
# MAGIC       ELSE UPPER(TRIM(ioc_code))
# MAGIC     END AS ioc_code,
# MAGIC
# MAGIC     -- carry Bronze ingestion metadata through Silver
# MAGIC     ingest_timestamp,
# MAGIC     ingest_source,
# MAGIC     file_name,
# MAGIC     source_url,
# MAGIC     copyright_notice,
# MAGIC
# MAGIC     current_timestamp() AS load_timestamp
# MAGIC   FROM naf_catalog.bronze.fifa_country_codes_raw
# MAGIC ),
# MAGIC dedup AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY country_key
# MAGIC       ORDER BY ingest_timestamp DESC, file_name DESC, country_name_fifa ASC
# MAGIC     ) AS rn
# MAGIC   FROM typed
# MAGIC   WHERE country_key IS NOT NULL
# MAGIC     AND country_key <> ''
# MAGIC )
# MAGIC SELECT
# MAGIC   country_name_fifa,
# MAGIC   country_norm,
# MAGIC   country_key,
# MAGIC   fifa_code,
# MAGIC   ioc_code,
# MAGIC   ingest_timestamp,
# MAGIC   ingest_source,
# MAGIC   file_name,
# MAGIC   source_url,
# MAGIC   copyright_notice,
# MAGIC   load_timestamp
# MAGIC FROM dedup
# MAGIC WHERE rn = 1;
# MAGIC
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- OPTIONAL DQ (run after build)
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- %sql
# MAGIC -- -- expect 0 rows: country_key unique + non-null
# MAGIC -- SELECT country_key, COUNT(*) AS cnt
# MAGIC -- FROM naf_catalog.silver.country_codes_fifa
# MAGIC -- GROUP BY 1
# MAGIC -- HAVING country_key IS NULL OR country_key = '' OR COUNT(*) > 1;
# MAGIC

# COMMAND ----------

# DBTITLE 1,silver.country_reference
# MAGIC %sql -- TABLE: naf_catalog.silver.country_reference
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Unified country reference (FIFA + ISO) for mapping and enrichment.
# MAGIC -- LAYER        : SILVER
# MAGIC -- GRAIN        : 1 row per country_key
# MAGIC -- PRIMARY KEY  : country_key
# MAGIC -- SOURCES      : naf_catalog.silver.country_codes_fifa
# MAGIC --               naf_catalog.silver.country_codes_iso
# MAGIC -- NOTES        : Dedup chooses most recent ingest per country_key within each source.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.silver.country_reference
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH fifa_dedup AS (
# MAGIC   SELECT
# MAGIC     country_key,
# MAGIC     country_norm,
# MAGIC     country_name_fifa,
# MAGIC     fifa_code,
# MAGIC     ioc_code,
# MAGIC     ingest_timestamp,
# MAGIC     ingest_source,
# MAGIC     file_name
# MAGIC   FROM naf_catalog.silver.country_codes_fifa
# MAGIC   WHERE country_key IS NOT NULL AND country_key <> ''
# MAGIC   QUALIFY row_number() OVER (
# MAGIC     PARTITION BY country_key
# MAGIC     ORDER BY ingest_timestamp DESC, country_name_fifa
# MAGIC   ) = 1
# MAGIC ),
# MAGIC iso_dedup AS (
# MAGIC   SELECT
# MAGIC     country_key,
# MAGIC     country_name_norm,
# MAGIC     country_name_iso,
# MAGIC     iso2_code,
# MAGIC     iso3_code,
# MAGIC     iso_numeric_code,
# MAGIC     ingest_timestamp,
# MAGIC     ingest_source,
# MAGIC     file_name
# MAGIC   FROM naf_catalog.silver.country_codes_iso
# MAGIC   WHERE country_key IS NOT NULL AND country_key <> ''
# MAGIC   QUALIFY row_number() OVER (
# MAGIC     PARTITION BY country_key
# MAGIC     ORDER BY ingest_timestamp DESC, country_name_iso
# MAGIC   ) = 1
# MAGIC )
# MAGIC SELECT
# MAGIC   COALESCE(f.country_key,  i.country_key)  AS country_key,
# MAGIC   COALESCE(f.country_norm, i.country_name_norm) AS country_norm,
# MAGIC
# MAGIC   f.country_name_fifa,
# MAGIC   i.country_name_iso,
# MAGIC
# MAGIC   f.fifa_code,
# MAGIC   f.ioc_code,
# MAGIC   i.iso2_code,
# MAGIC   i.iso3_code,
# MAGIC   i.iso_numeric_code,
# MAGIC
# MAGIC   (f.country_key IS NOT NULL) AS has_fifa,
# MAGIC   (i.country_key IS NOT NULL) AS has_iso,
# MAGIC
# MAGIC   -- lineage (keep both sides)
# MAGIC   f.ingest_timestamp AS fifa_ingest_timestamp,
# MAGIC   f.ingest_source    AS fifa_ingest_source,
# MAGIC   f.file_name        AS fifa_file_name,
# MAGIC
# MAGIC   i.ingest_timestamp AS iso_ingest_timestamp,
# MAGIC   i.ingest_source    AS iso_ingest_source,
# MAGIC   i.file_name        AS iso_file_name,
# MAGIC
# MAGIC   current_timestamp() AS load_timestamp
# MAGIC FROM fifa_dedup f
# MAGIC FULL OUTER JOIN iso_dedup i
# MAGIC   ON f.country_key = i.country_key;
# MAGIC

# COMMAND ----------

# DBTITLE 1,silver.nation_manual_map
# MAGIC %sql -- TABLE: naf_catalog.silver.nation_manual_map
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Manual overrides for nation name canonicalization.
# MAGIC -- LAYER        : SILVER
# MAGIC -- GRAIN        : 1 row per nation_key_raw (deterministic normalized key)
# MAGIC -- PRIMARY KEY  : nation_key_raw
# MAGIC -- SOURCES      : Curated in-notebook VALUES list (versioned with pipeline code)
# MAGIC -- NOTES        : Join using nation_key_raw, not raw display strings.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.silver.nation_manual_map
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH src AS (
# MAGIC   SELECT
# MAGIC     NULLIF(TRIM(nation_name_raw), '')       AS nation_name_raw,
# MAGIC     NULLIF(TRIM(nation_name_canonical), '') AS nation_name_canonical,
# MAGIC     NULLIF(TRIM(comment), '')               AS comment
# MAGIC   FROM VALUES
# MAGIC     ('Bolivia',                    'Bolivia (Plurinational State of)',     'Map to ISO Bolivia'),
# MAGIC     ('Bosnia Herzegovina',         'Bosnia and Herzegovina',               'Map to ISO Bosnia and Herzegovina'),
# MAGIC     ('Cote Divoire (ivory Coast)', 'Côte d''Ivoire',                       'Map to ISO Côte d''Ivoire'),
# MAGIC     ('Iran',                       'Iran (Islamic Republic of)',           'Map to ISO Iran'),
# MAGIC     ('Korea',                      'Korea, Republic of',                   'Map to ISO South Korea'),
# MAGIC     ('Moldova',                    'Moldova, Republic of',                 'Map to ISO Moldova'),
# MAGIC     ('New Caledonia (french)',     'New Caledonia',                        'Map to ISO New Caledonia'),
# MAGIC     ('Pitcairn Island',            'Pitcairn',                             'Map to ISO Pitcairn'),
# MAGIC     ('Russia',                     'Russian Federation',                   'Map to ISO Russia'),
# MAGIC     ('St Pierre And Miquelon',     'Saint Pierre and Miquelon',            'Map to ISO Saint Pierre and Miquelon'),
# MAGIC     ('Taiwan',                     'Taiwan, Province of China',            'Map to ISO Taiwan'),
# MAGIC     ('United States',              'United States of America',             'Map to ISO USA (NAF: United States)'),
# MAGIC     ('United States Of America',   'United States of America',             'Map to ISO USA (NAF: United States Of America)'),
# MAGIC     ('Vatican',                    'Holy See (Vatican City State)',        'Map to ISO Vatican'),
# MAGIC     ('Venezuela',                  'Venezuela (Bolivarian Republic of)',   'Map to ISO Venezuela'),
# MAGIC     ('Virgin Islands (usa)',       'Virgin Islands (U.S.)',                'Map to ISO US Virgin Islands'),
# MAGIC     ('North Ireland',              'Northern Ireland',                     'Map to Northern Ireland (NAF home nation)'),
# MAGIC     ('Slovak Republic',            'Slovakia',                             'Map to ISO Slovakia')
# MAGIC   AS t(nation_name_raw, nation_name_canonical, comment)
# MAGIC )
# MAGIC SELECT
# MAGIC   nation_name_raw,
# MAGIC   nation_name_canonical,
# MAGIC   comment,
# MAGIC
# MAGIC   -- normalized join keys (use these for mapping logic)
# MAGIC   REGEXP_REPLACE(UPPER(nation_name_raw),       '[^A-Z0-9]', '') AS nation_key_raw,
# MAGIC   REGEXP_REPLACE(UPPER(nation_name_canonical), '[^A-Z0-9]', '') AS nation_key_canonical,
# MAGIC
# MAGIC   current_timestamp() AS load_timestamp
# MAGIC FROM src
# MAGIC WHERE nation_name_raw IS NOT NULL
# MAGIC   AND nation_name_canonical IS NOT NULL;
# MAGIC

# COMMAND ----------

# DBTITLE 1,silver.nations_entity
# MAGIC %sql -- TABLE: naf_catalog.silver.nations_entity
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Canonical nation entity table (deduped + mapped to ISO/FIFA where possible).
# MAGIC -- LAYER        : SILVER
# MAGIC -- GRAIN        : 1 row per nation_id (deterministic from group_key)
# MAGIC -- PRIMARY KEY  : nation_id
# MAGIC -- SOURCES      : naf_catalog.bronze.coach_export_raw
# MAGIC --               naf_catalog.bronze.tournament_raw
# MAGIC --               naf_catalog.silver.country_reference
# MAGIC --               naf_catalog.silver.nation_manual_map
# MAGIC -- NOTES        : nation_id is a deterministic surrogate: PMOD(XXHASH64(group_key), 2147483647).
# MAGIC --                Unmapped nations (excluding GB home nations) collapse to group_key='UNKNOWN'.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.silver.nations_entity
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH coach_nations AS (
# MAGIC   SELECT
# MAGIC     TRIM(c.Country) AS nation_name_input,
# MAGIC     INITCAP(LOWER(TRIM(c.Country))) AS nation_name_clean,
# MAGIC     REGEXP_REPLACE(UPPER(TRIM(c.Country)), '[^A-Z0-9]', '') AS nation_key
# MAGIC   FROM naf_catalog.bronze.coach_export_raw c
# MAGIC   WHERE c.Country IS NOT NULL
# MAGIC     AND TRIM(c.Country) <> ''
# MAGIC     AND COALESCE(TRY_CAST(c.NAF_Nr AS INT), -1) <> 9
# MAGIC ),
# MAGIC tournament_nations AS (
# MAGIC   SELECT
# MAGIC     TRIM(t.tournamentnation) AS nation_name_input,
# MAGIC     INITCAP(LOWER(TRIM(t.tournamentnation))) AS nation_name_clean,
# MAGIC     REGEXP_REPLACE(UPPER(TRIM(t.tournamentnation)), '[^A-Z0-9]', '') AS nation_key
# MAGIC   FROM naf_catalog.bronze.tournament_raw t
# MAGIC   WHERE t.tournamentnation IS NOT NULL
# MAGIC     AND TRIM(t.tournamentnation) <> ''
# MAGIC     AND TRY_CAST(t.naf_variantsid AS INT) IN (1, 13, 15)
# MAGIC ),
# MAGIC raw_nations AS (
# MAGIC   SELECT * FROM coach_nations
# MAGIC   UNION ALL
# MAGIC   SELECT * FROM tournament_nations
# MAGIC ),
# MAGIC distinct_nations AS (
# MAGIC   SELECT DISTINCT
# MAGIC     nation_name_input,
# MAGIC     nation_name_clean,
# MAGIC     nation_key
# MAGIC   FROM raw_nations
# MAGIC   WHERE nation_key IS NOT NULL
# MAGIC     AND nation_key <> ''
# MAGIC ),
# MAGIC
# MAGIC auto_map AS (
# MAGIC   SELECT
# MAGIC     r.nation_name_input,
# MAGIC     r.nation_name_clean,
# MAGIC     r.nation_key,
# MAGIC
# MAGIC     cr.country_key        AS auto_country_key,
# MAGIC     cr.country_name_iso   AS iso_nation_name,
# MAGIC     cr.country_name_fifa  AS fifa_nation_name,
# MAGIC     cr.iso2_code,
# MAGIC     cr.iso3_code,
# MAGIC     cr.iso_numeric_code,
# MAGIC     cr.fifa_code,
# MAGIC     cr.ioc_code
# MAGIC   FROM distinct_nations r
# MAGIC   LEFT JOIN naf_catalog.silver.country_reference cr
# MAGIC     ON r.nation_key = cr.country_key
# MAGIC ),
# MAGIC
# MAGIC manual_map AS (
# MAGIC   SELECT
# MAGIC     a.nation_name_input,
# MAGIC     a.nation_name_clean,
# MAGIC     a.nation_key,
# MAGIC
# MAGIC     -- manual map table should expose normalized keys
# MAGIC     m.nation_key_canonical AS manual_country_key,
# MAGIC
# MAGIC     cr2.country_name_iso   AS manual_iso_name,
# MAGIC     cr2.country_name_fifa  AS manual_fifa_name,
# MAGIC     cr2.iso2_code          AS manual_iso2,
# MAGIC     cr2.iso3_code          AS manual_iso3,
# MAGIC     cr2.iso_numeric_code   AS manual_iso_numeric,
# MAGIC     cr2.fifa_code          AS manual_fifa,
# MAGIC     cr2.ioc_code           AS manual_ioc,
# MAGIC
# MAGIC     a.auto_country_key,
# MAGIC     a.iso_nation_name,
# MAGIC     a.fifa_nation_name,
# MAGIC     a.iso2_code,
# MAGIC     a.iso3_code,
# MAGIC     a.iso_numeric_code,
# MAGIC     a.fifa_code,
# MAGIC     a.ioc_code
# MAGIC   FROM auto_map a
# MAGIC   LEFT JOIN naf_catalog.silver.nation_manual_map m
# MAGIC     ON a.nation_key = m.nation_key_raw
# MAGIC   LEFT JOIN naf_catalog.silver.country_reference cr2
# MAGIC     ON m.nation_key_canonical = cr2.country_key
# MAGIC ),
# MAGIC
# MAGIC resolved AS (
# MAGIC   SELECT
# MAGIC     nation_name_input,
# MAGIC     nation_name_clean,
# MAGIC     nation_key,
# MAGIC
# MAGIC     COALESCE(manual_country_key, auto_country_key) AS nation_key_canonical,
# MAGIC
# MAGIC     COALESCE(manual_iso_name,  iso_nation_name)  AS iso_nation_name,
# MAGIC     COALESCE(manual_fifa_name, fifa_nation_name) AS fifa_nation_name,
# MAGIC
# MAGIC     COALESCE(manual_iso2, iso2_code)               AS iso2_code,
# MAGIC     COALESCE(manual_iso3, iso3_code)               AS iso3_code,
# MAGIC     COALESCE(manual_iso_numeric, iso_numeric_code) AS iso_numeric_code,
# MAGIC     COALESCE(manual_fifa, fifa_code)               AS fifa_code,
# MAGIC     COALESCE(manual_ioc,  ioc_code)                AS ioc_code
# MAGIC   FROM manual_map
# MAGIC ),
# MAGIC
# MAGIC grouped AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     CASE
# MAGIC       WHEN iso2_code IS NULL AND iso3_code IS NULL AND fifa_code IS NULL AND ioc_code IS NULL
# MAGIC        AND COALESCE(nation_key_canonical, nation_key) NOT IN ('ENGLAND','SCOTLAND','WALES','NORTHERNIRELAND')
# MAGIC         THEN 'UNKNOWN'
# MAGIC       ELSE COALESCE(nation_key_canonical, nation_key)
# MAGIC     END AS group_key
# MAGIC   FROM resolved
# MAGIC ),
# MAGIC
# MAGIC dedup AS (
# MAGIC   SELECT
# MAGIC     group_key,
# MAGIC
# MAGIC     -- representative observed raw key (debug only)
# MAGIC     MIN(nation_key) AS nation_key,
# MAGIC
# MAGIC     -- canonical identity key (always populated)
# MAGIC     group_key AS nation_key_canonical,
# MAGIC
# MAGIC     -- stable “best” name deterministically (prefer FIFA, then ISO, else cleaned input)
# MAGIC     MAX_BY(
# MAGIC       COALESCE(fifa_nation_name, iso_nation_name, nation_name_clean),
# MAGIC       NAMED_STRUCT(
# MAGIC         'score', CASE
# MAGIC                    WHEN fifa_nation_name IS NOT NULL THEN 2
# MAGIC                    WHEN iso_nation_name  IS NOT NULL THEN 1
# MAGIC                    ELSE 0
# MAGIC                  END,
# MAGIC         'name',  COALESCE(fifa_nation_name, iso_nation_name, nation_name_clean)
# MAGIC       )
# MAGIC     ) AS nation_name,
# MAGIC
# MAGIC     MAX(iso_nation_name)  AS iso_nation_name,
# MAGIC     MAX(fifa_nation_name) AS fifa_nation_name,
# MAGIC
# MAGIC     MAX(iso2_code)        AS iso2_code,
# MAGIC     MAX(iso3_code)        AS iso3_code,
# MAGIC     MAX(iso_numeric_code) AS iso_numeric_code,
# MAGIC     MAX(fifa_code)        AS fifa_code,
# MAGIC     MAX(ioc_code)         AS ioc_code
# MAGIC   FROM grouped
# MAGIC   GROUP BY group_key
# MAGIC ),
# MAGIC
# MAGIC final AS (
# MAGIC   SELECT
# MAGIC     CASE
# MAGIC       WHEN group_key = 'UNKNOWN' THEN 0
# MAGIC       ELSE CAST(PMOD(XXHASH64(group_key), 2147483647) AS INT)
# MAGIC     END AS nation_id,
# MAGIC
# MAGIC     nation_key,
# MAGIC     nation_key_canonical,
# MAGIC     group_key,  -- keep for Silver debug/audit (optional but recommended)
# MAGIC
# MAGIC     CASE WHEN group_key = 'UNKNOWN' THEN 'Unknown' ELSE nation_name END AS nation_name,
# MAGIC
# MAGIC     CASE
# MAGIC       WHEN group_key = 'NORTHERNIRELAND'       THEN 'Northern Ireland'
# MAGIC       WHEN group_key = 'UNITEDSTATESOFAMERICA' THEN 'United States'
# MAGIC       WHEN group_key = 'TAIWANPROVINCEOFCHINA' THEN 'Taiwan'
# MAGIC       WHEN group_key = 'UNKNOWN'               THEN 'Unknown'
# MAGIC       ELSE nation_name
# MAGIC     END AS nation_name_display,
# MAGIC
# MAGIC     iso_nation_name,
# MAGIC     fifa_nation_name,
# MAGIC     iso2_code,
# MAGIC     iso3_code,
# MAGIC     iso_numeric_code,
# MAGIC     fifa_code,
# MAGIC     ioc_code,
# MAGIC
# MAGIC     CASE
# MAGIC       WHEN group_key = 'ENGLAND'          THEN 'GB-ENG'
# MAGIC       WHEN group_key = 'SCOTLAND'         THEN 'GB-SCT'
# MAGIC       WHEN group_key = 'WALES'            THEN 'GB-WLS'
# MAGIC       WHEN group_key = 'NORTHERNIRELAND'  THEN 'GB-NIR'
# MAGIC       WHEN iso2_code IS NOT NULL          THEN UPPER(iso2_code)
# MAGIC       ELSE CAST(NULL AS STRING)
# MAGIC     END AS flag_code,
# MAGIC
# MAGIC     current_timestamp() AS load_timestamp
# MAGIC   FROM dedup
# MAGIC ),
# MAGIC
# MAGIC unknown_row AS (
# MAGIC   SELECT
# MAGIC     0 AS nation_id,
# MAGIC     CAST(NULL AS STRING) AS nation_key,
# MAGIC     'UNKNOWN' AS nation_key_canonical,
# MAGIC     'UNKNOWN' AS group_key,                -- ✅ added to match final
# MAGIC
# MAGIC     'Unknown' AS nation_name,
# MAGIC     'Unknown' AS nation_name_display,
# MAGIC
# MAGIC     CAST(NULL AS STRING) AS iso_nation_name,
# MAGIC     CAST(NULL AS STRING) AS fifa_nation_name,
# MAGIC     CAST(NULL AS STRING) AS iso2_code,
# MAGIC     CAST(NULL AS STRING) AS iso3_code,
# MAGIC     CAST(NULL AS STRING) AS iso_numeric_code,
# MAGIC     CAST(NULL AS STRING) AS fifa_code,
# MAGIC     CAST(NULL AS STRING) AS ioc_code,
# MAGIC     CAST(NULL AS STRING) AS flag_code,
# MAGIC
# MAGIC     current_timestamp() AS load_timestamp
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   nation_id,
# MAGIC   nation_key,
# MAGIC   nation_key_canonical,
# MAGIC   group_key,
# MAGIC   nation_name,
# MAGIC   nation_name_display,
# MAGIC   iso_nation_name,
# MAGIC   fifa_nation_name,
# MAGIC   iso2_code,
# MAGIC   iso3_code,
# MAGIC   iso_numeric_code,
# MAGIC   fifa_code,
# MAGIC   ioc_code,
# MAGIC   flag_code,
# MAGIC   load_timestamp
# MAGIC FROM final
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   nation_id,
# MAGIC   nation_key,
# MAGIC   nation_key_canonical,
# MAGIC   group_key,
# MAGIC   nation_name,
# MAGIC   nation_name_display,
# MAGIC   iso_nation_name,
# MAGIC   fifa_nation_name,
# MAGIC   iso2_code,
# MAGIC   iso3_code,
# MAGIC   iso_numeric_code,
# MAGIC   fifa_code,
# MAGIC   ioc_code,
# MAGIC   flag_code,
# MAGIC   load_timestamp
# MAGIC FROM unknown_row
# MAGIC WHERE NOT EXISTS (
# MAGIC   SELECT 1
# MAGIC   FROM final
# MAGIC   WHERE nation_id = 0
# MAGIC );
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,silver.coaches_clean
# MAGIC %sql -- TABLE: naf_catalog.silver.coaches_clean
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Canonical cleaned coaches table (types + nation_id mapping).
# MAGIC -- LAYER        : SILVER
# MAGIC -- GRAIN        : 1 row per coach_id
# MAGIC -- PRIMARY KEY  : coach_id
# MAGIC -- SOURCES      : naf_catalog.bronze.coach_export_raw
# MAGIC --               naf_catalog.silver.nations_entity
# MAGIC -- NOTES        : coach_name follows “preserve-but-clean” (normalize spaces; only fix casing
# MAGIC --                when it’s obviously all-lower or all-upper).
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.silver.coaches_clean
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH base AS (
# MAGIC   SELECT
# MAGIC     TRY_CAST(c.NAF_Nr AS INT) AS coach_id,
# MAGIC
# MAGIC     -- preserve-but-clean coach name: normalize whitespace; fix obvious all-lower/all-upper casing
# MAGIC     CASE
# MAGIC       WHEN c.NAF_name IS NULL OR TRIM(c.NAF_name) = '' THEN NULL
# MAGIC       ELSE
# MAGIC         CASE
# MAGIC           WHEN REGEXP_REPLACE(TRIM(c.NAF_name), '\\s+', ' ')
# MAGIC                = LOWER(REGEXP_REPLACE(TRIM(c.NAF_name), '\\s+', ' '))
# MAGIC             THEN INITCAP(LOWER(REGEXP_REPLACE(TRIM(c.NAF_name), '\\s+', ' ')))
# MAGIC           WHEN REGEXP_REPLACE(TRIM(c.NAF_name), '\\s+', ' ')
# MAGIC                = UPPER(REGEXP_REPLACE(TRIM(c.NAF_name), '\\s+', ' '))
# MAGIC             THEN INITCAP(LOWER(REGEXP_REPLACE(TRIM(c.NAF_name), '\\s+', ' ')))
# MAGIC           ELSE REGEXP_REPLACE(TRIM(c.NAF_name), '\\s+', ' ')
# MAGIC         END
# MAGIC     END AS coach_name,
# MAGIC
# MAGIC     INITCAP(LOWER(TRIM(c.Country))) AS coach_nation,
# MAGIC     REGEXP_REPLACE(UPPER(TRIM(c.Country)), '[^A-Z0-9]', '') AS nation_key,
# MAGIC
# MAGIC     TRY_CAST(c.Registration_Date AS DATE) AS date_registration,
# MAGIC     TRY_CAST(c.Expiry_Date       AS DATE) AS date_expiry,
# MAGIC
# MAGIC     -- Bronze lineage (standard)
# MAGIC     c.ingest_timestamp,
# MAGIC     c.ingest_source,
# MAGIC     c.file_name
# MAGIC   FROM naf_catalog.bronze.coach_export_raw c
# MAGIC   WHERE TRY_CAST(c.NAF_Nr AS INT) IS NOT NULL
# MAGIC     AND TRY_CAST(c.NAF_Nr AS INT) <> 9
# MAGIC ),
# MAGIC nation_lookup AS (
# MAGIC   -- allow matching on either raw nation_key or canonical nation_key_canonical
# MAGIC   SELECT nation_id, nation_key AS lookup_key, 0 AS precedence
# MAGIC   FROM naf_catalog.silver.nations_entity
# MAGIC   WHERE nation_key IS NOT NULL AND nation_key <> ''
# MAGIC
# MAGIC   UNION ALL
# MAGIC
# MAGIC   SELECT nation_id, nation_key_canonical AS lookup_key, 1 AS precedence
# MAGIC   FROM naf_catalog.silver.nations_entity
# MAGIC   WHERE nation_key_canonical IS NOT NULL AND nation_key_canonical <> ''
# MAGIC ),
# MAGIC resolved AS (
# MAGIC   SELECT
# MAGIC     b.*,
# MAGIC     nl.nation_id,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY b.coach_id
# MAGIC       ORDER BY COALESCE(nl.precedence, 99)
# MAGIC     ) AS rn
# MAGIC   FROM base b
# MAGIC   LEFT JOIN nation_lookup nl
# MAGIC     ON b.nation_key = nl.lookup_key
# MAGIC )
# MAGIC SELECT
# MAGIC   coach_id,
# MAGIC   coach_name,
# MAGIC   coach_nation,
# MAGIC   COALESCE(nation_id, 0) AS nation_id,
# MAGIC   date_registration,
# MAGIC   date_expiry,
# MAGIC
# MAGIC   ingest_timestamp,
# MAGIC   ingest_source,
# MAGIC   file_name,
# MAGIC
# MAGIC   current_timestamp() AS load_timestamp
# MAGIC FROM resolved
# MAGIC WHERE rn = 1;
# MAGIC

# COMMAND ----------

# DBTITLE 1,silver.coach_rating_variant_clean
# MAGIC %sql -- TABLE: naf_catalog.silver.coach_rating_variant_clean
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Clean coach rating snapshot records by variant/race (typed + deduped).
# MAGIC -- LAYER        : SILVER
# MAGIC -- GRAIN        : 1 row per (coach_id, race_id, variant_id, rating_update_timestamp)
# MAGIC -- PRIMARY KEY  : (coach_id, race_id, variant_id, rating_update_timestamp)
# MAGIC -- SOURCES      : naf_catalog.bronze.coach_ranking_variant_raw
# MAGIC -- NOTES        : Parses Bronze dateUpdate (STRING) → rating_update_timestamp (TIMESTAMP).
# MAGIC --                Adds rating_update_date + date_id (YYYYMMDD) deterministically from timestamp.
# MAGIC --                Dedup keeps latest ingested copy of the same logical record.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.silver.coach_rating_variant_clean
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH typed AS (
# MAGIC   SELECT
# MAGIC     TRY_CAST(b.coachID   AS INT) AS coach_id,
# MAGIC     TRY_CAST(b.raceID    AS INT) AS race_id,
# MAGIC     TRY_CAST(b.variantID AS INT) AS variant_id,
# MAGIC
# MAGIC     TRY_TO_TIMESTAMP(NULLIF(TRIM(b.dateUpdate), ''), 'yyyy-MM-dd HH:mm:ss') AS rating_update_timestamp,
# MAGIC
# MAGIC     TRY_CAST(b.ranking      AS DOUBLE) AS rating_value,
# MAGIC     TRY_CAST(b.ranking_temp AS DOUBLE) AS rating_value_temp,
# MAGIC
# MAGIC     -- Bronze lineage (standard)
# MAGIC     b.ingest_timestamp,
# MAGIC     b.ingest_source,
# MAGIC     b.file_name,
# MAGIC
# MAGIC     current_timestamp() AS load_timestamp
# MAGIC   FROM naf_catalog.bronze.coach_ranking_variant_raw b
# MAGIC   WHERE TRY_CAST(b.variantID AS INT) IN (1, 13, 15)
# MAGIC     AND TRY_CAST(b.coachID AS INT) IS NOT NULL
# MAGIC     AND TRY_CAST(b.coachID AS INT) <> 9
# MAGIC ),
# MAGIC filtered AS (
# MAGIC   SELECT *
# MAGIC   FROM typed
# MAGIC   WHERE rating_update_timestamp IS NOT NULL
# MAGIC ),
# MAGIC dedup AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     TO_DATE(rating_update_timestamp) AS rating_update_date,
# MAGIC     CAST(date_format(TO_DATE(rating_update_timestamp), 'yyyyMMdd') AS INT) AS date_id,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY coach_id, race_id, variant_id, rating_update_timestamp
# MAGIC       ORDER BY ingest_timestamp DESC, file_name DESC
# MAGIC     ) AS rn
# MAGIC   FROM filtered
# MAGIC )
# MAGIC SELECT
# MAGIC   coach_id,
# MAGIC   race_id,
# MAGIC   variant_id,
# MAGIC   rating_update_timestamp,
# MAGIC   rating_update_date,
# MAGIC   date_id,
# MAGIC   rating_value,
# MAGIC   rating_value_temp,
# MAGIC   ingest_timestamp,
# MAGIC   ingest_source,
# MAGIC   file_name,
# MAGIC   load_timestamp
# MAGIC FROM dedup
# MAGIC WHERE rn = 1;
# MAGIC
# MAGIC -- OPTIONAL DQ (run after build)
# MAGIC -- %sql
# MAGIC -- SELECT COUNT(*) AS bad_null_timestamps
# MAGIC -- FROM naf_catalog.silver.coach_rating_variant_clean
# MAGIC -- WHERE rating_update_timestamp IS NULL;
# MAGIC

# COMMAND ----------

# DBTITLE 1,silver.games_clean
# MAGIC %sql
# MAGIC -- TABLE: naf_catalog.silver.games_clean
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Canonical cleaned games feed (Silver), with coach referential integrity enforced.
# MAGIC -- LAYER        : SILVER
# MAGIC -- GRAIN        : 1 row per game_id (supported variants only).
# MAGIC -- PRIMARY KEY  : game_id
# MAGIC -- SOURCES      : naf_catalog.bronze.game_raw
# MAGIC --               naf_catalog.silver.coaches_clean  (authoritative coach set)
# MAGIC -- NOTES        : Silver enforces NOT NULL for emitted *_id columns, removes self-play games, and race_id = 0,
# MAGIC --               deduplicates by game_id (latest ingest).
# MAGIC --               Policy: keep only games where BOTH home/away coach_id exist in silver.coaches_clean.
# MAGIC --               source_season_num is passthrough metadata from the source (not a modeled key; may be incomplete).
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.silver.games_clean
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH typed_raw AS (
# MAGIC   SELECT
# MAGIC     TRY_CAST(b.gameid       AS INT) AS game_id,
# MAGIC     TRY_CAST(b.seasonid     AS INT) AS source_season_num,
# MAGIC     TRY_CAST(b.tournamentid AS INT) AS tournament_id,
# MAGIC
# MAGIC     TRY_CAST(b.homecoachid  AS INT) AS home_coach_id,
# MAGIC     TRY_CAST(b.awaycoachid  AS INT) AS away_coach_id,
# MAGIC
# MAGIC     TRY_CAST(b.racehome     AS INT) AS home_race_id,
# MAGIC     TRY_CAST(b.raceaway     AS INT) AS away_race_id,
# MAGIC
# MAGIC     TRY_CAST(b.goalshome    AS INT) AS td_home,
# MAGIC     TRY_CAST(b.goalsaway    AS INT) AS td_away,
# MAGIC
# MAGIC     TRY_CAST(b.rephome            AS DOUBLE) / 100 AS elo_home_before,
# MAGIC     TRY_CAST(b.repaway            AS DOUBLE) / 100 AS elo_away_before,
# MAGIC     TRY_CAST(b.rephome_calibrated AS DOUBLE) / 100 AS elo_home_after,
# MAGIC     TRY_CAST(b.repaway_calibrated AS DOUBLE) / 100 AS elo_away_after,
# MAGIC
# MAGIC     TRY_TO_DATE(NULLIF(TRIM(b.date), ''), 'yyyy-MM-dd') AS game_date,
# MAGIC     TRY_CAST(b.hour AS INT) AS event_hour,
# MAGIC
# MAGIC     CASE
# MAGIC       WHEN NULLIF(TRIM(b.newdate), '') IS NULL
# MAGIC         OR TRIM(b.newdate) = '0000-00-00 00:00:00'
# MAGIC       THEN TRY_TO_TIMESTAMP(
# MAGIC         CONCAT(
# MAGIC           NULLIF(TRIM(b.date), ''),
# MAGIC           ' ',
# MAGIC           LPAD(CAST(COALESCE(TRY_CAST(b.hour AS INT), 0) AS STRING), 2, '0'),
# MAGIC           ':00:00'
# MAGIC         ),
# MAGIC         'yyyy-MM-dd HH:mm:ss'
# MAGIC       )
# MAGIC       ELSE TRY_TO_TIMESTAMP(TRIM(b.newdate), 'yyyy-MM-dd HH:mm:ss')
# MAGIC     END AS event_timestamp,
# MAGIC
# MAGIC     TRY_CAST(b.naf_variantsid AS INT) AS variant_id,
# MAGIC
# MAGIC     b.ingest_timestamp,
# MAGIC     b.ingest_source,
# MAGIC     b.file_name,
# MAGIC
# MAGIC     current_timestamp() AS load_timestamp
# MAGIC   FROM naf_catalog.bronze.game_raw b
# MAGIC   WHERE TRY_CAST(b.naf_variantsid AS INT) IN (1, 13, 15)
# MAGIC     AND TRY_CAST(b.gameid AS INT) IS NOT NULL
# MAGIC ),
# MAGIC typed AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     CAST(date_format(game_date, 'yyyyMMdd') AS INT) AS date_id
# MAGIC   FROM typed_raw
# MAGIC ),
# MAGIC valid_coaches AS (
# MAGIC   SELECT DISTINCT coach_id
# MAGIC   FROM naf_catalog.silver.coaches_clean
# MAGIC   WHERE coach_id IS NOT NULL
# MAGIC ),
# MAGIC typed_valid AS (
# MAGIC   SELECT t.*
# MAGIC   FROM typed t
# MAGIC   INNER JOIN valid_coaches hc
# MAGIC     ON t.home_coach_id = hc.coach_id
# MAGIC   INNER JOIN valid_coaches ac
# MAGIC     ON t.away_coach_id = ac.coach_id
# MAGIC ),
# MAGIC dedup AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY game_id
# MAGIC       ORDER BY ingest_timestamp DESC, file_name DESC
# MAGIC     ) AS rn
# MAGIC   FROM typed_valid
# MAGIC   WHERE
# MAGIC     -- Strict NOT NULL ids (Silver responsibility)
# MAGIC     tournament_id   IS NOT NULL
# MAGIC     AND variant_id  IS NOT NULL
# MAGIC     AND date_id     IS NOT NULL
# MAGIC
# MAGIC     AND home_coach_id IS NOT NULL
# MAGIC     AND away_coach_id IS NOT NULL
# MAGIC     AND home_race_id  IS NOT NULL AND home_race_id <> 0
# MAGIC     AND away_race_id  IS NOT NULL AND away_race_id <> 0
# MAGIC
# MAGIC     -- Existing exclusion rule + self-play removal
# MAGIC     AND home_coach_id <> 9
# MAGIC     AND away_coach_id <> 9
# MAGIC     AND home_coach_id <> away_coach_id
# MAGIC )
# MAGIC SELECT
# MAGIC   game_id,
# MAGIC   source_season_num,
# MAGIC   tournament_id,
# MAGIC
# MAGIC   home_coach_id,
# MAGIC   away_coach_id,
# MAGIC
# MAGIC   home_race_id,
# MAGIC   away_race_id,
# MAGIC
# MAGIC   td_home,
# MAGIC   td_away,
# MAGIC
# MAGIC   elo_home_before,
# MAGIC   elo_away_before,
# MAGIC   elo_home_after,
# MAGIC   elo_away_after,
# MAGIC
# MAGIC   event_timestamp,
# MAGIC   game_date,
# MAGIC   date_id,
# MAGIC   event_hour,
# MAGIC
# MAGIC   variant_id,
# MAGIC
# MAGIC   ingest_timestamp,
# MAGIC   ingest_source,
# MAGIC   file_name,
# MAGIC
# MAGIC   load_timestamp
# MAGIC FROM dedup
# MAGIC WHERE rn = 1;
# MAGIC

# COMMAND ----------

# DBTITLE 1,silver.races_clean
# MAGIC %sql
# MAGIC -- TABLE: naf_catalog.silver.races_clean
# MAGIC -- PURPOSE     : Canonical cleaned race lookup (typed + normalized booleans) for joins.
# MAGIC -- GRAIN       : 1 row per race_id
# MAGIC -- PRIMARY KEY : race_id
# MAGIC -- NOTES       : Enforce counted races (plus Bretonnians=26 exception) in Silver; dedup by race_id.
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.silver.races_clean
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH typed AS (
# MAGIC   SELECT
# MAGIC     TRY_CAST(b.raceid AS INT) AS race_id,
# MAGIC     NULLIF(TRIM(b.name), '')  AS race_name,
# MAGIC     TRY_CAST(b.order_ AS INT) AS sort_order,
# MAGIC
# MAGIC     CASE
# MAGIC       WHEN b.selectable IS NULL THEN NULL
# MAGIC       WHEN LOWER(TRIM(CAST(b.selectable AS STRING))) IN ('y','yes','true','1') THEN TRUE
# MAGIC       WHEN LOWER(TRIM(CAST(b.selectable AS STRING))) IN ('n','no','false','0') THEN FALSE
# MAGIC       ELSE NULL
# MAGIC     END AS race_selectable,
# MAGIC
# MAGIC     CASE
# MAGIC       WHEN b.race_count IS NULL THEN NULL
# MAGIC       WHEN LOWER(TRIM(CAST(b.race_count AS STRING))) IN ('y','yes','true','1') THEN TRUE
# MAGIC       WHEN LOWER(TRIM(CAST(b.race_count AS STRING))) IN ('n','no','false','0') THEN FALSE
# MAGIC       ELSE NULL
# MAGIC     END AS race_is_counted,
# MAGIC
# MAGIC     b.ingest_timestamp,
# MAGIC     b.ingest_source,
# MAGIC     b.file_name,
# MAGIC
# MAGIC     current_timestamp() AS load_timestamp
# MAGIC   FROM naf_catalog.bronze.race_raw b
# MAGIC   WHERE TRY_CAST(b.raceid AS INT) IS NOT NULL
# MAGIC ),
# MAGIC filtered AS (
# MAGIC   SELECT *
# MAGIC   FROM typed
# MAGIC   WHERE race_id <> 0
# MAGIC     AND (race_is_counted = TRUE OR race_id = 26) -- include Bretonnians
# MAGIC ),
# MAGIC dedup AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY race_id
# MAGIC       ORDER BY ingest_timestamp DESC, file_name DESC
# MAGIC     ) AS rn
# MAGIC   FROM filtered
# MAGIC )
# MAGIC SELECT
# MAGIC   race_id,
# MAGIC   race_name,
# MAGIC   sort_order,
# MAGIC   race_selectable,
# MAGIC   race_is_counted,
# MAGIC   ingest_timestamp,
# MAGIC   ingest_source,
# MAGIC   file_name,
# MAGIC   load_timestamp
# MAGIC FROM dedup
# MAGIC WHERE rn = 1;
# MAGIC

# COMMAND ----------

# DBTITLE 1,silver.tournaments_clean
# MAGIC %sql
# MAGIC -- TABLE: naf_catalog.silver.tournaments_clean
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Canonical cleaned tournaments table (types + nation_id mapping + date_id keys),
# MAGIC --                restricted to tournaments that appear in naf_catalog.silver.games_clean.
# MAGIC -- LAYER        : SILVER
# MAGIC -- GRAIN        : 1 row per tournament_id
# MAGIC -- PRIMARY KEY  : tournament_id
# MAGIC -- SOURCES      : naf_catalog.bronze.tournament_raw
# MAGIC --               naf_catalog.silver.nations_entity
# MAGIC --               naf_catalog.silver.games_clean (tournament coverage filter)
# MAGIC -- NOTES        : - Tournaments with no games are excluded (semi-join to games_clean).
# MAGIC --                - Any tournament_id referenced by games_clean but missing in bronze export
# MAGIC --                  is generated as a deterministic "Unknown Tournament <id>" stub row
# MAGIC --                  (so Gold_dim / Gold_fact can maintain FK integrity).
# MAGIC --                - date_end defaults to date_start when missing (single-date convention).
# MAGIC --                - start_date_id/end_date_id are YYYYMMDD INT keys derived from date_start/date_end.
# MAGIC --                - nation_id uses 0 as Unknown when not mapped.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.silver.tournaments_clean
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH tournament_ids_in_games AS (
# MAGIC   SELECT DISTINCT tournament_id
# MAGIC   FROM naf_catalog.silver.games_clean
# MAGIC   WHERE tournament_id IS NOT NULL
# MAGIC ),
# MAGIC base AS (
# MAGIC   SELECT
# MAGIC     TRY_CAST(t.tournamentid          AS INT) AS tournament_id,
# MAGIC     TRY_CAST(t.tournamentorganizerid AS INT) AS organizer_coach_id,
# MAGIC
# MAGIC     -- preserve-but-clean tournament name (normalize whitespace; fix obvious all-lower/all-upper)
# MAGIC     CASE
# MAGIC       WHEN t.tournamentname IS NULL OR TRIM(t.tournamentname) = '' THEN NULL
# MAGIC       ELSE
# MAGIC         CASE
# MAGIC           WHEN REGEXP_REPLACE(TRIM(t.tournamentname), '\\s+', ' ')
# MAGIC                = LOWER(REGEXP_REPLACE(TRIM(t.tournamentname), '\\s+', ' '))
# MAGIC             THEN INITCAP(LOWER(REGEXP_REPLACE(TRIM(t.tournamentname), '\\s+', ' ')))
# MAGIC           WHEN REGEXP_REPLACE(TRIM(t.tournamentname), '\\s+', ' ')
# MAGIC                = UPPER(REGEXP_REPLACE(TRIM(t.tournamentname), '\\s+', ' '))
# MAGIC             THEN INITCAP(LOWER(REGEXP_REPLACE(TRIM(t.tournamentname), '\\s+', ' ')))
# MAGIC           ELSE REGEXP_REPLACE(TRIM(t.tournamentname), '\\s+', ' ')
# MAGIC         END
# MAGIC     END AS tournament_name,
# MAGIC
# MAGIC     -- nation label + join key
# MAGIC     INITCAP(LOWER(TRIM(t.tournamentnation))) AS tournament_nation,
# MAGIC     REGEXP_REPLACE(UPPER(TRIM(t.tournamentnation)), '[^A-Z0-9]', '') AS nation_key,
# MAGIC
# MAGIC     TRY_TO_DATE(t.tournamentstartdate, 'yyyy-MM-dd') AS date_start_raw,
# MAGIC     TRY_TO_DATE(t.tournamentenddate,   'yyyy-MM-dd') AS date_end_raw,
# MAGIC
# MAGIC     t.tournamentinformation AS tournament_information,
# MAGIC
# MAGIC     CASE
# MAGIC       WHEN LOWER(TRIM(t.tournamentmajor)) IN ('y','yes','true','1') THEN TRUE
# MAGIC       ELSE FALSE
# MAGIC     END AS is_major_tournament,
# MAGIC
# MAGIC     TRY_CAST(t.naf_rulesetid  AS INT) AS ruleset_id,
# MAGIC     TRY_CAST(t.naf_variantsid AS INT) AS variant_id,
# MAGIC
# MAGIC     t.ingest_timestamp,
# MAGIC     t.ingest_source,
# MAGIC     t.file_name
# MAGIC   FROM naf_catalog.bronze.tournament_raw t
# MAGIC   WHERE TRY_CAST(t.naf_variantsid AS INT) IN (1, 13, 15)
# MAGIC     AND TRY_CAST(t.tournamentid AS INT) IS NOT NULL
# MAGIC ),
# MAGIC nation_lookup AS (
# MAGIC   -- allow matching on either raw nation_key or canonical nation_key_canonical
# MAGIC   SELECT nation_id, nation_key AS lookup_key, 0 AS precedence
# MAGIC   FROM naf_catalog.silver.nations_entity
# MAGIC   WHERE nation_key IS NOT NULL AND nation_key <> ''
# MAGIC
# MAGIC   UNION ALL
# MAGIC
# MAGIC   SELECT nation_id, nation_key_canonical AS lookup_key, 1 AS precedence
# MAGIC   FROM naf_catalog.silver.nations_entity
# MAGIC   WHERE nation_key_canonical IS NOT NULL AND nation_key_canonical <> ''
# MAGIC ),
# MAGIC resolved AS (
# MAGIC   SELECT
# MAGIC     b.*,
# MAGIC     nl.nation_id AS mapped_nation_id,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY b.tournament_id
# MAGIC       ORDER BY
# MAGIC         COALESCE(nl.precedence, 99) ASC,
# MAGIC         b.ingest_timestamp DESC,
# MAGIC         b.file_name DESC
# MAGIC     ) AS rn
# MAGIC   FROM base b
# MAGIC   LEFT JOIN nation_lookup nl
# MAGIC     ON b.nation_key = nl.lookup_key
# MAGIC ),
# MAGIC main AS (
# MAGIC   -- keep only tournaments that have games
# MAGIC   SELECT
# MAGIC     r.tournament_id,
# MAGIC     r.organizer_coach_id,
# MAGIC     r.tournament_name,
# MAGIC     r.tournament_nation,
# MAGIC     COALESCE(r.mapped_nation_id, 0) AS nation_id,
# MAGIC
# MAGIC     r.date_start_raw AS date_start,
# MAGIC     COALESCE(r.date_end_raw, r.date_start_raw) AS date_end,
# MAGIC
# MAGIC     CASE
# MAGIC       WHEN r.date_start_raw IS NULL THEN NULL
# MAGIC       ELSE CAST(date_format(r.date_start_raw, 'yyyyMMdd') AS INT)
# MAGIC     END AS start_date_id,
# MAGIC     CASE
# MAGIC       WHEN COALESCE(r.date_end_raw, r.date_start_raw) IS NULL THEN NULL
# MAGIC       ELSE CAST(date_format(COALESCE(r.date_end_raw, r.date_start_raw), 'yyyyMMdd') AS INT)
# MAGIC     END AS end_date_id,
# MAGIC
# MAGIC     r.tournament_information,
# MAGIC     COALESCE(r.is_major_tournament, FALSE) AS is_major_tournament,
# MAGIC     r.ruleset_id,
# MAGIC     r.variant_id,
# MAGIC
# MAGIC     r.ingest_timestamp,
# MAGIC     r.ingest_source,
# MAGIC     r.file_name,
# MAGIC
# MAGIC     current_timestamp() AS load_timestamp
# MAGIC   FROM resolved r
# MAGIC   INNER JOIN tournament_ids_in_games g
# MAGIC     ON r.tournament_id = g.tournament_id
# MAGIC   WHERE r.rn = 1
# MAGIC ),
# MAGIC missing AS (
# MAGIC   -- generate stubs for any tournament_id referenced by games_clean but missing from bronze export
# MAGIC   SELECT
# MAGIC     g.tournament_id AS tournament_id,
# MAGIC     CAST(NULL AS INT) AS organizer_coach_id,
# MAGIC     CONCAT('Unknown Tournament ', CAST(g.tournament_id AS STRING)) AS tournament_name,
# MAGIC     CAST(NULL AS STRING) AS tournament_nation,
# MAGIC     0 AS nation_id,
# MAGIC
# MAGIC     CAST(NULL AS DATE) AS date_start,
# MAGIC     CAST(NULL AS DATE) AS date_end,
# MAGIC     CAST(NULL AS INT)  AS start_date_id,
# MAGIC     CAST(NULL AS INT)  AS end_date_id,
# MAGIC
# MAGIC     CAST(NULL AS STRING) AS tournament_information,
# MAGIC     FALSE AS is_major_tournament,
# MAGIC     CAST(NULL AS INT) AS ruleset_id,
# MAGIC     MIN(gc.variant_id) AS variant_id,
# MAGIC
# MAGIC     CAST(NULL AS TIMESTAMP) AS ingest_timestamp,
# MAGIC     CAST('generated_from_games_clean' AS STRING) AS ingest_source,
# MAGIC     CAST(NULL AS STRING) AS file_name,
# MAGIC
# MAGIC     current_timestamp() AS load_timestamp
# MAGIC   FROM tournament_ids_in_games g
# MAGIC   LEFT JOIN main m
# MAGIC     ON g.tournament_id = m.tournament_id
# MAGIC   LEFT JOIN naf_catalog.silver.games_clean gc
# MAGIC     ON g.tournament_id = gc.tournament_id
# MAGIC   WHERE m.tournament_id IS NULL
# MAGIC   GROUP BY g.tournament_id
# MAGIC )
# MAGIC SELECT * FROM main
# MAGIC UNION ALL
# MAGIC SELECT * FROM missing;
# MAGIC

# COMMAND ----------

# DBTITLE 1,silver.tournament_statistics_group_clean
# MAGIC %sql -- TABLE: naf_catalog.silver.tournament_statistics_group_clean
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Canonical cleaned tournament statistics event rows (typed + dedup-ready).
# MAGIC -- LAYER        : SILVER
# MAGIC -- GRAIN        : 1 row per (tournament_id, coach_id, race_id, stat_id, date_event, notes)
# MAGIC -- PRIMARY KEY  : (tournament_id, coach_id, race_id, stat_id, date_event, notes)  -- logical (dedup key)
# MAGIC -- SOURCES      : naf_catalog.bronze.tournament_statistics_group_raw
# MAGIC -- NOTES        : date_event parsed in Silver; date_id (YYYYMMDD) derived deterministically from date_event.
# MAGIC --                notes kept as-is (nullable); include it in the logical key only if duplicates differ by notes.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.silver.tournament_statistics_group_clean
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH typed AS (
# MAGIC   SELECT
# MAGIC     TRY_CAST(b.typeID       AS INT) AS stat_id,
# MAGIC     TRY_CAST(b.tournamentID AS INT) AS tournament_id,
# MAGIC     TRY_CAST(b.coachID      AS INT) AS coach_id,
# MAGIC     TRY_CAST(b.raceID       AS INT) AS race_id,
# MAGIC
# MAGIC     NULLIF(TRIM(b.notes), '') AS notes,
# MAGIC
# MAGIC     TRY_TO_DATE(NULLIF(TRIM(b.date), ''), 'yyyy-MM-dd') AS date_event,
# MAGIC     CASE
# MAGIC       WHEN TRY_TO_DATE(NULLIF(TRIM(b.date), ''), 'yyyy-MM-dd') IS NULL THEN NULL
# MAGIC       ELSE CAST(date_format(TRY_TO_DATE(NULLIF(TRIM(b.date), ''), 'yyyy-MM-dd'), 'yyyyMMdd') AS INT)
# MAGIC     END AS date_id,
# MAGIC
# MAGIC     b.ingest_timestamp,
# MAGIC     b.ingest_source,
# MAGIC     b.file_name,
# MAGIC
# MAGIC     current_timestamp() AS load_timestamp
# MAGIC   FROM naf_catalog.bronze.tournament_statistics_group_raw b
# MAGIC   WHERE TRY_CAST(b.typeID AS INT) IS NOT NULL
# MAGIC ),
# MAGIC dedup AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY
# MAGIC         stat_id, tournament_id, coach_id, race_id,
# MAGIC         date_event,
# MAGIC         COALESCE(notes, '__NULL__')
# MAGIC       ORDER BY ingest_timestamp DESC, file_name DESC
# MAGIC     ) AS rn
# MAGIC   FROM typed
# MAGIC )
# MAGIC SELECT
# MAGIC   stat_id,
# MAGIC   tournament_id,
# MAGIC   coach_id,
# MAGIC   race_id,
# MAGIC   notes,
# MAGIC   date_event,
# MAGIC   date_id,
# MAGIC   ingest_timestamp,
# MAGIC   ingest_source,
# MAGIC   file_name,
# MAGIC   load_timestamp
# MAGIC FROM dedup
# MAGIC WHERE rn = 1;
# MAGIC

# COMMAND ----------

# DBTITLE 1,silver.tournament_statistics_list_clean
# MAGIC %sql -- TABLE: naf_catalog.silver.tournament_statistics_list_clean
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Canonical cleaned tournament statistic type lookup for joins.
# MAGIC -- LAYER        : SILVER
# MAGIC -- GRAIN        : 1 row per stat_id
# MAGIC -- PRIMARY KEY  : stat_id
# MAGIC -- SOURCES      : naf_catalog.bronze.tournament_statistics_list_raw
# MAGIC -- NOTES        : stat_name/stat_label are preserve-but-clean (trim + normalize whitespace).
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.silver.tournament_statistics_list_clean
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH typed AS (
# MAGIC   SELECT
# MAGIC     TRY_CAST(b.id AS INT) AS stat_id,
# MAGIC
# MAGIC     CASE
# MAGIC       WHEN b.name IS NULL OR TRIM(b.name) = '' THEN NULL
# MAGIC       ELSE REGEXP_REPLACE(TRIM(b.name), '\\s+', ' ')
# MAGIC     END AS stat_name,
# MAGIC
# MAGIC     CASE
# MAGIC       WHEN b.label IS NULL OR TRIM(b.label) = '' THEN NULL
# MAGIC       ELSE REGEXP_REPLACE(TRIM(b.label), '\\s+', ' ')
# MAGIC     END AS stat_label,
# MAGIC
# MAGIC     TRY_CAST(b.order_ AS INT) AS stat_sort_order,
# MAGIC
# MAGIC     -- Bronze lineage (standard)
# MAGIC     b.ingest_timestamp,
# MAGIC     b.ingest_source,
# MAGIC     b.file_name,
# MAGIC
# MAGIC     current_timestamp() AS load_timestamp
# MAGIC   FROM naf_catalog.bronze.tournament_statistics_list_raw b
# MAGIC   WHERE TRY_CAST(b.id AS INT) IS NOT NULL
# MAGIC ),
# MAGIC dedup AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY stat_id
# MAGIC       ORDER BY ingest_timestamp DESC, file_name DESC
# MAGIC     ) AS rn
# MAGIC   FROM typed
# MAGIC )
# MAGIC SELECT
# MAGIC   stat_id,
# MAGIC   stat_name,
# MAGIC   stat_label,
# MAGIC   stat_sort_order,
# MAGIC   ingest_timestamp,
# MAGIC   ingest_source,
# MAGIC   file_name,
# MAGIC   load_timestamp
# MAGIC FROM dedup
# MAGIC WHERE rn = 1;
# MAGIC

# COMMAND ----------

# DBTITLE 1,silver.tournament_coach_clean
# MAGIC %sql -- TABLE: naf_catalog.silver.tournament_coach_clean
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Canonical cleaned tournament participation rows (coach + race per tournament).
# MAGIC -- LAYER        : SILVER
# MAGIC -- GRAIN        : 1 row per (tournament_id, coach_id)   -- per NAF export semantics
# MAGIC -- PRIMARY KEY  : (tournament_id, coach_id)            -- logical (dedup key)
# MAGIC -- SOURCES      : naf_catalog.bronze.tournament_coach_raw
# MAGIC -- NOTES        : race_id reflects the coach's race for the tournament.
# MAGIC --                Dedup keeps latest ingested record per (tournament_id, coach_id).
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.silver.tournament_coach_clean
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH typed AS (
# MAGIC   SELECT
# MAGIC     TRY_CAST(b.naftournament AS INT) AS tournament_id,
# MAGIC     TRY_CAST(b.nafcoach      AS INT) AS coach_id,
# MAGIC     TRY_CAST(b.race          AS INT) AS race_id,
# MAGIC
# MAGIC     -- Bronze lineage (standard)
# MAGIC     b.ingest_timestamp,
# MAGIC     b.ingest_source,
# MAGIC     b.file_name,
# MAGIC
# MAGIC     current_timestamp() AS load_timestamp
# MAGIC   FROM naf_catalog.bronze.tournament_coach_raw b
# MAGIC   WHERE TRY_CAST(b.naftournament AS INT) IS NOT NULL
# MAGIC     AND TRY_CAST(b.nafcoach AS INT) IS NOT NULL
# MAGIC     AND TRY_CAST(b.nafcoach AS INT) <> 9
# MAGIC ),
# MAGIC dedup AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY tournament_id, coach_id
# MAGIC       ORDER BY ingest_timestamp DESC, file_name DESC
# MAGIC     ) AS rn
# MAGIC   FROM typed
# MAGIC )
# MAGIC SELECT
# MAGIC   tournament_id,
# MAGIC   coach_id,
# MAGIC   race_id,
# MAGIC   ingest_timestamp,
# MAGIC   ingest_source,
# MAGIC   file_name,
# MAGIC   load_timestamp
# MAGIC FROM dedup
# MAGIC WHERE rn = 1;
# MAGIC

# COMMAND ----------

# DBTITLE 1,silver.variants_clean
# MAGIC %sql -- TABLE: naf_catalog.silver.variants_clean
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Canonical cleaned variant lookup (supported variants only).
# MAGIC -- LAYER        : SILVER
# MAGIC -- GRAIN        : 1 row per variant_id
# MAGIC -- PRIMARY KEY  : variant_id
# MAGIC -- SOURCES      : naf_catalog.bronze.variants_raw
# MAGIC -- NOTES        : Filtered to supported variants (1, 13, 15) per pipeline scope.
# MAGIC --                variant_name is preserve-but-clean (trim + normalize whitespace).
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.silver.variants_clean
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH typed AS (
# MAGIC   SELECT
# MAGIC     TRY_CAST(b.variantid AS INT) AS variant_id,
# MAGIC
# MAGIC     CASE
# MAGIC       WHEN b.variantname IS NULL OR TRIM(b.variantname) = '' THEN NULL
# MAGIC       ELSE REGEXP_REPLACE(TRIM(b.variantname), '\\s+', ' ')
# MAGIC     END AS variant_name,
# MAGIC
# MAGIC     TRY_CAST(b.order_ AS INT) AS variant_sort_order,
# MAGIC
# MAGIC     -- Bronze lineage (standard)
# MAGIC     b.ingest_timestamp,
# MAGIC     b.ingest_source,
# MAGIC     b.file_name,
# MAGIC
# MAGIC     current_timestamp() AS load_timestamp
# MAGIC   FROM naf_catalog.bronze.variants_raw b
# MAGIC   WHERE TRY_CAST(b.variantid AS INT) IS NOT NULL
# MAGIC     AND TRY_CAST(b.variantid AS INT) IN (1, 13, 15)
# MAGIC ),
# MAGIC dedup AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY variant_id
# MAGIC       ORDER BY ingest_timestamp DESC, file_name DESC
# MAGIC     ) AS rn
# MAGIC   FROM typed
# MAGIC )
# MAGIC SELECT
# MAGIC   variant_id,
# MAGIC   variant_name,
# MAGIC   variant_sort_order,
# MAGIC   ingest_timestamp,
# MAGIC   ingest_source,
# MAGIC   file_name,
# MAGIC   load_timestamp
# MAGIC FROM dedup
# MAGIC WHERE rn = 1;

# COMMAND ----------

# DBTITLE 1,Silver validation checks
# MAGIC %sql
# MAGIC -- =====================================================================
# MAGIC -- SILVER VALIDATION (layer-local)
# MAGIC -- Expectation: all “bad_*” counts = 0 and “unknown_rows” = 1
# MAGIC -- =====================================================================
# MAGIC
# MAGIC -- -------------------------
# MAGIC -- Uniqueness: source IDs
# MAGIC -- -------------------------
# MAGIC SELECT 'silver.coaches_clean' AS table_name,
# MAGIC        COUNT(*) AS rows,
# MAGIC        COUNT(DISTINCT coach_id) AS distinct_ids,
# MAGIC        (COUNT(*) - COUNT(DISTINCT coach_id)) AS bad_duplicates
# MAGIC FROM naf_catalog.silver.coaches_clean;
# MAGIC
# MAGIC SELECT 'silver.games_clean' AS table_name,
# MAGIC        COUNT(*) AS rows,
# MAGIC        COUNT(DISTINCT game_id) AS distinct_ids,
# MAGIC        (COUNT(*) - COUNT(DISTINCT game_id)) AS bad_duplicates
# MAGIC FROM naf_catalog.silver.games_clean;
# MAGIC
# MAGIC SELECT 'silver.tournaments_clean' AS table_name,
# MAGIC        COUNT(*) AS rows,
# MAGIC        COUNT(DISTINCT tournament_id) AS distinct_ids,
# MAGIC        (COUNT(*) - COUNT(DISTINCT tournament_id)) AS bad_duplicates
# MAGIC FROM naf_catalog.silver.tournaments_clean;
# MAGIC
# MAGIC SELECT 'silver.races_clean' AS table_name,
# MAGIC        COUNT(*) AS rows,
# MAGIC        COUNT(DISTINCT race_id) AS distinct_ids,
# MAGIC        (COUNT(*) - COUNT(DISTINCT race_id)) AS bad_duplicates
# MAGIC FROM naf_catalog.silver.races_clean;
# MAGIC
# MAGIC SELECT 'silver.variants_clean' AS table_name,
# MAGIC        COUNT(*) AS rows,
# MAGIC        COUNT(DISTINCT variant_id) AS distinct_ids,
# MAGIC        (COUNT(*) - COUNT(DISTINCT variant_id)) AS bad_duplicates
# MAGIC FROM naf_catalog.silver.variants_clean;
# MAGIC
# MAGIC SELECT 'silver.tournament_statistics_list_clean' AS table_name,
# MAGIC        COUNT(*) AS rows,
# MAGIC        COUNT(DISTINCT stat_id) AS distinct_ids,
# MAGIC        (COUNT(*) - COUNT(DISTINCT stat_id)) AS bad_duplicates
# MAGIC FROM naf_catalog.silver.tournament_statistics_list_clean;
# MAGIC
# MAGIC -- tournament_coach_clean logical PK: (tournament_id, coach_id)
# MAGIC SELECT 'silver.tournament_coach_clean' AS table_name,
# MAGIC        COUNT(*) AS rows,
# MAGIC        COUNT(DISTINCT CONCAT_WS('::', CAST(tournament_id AS STRING), CAST(coach_id AS STRING))) AS distinct_ids,
# MAGIC        (COUNT(*) - COUNT(DISTINCT CONCAT_WS('::', CAST(tournament_id AS STRING), CAST(coach_id AS STRING)))) AS bad_duplicates
# MAGIC FROM naf_catalog.silver.tournament_coach_clean;
# MAGIC
# MAGIC -- -------------------------
# MAGIC -- Not-null key checks
# MAGIC -- -------------------------
# MAGIC SELECT 'silver.coaches_clean' AS table_name,
# MAGIC        SUM(CASE WHEN coach_id  IS NULL THEN 1 ELSE 0 END) AS bad_null_coach_id,
# MAGIC        SUM(CASE WHEN nation_id IS NULL THEN 1 ELSE 0 END) AS bad_null_nation_id
# MAGIC FROM naf_catalog.silver.coaches_clean;
# MAGIC
# MAGIC SELECT 'silver.games_clean' AS table_name,
# MAGIC        SUM(CASE WHEN game_id       IS NULL THEN 1 ELSE 0 END) AS bad_null_game_id,
# MAGIC        SUM(CASE WHEN home_coach_id IS NULL THEN 1 ELSE 0 END) AS bad_null_home_coach_id,
# MAGIC        SUM(CASE WHEN away_coach_id IS NULL THEN 1 ELSE 0 END) AS bad_null_away_coach_id,
# MAGIC        SUM(CASE WHEN home_race_id  IS NULL THEN 1 ELSE 0 END) AS bad_null_home_race_id,
# MAGIC        SUM(CASE WHEN away_race_id  IS NULL THEN 1 ELSE 0 END) AS bad_null_away_race_id
# MAGIC FROM naf_catalog.silver.games_clean;
# MAGIC
# MAGIC SELECT 'silver.tournaments_clean' AS table_name,
# MAGIC        SUM(CASE WHEN tournament_id IS NULL THEN 1 ELSE 0 END) AS bad_null_tournament_id,
# MAGIC        SUM(CASE WHEN nation_id     IS NULL THEN 1 ELSE 0 END) AS bad_null_nation_id
# MAGIC FROM naf_catalog.silver.tournaments_clean;
# MAGIC
# MAGIC -- -------------------------
# MAGIC -- date_id integrity
# MAGIC -- -------------------------
# MAGIC SELECT 'silver.games_clean date_id' AS check_name,
# MAGIC        COUNT(*) AS bad_rows
# MAGIC FROM naf_catalog.silver.games_clean
# MAGIC WHERE game_date IS NOT NULL
# MAGIC   AND date_id <> CAST(date_format(game_date, 'yyyyMMdd') AS INT);
# MAGIC
# MAGIC SELECT 'silver.tournaments_clean date_id' AS check_name,
# MAGIC        COUNT(*) AS bad_rows
# MAGIC FROM naf_catalog.silver.tournaments_clean
# MAGIC WHERE date_start IS NOT NULL
# MAGIC   AND start_date_id <> CAST(date_format(date_start, 'yyyyMMdd') AS INT);
# MAGIC
# MAGIC SELECT 'silver.tournaments_clean date_end_id' AS check_name,
# MAGIC        COUNT(*) AS bad_rows
# MAGIC FROM naf_catalog.silver.tournaments_clean
# MAGIC WHERE date_end IS NOT NULL
# MAGIC   AND end_date_id <> CAST(date_format(date_end, 'yyyyMMdd') AS INT);
# MAGIC
# MAGIC SELECT 'silver.tournament_statistics_group_clean date_id' AS check_name,
# MAGIC        COUNT(*) AS bad_rows
# MAGIC FROM naf_catalog.silver.tournament_statistics_group_clean
# MAGIC WHERE date_event IS NOT NULL
# MAGIC   AND date_id <> CAST(date_format(date_event, 'yyyyMMdd') AS INT);
# MAGIC
# MAGIC -- -------------------------
# MAGIC -- Nation surrogate key safety
# MAGIC -- -------------------------
# MAGIC SELECT 'silver.nations_entity nation_id unique' AS check_name,
# MAGIC        COUNT(*) AS rows,
# MAGIC        COUNT(DISTINCT nation_id) AS distinct_ids,
# MAGIC        (COUNT(*) - COUNT(DISTINCT nation_id)) AS bad_duplicates
# MAGIC FROM naf_catalog.silver.nations_entity;
# MAGIC
# MAGIC SELECT 'silver.nations_entity UNKNOWN count' AS check_name,
# MAGIC        COUNT(*) AS unknown_rows
# MAGIC FROM naf_catalog.silver.nations_entity
# MAGIC WHERE nation_key_canonical = 'UNKNOWN';
# MAGIC
# MAGIC -- Recompute group_key the same way and confirm no collisions in the int-range hash
# MAGIC WITH g AS (
# MAGIC   SELECT
# MAGIC     CASE
# MAGIC       WHEN iso2_code IS NULL AND iso3_code IS NULL AND fifa_code IS NULL AND ioc_code IS NULL
# MAGIC         AND COALESCE(nation_key_canonical, nation_key) NOT IN ('ENGLAND','SCOTLAND','WALES','NORTHERNIRELAND')
# MAGIC       THEN 'UNKNOWN'
# MAGIC       ELSE COALESCE(nation_key_canonical, nation_key)
# MAGIC     END AS group_key
# MAGIC   FROM naf_catalog.silver.nations_entity
# MAGIC )
# MAGIC SELECT 'silver.nations_entity group_key hash collisions' AS check_name,
# MAGIC        COUNT(DISTINCT group_key) AS distinct_keys,
# MAGIC        COUNT(DISTINCT CAST(PMOD(XXHASH64(group_key), 2147483647) AS INT)) AS distinct_ids,
# MAGIC        (COUNT(DISTINCT group_key) - COUNT(DISTINCT CAST(PMOD(XXHASH64(group_key), 2147483647) AS INT))) AS bad_collisions
# MAGIC FROM g;
# MAGIC
# MAGIC -- -------------------------
# MAGIC -- Codes uppercase (selected tables)
# MAGIC -- -------------------------
# MAGIC SELECT 'silver.country_reference codes uppercase' AS check_name,
# MAGIC        COUNT(*) AS bad_rows
# MAGIC FROM naf_catalog.silver.country_reference
# MAGIC WHERE (fifa_code IS NOT NULL AND fifa_code <> UPPER(fifa_code))
# MAGIC    OR (ioc_code  IS NOT NULL AND ioc_code  <> UPPER(ioc_code))
# MAGIC    OR (iso2_code IS NOT NULL AND iso2_code <> UPPER(iso2_code))
# MAGIC    OR (iso3_code IS NOT NULL AND iso3_code <> UPPER(iso3_code));
# MAGIC
