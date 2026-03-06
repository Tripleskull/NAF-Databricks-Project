# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Summary – Nation
# MAGIC
# MAGIC This notebook builds the **nation-facing summary tables** in `naf_catalog.gold_summary`.
# MAGIC
# MAGIC ## Purpose
# MAGIC `gold_summary` contains **aggregations / KPIs / rollups** used by dashboards and exports.  
# MAGIC It must stay **ID-based and analytics-ready**, while all **display fields** (names, flags, labels) belong in `gold_presentation`.
# MAGIC
# MAGIC ## Dependencies (inputs)
# MAGIC - `naf_catalog.gold_fact.coach_games_fact` (coach-participation view of games)
# MAGIC - `naf_catalog.gold_fact.games_fact` (one row per game)
# MAGIC - `naf_catalog.gold_fact.rating_history_fact` (ELO event history; GLOBAL = “glo”)
# MAGIC - `naf_catalog.gold_dim.coach_dim` (coach → nation_id)
# MAGIC - `naf_catalog.gold_dim.tournament_dim` (tournament → host nation_id)
# MAGIC
# MAGIC ## Output tables (this notebook)
# MAGIC Nation activity / volume:
# MAGIC - `gold_summary.nation_games_timeseries`
# MAGIC - `gold_summary.nation_coach_activity_timeseries`
# MAGIC - `gold_summary.nation_overview_summary`
# MAGIC
# MAGIC Nation breakdowns:
# MAGIC - `gold_summary.nation_race_summary`
# MAGIC - `gold_summary.nation_vs_nation_summary`
# MAGIC
# MAGIC Ratings (nation-scoped, still ID-based):
# MAGIC - `gold_summary.nation_coach_glo_metrics` (coach-level metrics by nation)
# MAGIC
# MAGIC ## Notebook conventions
# MAGIC - **One object per cell** (one `CREATE OR REPLACE TABLE ...` per cell).
# MAGIC - First line must include the object name for collapsible navigation:
# MAGIC   `%sql -- TABLE: naf_catalog.gold_summary.<table_name>`
# MAGIC - `gold_summary` tables: **no descriptive fields** (`nation_name*`, `flag_code`, etc).
# MAGIC   Join those in `gold_presentation`.
# MAGIC - “glo” = **GLOBAL NAF_ELO** (race-agnostic, `COALESCE(race_id,0)=0`).
# MAGIC   `is_valid_glo` is a **reporting/stability flag**, not “does the rating exist”.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TABLE: naf_catalog.gold_summary.nation_coach_glo_metrics
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE:
# MAGIC --   - One row per (nation_id, coach_id)
# MAGIC --   - GLOBAL (race-agnostic) NAF_ELO metrics from rating_history_fact
# MAGIC -- NOTES:
# MAGIC --   - is_valid_glo indicates stable sample (games_played >= min_games)
# MAGIC --   - glo_* metrics are computed from the coach's >= 50th GLOBAL game onward (stable sample only)
# MAGIC --   - glo_*_all metrics are computed from all GLOBAL games (optional but useful)
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_summary.nation_coach_glo_metrics
# MAGIC USING DELTA AS
# MAGIC
# MAGIC WITH params AS (
# MAGIC   SELECT min_games_nation_glo AS min_games
# MAGIC   FROM naf_catalog.gold_dim.analytical_config
# MAGIC ),
# MAGIC
# MAGIC rh_dedup AS (
# MAGIC   SELECT
# MAGIC     coach_id,
# MAGIC     game_id,
# MAGIC     game_index,
# MAGIC     event_timestamp,
# MAGIC     rating_after
# MAGIC   FROM naf_catalog.gold_fact.rating_history_fact
# MAGIC   WHERE scope = 'GLOBAL'
# MAGIC     AND rating_system = 'NAF_ELO'
# MAGIC     AND COALESCE(race_id, 0) = 0
# MAGIC     AND coach_id IS NOT NULL
# MAGIC   QUALIFY ROW_NUMBER() OVER (
# MAGIC     PARTITION BY coach_id, game_id
# MAGIC     ORDER BY event_timestamp DESC, game_index DESC, game_id DESC
# MAGIC   ) = 1
# MAGIC ),
# MAGIC
# MAGIC hist AS (
# MAGIC   SELECT
# MAGIC     cd.nation_id,
# MAGIC     r.coach_id,
# MAGIC     r.game_id,
# MAGIC     r.game_index,
# MAGIC     r.event_timestamp,
# MAGIC     r.rating_after
# MAGIC   FROM rh_dedup r
# MAGIC   INNER JOIN naf_catalog.gold_dim.coach_dim cd
# MAGIC     ON r.coach_id = cd.coach_id
# MAGIC   WHERE cd.nation_id IS NOT NULL
# MAGIC ),
# MAGIC
# MAGIC numbered AS (
# MAGIC   SELECT
# MAGIC     h.*,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY coach_id
# MAGIC       ORDER BY game_index ASC, game_id ASC
# MAGIC     ) AS coach_game_number,
# MAGIC     COUNT(*) OVER (PARTITION BY coach_id) AS games_played
# MAGIC   FROM hist h
# MAGIC ),
# MAGIC
# MAGIC all_agg AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     coach_id,
# MAGIC     MAX(rating_after)                    AS glo_peak_all,
# MAGIC     AVG(rating_after)                    AS glo_mean_all,
# MAGIC     PERCENTILE_APPROX(rating_after, 0.5) AS glo_median_all,
# MAGIC     MAX(games_played)                    AS games_played
# MAGIC   FROM numbered
# MAGIC   GROUP BY nation_id, coach_id
# MAGIC ),
# MAGIC
# MAGIC eligible AS (
# MAGIC   SELECT
# MAGIC     n.nation_id,
# MAGIC     n.coach_id,
# MAGIC     n.rating_after
# MAGIC   FROM numbered n
# MAGIC   CROSS JOIN params p
# MAGIC   WHERE n.games_played >= p.min_games
# MAGIC     AND n.coach_game_number >= p.min_games
# MAGIC ),
# MAGIC
# MAGIC eligible_agg AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     coach_id,
# MAGIC     MAX(rating_after)                    AS glo_peak,
# MAGIC     AVG(rating_after)                    AS glo_mean,
# MAGIC     PERCENTILE_APPROX(rating_after, 0.5) AS glo_median
# MAGIC   FROM eligible
# MAGIC   GROUP BY nation_id, coach_id
# MAGIC ),
# MAGIC
# MAGIC -- Opponent strength per coach (from coach_performance_summary, built in 331).
# MAGIC opponent_strength AS (
# MAGIC   SELECT coach_id, avg_opponent_glo_peak
# MAGIC   FROM naf_catalog.gold_summary.coach_performance_summary
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   a.nation_id,
# MAGIC   a.coach_id,
# MAGIC
# MAGIC   -- Stable-sample metrics (NULL unless valid)
# MAGIC   e.glo_peak,
# MAGIC   e.glo_mean,
# MAGIC   e.glo_median,
# MAGIC
# MAGIC   -- Optional: full-history metrics
# MAGIC   a.glo_peak_all,
# MAGIC   a.glo_mean_all,
# MAGIC   a.glo_median_all,
# MAGIC
# MAGIC   -- Opponent strength context
# MAGIC   os.avg_opponent_glo_peak,
# MAGIC
# MAGIC   a.games_played,
# MAGIC   (a.games_played >= p.min_games) AS is_valid_glo,
# MAGIC
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM all_agg a
# MAGIC CROSS JOIN params p
# MAGIC LEFT JOIN eligible_agg e
# MAGIC   ON a.nation_id = e.nation_id
# MAGIC  AND a.coach_id  = e.coach_id
# MAGIC LEFT JOIN opponent_strength os
# MAGIC   ON a.coach_id = os.coach_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql -- TABLE: naf_catalog.gold_summary.nation_overview_summary
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE:
# MAGIC --   - One row per nation_id (participation nationality + host nationality + rating rollups)
# MAGIC -- GRAIN:
# MAGIC --   - One row per nation_id
# MAGIC -- SOURCES:
# MAGIC --   - naf_catalog.gold_fact.coach_games_fact (coach nationality participation)
# MAGIC --   - naf_catalog.gold_fact.games_fact + naf_catalog.gold_dim.tournament_dim (host nation)
# MAGIC --   - naf_catalog.gold_summary.nation_coach_glo_metrics (GLOBAL ELO distribution spine)
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_summary.nation_overview_summary
# MAGIC USING DELTA AS
# MAGIC
# MAGIC WITH coach_base AS (
# MAGIC   SELECT
# MAGIC     cg.game_id,
# MAGIC     cg.tournament_id,
# MAGIC     cg.coach_id,
# MAGIC     cg.result_numeric,
# MAGIC     cd.nation_id
# MAGIC   FROM naf_catalog.gold_fact.coach_games_fact cg
# MAGIC   INNER JOIN naf_catalog.gold_dim.coach_dim cd
# MAGIC     ON cg.coach_id = cd.coach_id
# MAGIC   WHERE cd.nation_id IS NOT NULL
# MAGIC     AND cg.game_id IS NOT NULL
# MAGIC     AND cg.coach_id IS NOT NULL
# MAGIC     AND cg.result_numeric IN (0.0, 0.5, 1.0)
# MAGIC   QUALIFY ROW_NUMBER() OVER (
# MAGIC     PARTITION BY cg.game_id, cg.coach_id
# MAGIC     ORDER BY cg.event_timestamp DESC NULLS LAST,
# MAGIC              cg.game_date       DESC NULLS LAST,
# MAGIC              cg.tournament_id   DESC NULLS LAST
# MAGIC   ) = 1
# MAGIC ),
# MAGIC
# MAGIC coach_nation_agg AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     CAST(COUNT(DISTINCT coach_id)      AS INT) AS coaches_count,
# MAGIC     CAST(COUNT(*)                      AS INT) AS coach_participations_count,
# MAGIC     CAST(COUNT(DISTINCT game_id)       AS INT) AS game_representations_count,
# MAGIC     CAST(COUNT(DISTINCT tournament_id) AS INT) AS tournaments_attended_count,
# MAGIC     AVG(result_numeric)                        AS avg_points_per_game
# MAGIC   FROM coach_base
# MAGIC   GROUP BY nation_id
# MAGIC ),
# MAGIC
# MAGIC coach_global_totals AS (
# MAGIC   SELECT
# MAGIC     CAST(COUNT(DISTINCT coach_id)      AS INT) AS coaches_count_global,
# MAGIC     CAST(COUNT(*)                      AS INT) AS coach_participations_count_global,
# MAGIC     CAST(COUNT(DISTINCT game_id)       AS INT) AS game_representations_count_global,
# MAGIC     CAST(COUNT(DISTINCT tournament_id) AS INT) AS tournaments_attended_count_global
# MAGIC   FROM coach_base
# MAGIC ),
# MAGIC
# MAGIC host_base AS (
# MAGIC   SELECT DISTINCT
# MAGIC     g.game_id,
# MAGIC     g.tournament_id,
# MAGIC     td.nation_id
# MAGIC   FROM naf_catalog.gold_fact.games_fact g
# MAGIC   INNER JOIN naf_catalog.gold_dim.tournament_dim td
# MAGIC     ON g.tournament_id = td.tournament_id
# MAGIC   WHERE td.nation_id IS NOT NULL
# MAGIC     AND g.game_id IS NOT NULL
# MAGIC     AND g.tournament_id IS NOT NULL
# MAGIC ),
# MAGIC
# MAGIC host_nation_agg AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     CAST(COUNT(DISTINCT game_id)       AS INT) AS games_hosted_count,
# MAGIC     CAST(COUNT(DISTINCT tournament_id) AS INT) AS tournaments_hosted_count
# MAGIC   FROM host_base
# MAGIC   GROUP BY nation_id
# MAGIC ),
# MAGIC
# MAGIC host_global_totals AS (
# MAGIC   SELECT
# MAGIC     CAST(COUNT(DISTINCT game_id)       AS INT) AS games_hosted_count_global,
# MAGIC     CAST(COUNT(DISTINCT tournament_id) AS INT) AS tournaments_hosted_count_global
# MAGIC   FROM host_base
# MAGIC ),
# MAGIC
# MAGIC glo_peak_by_nation AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     MAX(glo_peak)                    AS glo_peak_max,
# MAGIC     AVG(glo_peak)                    AS glo_peak_mean,
# MAGIC     PERCENTILE_APPROX(glo_peak, 0.5) AS glo_peak_median
# MAGIC   FROM naf_catalog.gold_summary.nation_coach_glo_metrics
# MAGIC   WHERE is_valid_glo = TRUE
# MAGIC     AND glo_peak IS NOT NULL
# MAGIC   GROUP BY nation_id
# MAGIC ),
# MAGIC
# MAGIC all_nations AS (
# MAGIC   -- include all nations for stable presentation contracts (0s where no activity)
# MAGIC   SELECT nation_id FROM naf_catalog.gold_dim.nation_dim
# MAGIC ),
# MAGIC
# MAGIC -- Opponent strength aggregated to nation level (valid coaches only).
# MAGIC opp_strength_by_nation AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     AVG(avg_opponent_glo_peak) AS avg_opponent_glo_peak
# MAGIC   FROM naf_catalog.gold_summary.nation_coach_glo_metrics
# MAGIC   WHERE is_valid_glo = TRUE
# MAGIC     AND avg_opponent_glo_peak IS NOT NULL
# MAGIC   GROUP BY nation_id
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   n.nation_id,
# MAGIC
# MAGIC   COALESCE(cna.coaches_count, 0)              AS coaches_count,
# MAGIC   COALESCE(cna.coach_participations_count, 0) AS coach_participations_count,
# MAGIC   COALESCE(cna.game_representations_count, 0) AS game_representations_count,
# MAGIC   COALESCE(cna.tournaments_attended_count, 0) AS tournaments_attended_count,
# MAGIC   cna.avg_points_per_game                     AS avg_points_per_game,
# MAGIC
# MAGIC   100.0 * COALESCE(cna.coaches_count, 0)
# MAGIC     / NULLIF(cgt.coaches_count_global, 0)     AS coaches_global_pct,
# MAGIC
# MAGIC   100.0 * COALESCE(cna.coach_participations_count, 0)
# MAGIC     / NULLIF(cgt.coach_participations_count_global, 0) AS coach_participations_global_pct,
# MAGIC
# MAGIC   100.0 * COALESCE(cna.game_representations_count, 0)
# MAGIC     / NULLIF(cgt.game_representations_count_global, 0) AS game_representations_global_pct,
# MAGIC
# MAGIC   100.0 * COALESCE(cna.tournaments_attended_count, 0)
# MAGIC     / NULLIF(cgt.tournaments_attended_count_global, 0) AS tournaments_attended_global_pct,
# MAGIC
# MAGIC   COALESCE(hna.games_hosted_count, 0)       AS games_hosted_count,
# MAGIC   COALESCE(hna.tournaments_hosted_count, 0) AS tournaments_hosted_count,
# MAGIC
# MAGIC   100.0 * COALESCE(hna.games_hosted_count, 0)
# MAGIC     / NULLIF(hgt.games_hosted_count_global, 0) AS games_hosted_global_pct,
# MAGIC
# MAGIC   100.0 * COALESCE(hna.tournaments_hosted_count, 0)
# MAGIC     / NULLIF(hgt.tournaments_hosted_count_global, 0) AS tournaments_hosted_global_pct,
# MAGIC
# MAGIC   gpn.glo_peak_max,
# MAGIC   gpn.glo_peak_mean,
# MAGIC   gpn.glo_peak_median,
# MAGIC
# MAGIC   osn.avg_opponent_glo_peak,
# MAGIC
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM all_nations n
# MAGIC LEFT JOIN coach_nation_agg cna ON n.nation_id = cna.nation_id
# MAGIC CROSS JOIN coach_global_totals cgt
# MAGIC LEFT JOIN host_nation_agg hna ON n.nation_id = hna.nation_id
# MAGIC CROSS JOIN host_global_totals hgt
# MAGIC LEFT JOIN glo_peak_by_nation gpn ON n.nation_id = gpn.nation_id
# MAGIC LEFT JOIN opp_strength_by_nation osn ON n.nation_id = osn.nation_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TABLE: naf_catalog.gold_summary.nation_games_timeseries
# MAGIC -- =============================================================================
# MAGIC -- PURPOSE:
# MAGIC --   - Nation "host" activity over time (via tournament host nation)
# MAGIC -- GRAIN:
# MAGIC --   - One row per (nation_id, game_date)
# MAGIC -- =============================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_summary.nation_games_timeseries
# MAGIC USING DELTA AS
# MAGIC
# MAGIC WITH base AS (
# MAGIC   SELECT DISTINCT
# MAGIC     g.game_id,
# MAGIC     g.tournament_id,
# MAGIC     g.game_date,
# MAGIC     td.nation_id
# MAGIC   FROM naf_catalog.gold_fact.games_fact g
# MAGIC   INNER JOIN naf_catalog.gold_dim.tournament_dim td
# MAGIC     ON g.tournament_id = td.tournament_id
# MAGIC   WHERE td.nation_id IS NOT NULL
# MAGIC     AND g.game_id IS NOT NULL
# MAGIC     AND g.game_date IS NOT NULL
# MAGIC ),
# MAGIC
# MAGIC agg AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     game_date,
# MAGIC     CAST(COUNT(DISTINCT game_id) AS INT)       AS games_count,
# MAGIC     CAST(COUNT(DISTINCT tournament_id) AS INT) AS tournaments_count
# MAGIC   FROM base
# MAGIC   GROUP BY nation_id, game_date
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   nation_id,
# MAGIC   game_date,
# MAGIC   games_count,
# MAGIC   tournaments_count,
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM agg;
# MAGIC

# COMMAND ----------

# MAGIC %sql -- TABLE: naf_catalog.gold_summary.nation_coach_activity_timeseries
# MAGIC -- =============================================================================
# MAGIC -- PURPOSE:
# MAGIC --   - Nation participation activity over time (via coach nationality)
# MAGIC -- GRAIN:
# MAGIC --   - One row per (nation_id, game_date)
# MAGIC -- MEASURES:
# MAGIC --   - coach_participations_count  = distinct (coach_id, game_id)
# MAGIC --   - game_representations_count  = distinct game_id
# MAGIC --   - coaches_active_count        = distinct coach_id
# MAGIC --   - tournaments_represented_count = distinct tournament_id (optional but useful)
# MAGIC -- SOURCES:
# MAGIC --   - naf_catalog.gold_fact.coach_games_fact
# MAGIC --   - naf_catalog.gold_dim.coach_dim (coach_id -> nation_id)
# MAGIC -- =============================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_summary.nation_coach_activity_timeseries
# MAGIC USING DELTA AS
# MAGIC
# MAGIC WITH base AS (
# MAGIC   SELECT
# MAGIC     cd.nation_id,
# MAGIC     cg.game_date,
# MAGIC     cg.game_id,
# MAGIC     cg.coach_id,
# MAGIC     cg.tournament_id
# MAGIC   FROM naf_catalog.gold_fact.coach_games_fact cg
# MAGIC   INNER JOIN naf_catalog.gold_dim.coach_dim cd
# MAGIC     ON cg.coach_id = cd.coach_id
# MAGIC   WHERE cd.nation_id IS NOT NULL
# MAGIC     AND cg.game_date IS NOT NULL
# MAGIC     AND cg.game_id   IS NOT NULL
# MAGIC     AND cg.coach_id  IS NOT NULL
# MAGIC   QUALIFY ROW_NUMBER() OVER (
# MAGIC     PARTITION BY cg.game_id, cg.coach_id
# MAGIC     ORDER BY
# MAGIC       cg.event_timestamp DESC NULLS LAST,
# MAGIC       cg.game_date       DESC NULLS LAST,
# MAGIC       cg.tournament_id   DESC NULLS LAST
# MAGIC   ) = 1
# MAGIC ),
# MAGIC
# MAGIC agg AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     game_date,
# MAGIC     CAST(COUNT(*) AS INT)                  AS coach_participations_count,
# MAGIC     CAST(COUNT(DISTINCT game_id) AS INT)   AS game_representations_count,
# MAGIC     CAST(COUNT(DISTINCT coach_id) AS INT)  AS coaches_active_count,
# MAGIC     CAST(COUNT(DISTINCT tournament_id) AS INT) AS tournaments_represented_count
# MAGIC   FROM base
# MAGIC   GROUP BY nation_id, game_date
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   nation_id,
# MAGIC   game_date,
# MAGIC   coach_participations_count,
# MAGIC   game_representations_count,
# MAGIC   coaches_active_count,
# MAGIC   tournaments_represented_count,
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM agg;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TABLE: naf_catalog.gold_summary.nation_members_cumulative_weekly
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Weekly cumulative count of distinct NAF-registered coaches
# MAGIC --                per nation + World aggregate (nation_id = 0).
# MAGIC --                Uses each coach's earliest game date as a proxy for
# MAGIC --                "member since" (coach_dim has no registration date).
# MAGIC -- LAYER        : GOLD_SUMMARY
# MAGIC -- CONTRACT TYPE: SERIES (weekly cumulative)
# MAGIC -- GRAIN        : 1 row per (nation_id, iso_week)
# MAGIC -- PRIMARY KEY  : (nation_id, iso_week)
# MAGIC -- SOURCES      : naf_catalog.gold_fact.coach_games_fact,
# MAGIC --                naf_catalog.gold_dim.coach_dim
# MAGIC -- FEEDS        : W2 — NAF Members Over Time (Nation Dashboard)
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_summary.nation_members_cumulative_weekly
# MAGIC USING DELTA AS
# MAGIC
# MAGIC WITH first_game AS (
# MAGIC   -- Earliest game date per coach (proxy for NAF registration)
# MAGIC   SELECT
# MAGIC     cd.nation_id,
# MAGIC     cg.coach_id,
# MAGIC     MIN(cg.game_date)                                        AS first_game_date,
# MAGIC     DATE_TRUNC('WEEK', MIN(cg.game_date))                    AS first_week
# MAGIC   FROM naf_catalog.gold_fact.coach_games_fact AS cg
# MAGIC   INNER JOIN naf_catalog.gold_dim.coach_dim   AS cd
# MAGIC     ON cg.coach_id = cd.coach_id
# MAGIC   WHERE cd.nation_id IS NOT NULL
# MAGIC     AND cg.game_date IS NOT NULL
# MAGIC   GROUP BY cd.nation_id, cg.coach_id
# MAGIC ),
# MAGIC
# MAGIC -- Count of new coaches joining each week per nation
# MAGIC nation_weekly_new AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     first_week                                                AS iso_week,
# MAGIC     CAST(COUNT(*) AS INT)                                     AS new_coaches
# MAGIC   FROM first_game
# MAGIC   GROUP BY nation_id, first_week
# MAGIC ),
# MAGIC
# MAGIC -- World aggregate: count new coaches per week across all nations
# MAGIC world_weekly_new AS (
# MAGIC   SELECT
# MAGIC     0                                                         AS nation_id,
# MAGIC     first_week                                                AS iso_week,
# MAGIC     CAST(COUNT(*) AS INT)                                     AS new_coaches
# MAGIC   FROM first_game
# MAGIC   GROUP BY first_week
# MAGIC ),
# MAGIC
# MAGIC -- Union nation rows + World rows
# MAGIC combined AS (
# MAGIC   SELECT nation_id, iso_week, new_coaches FROM nation_weekly_new
# MAGIC   UNION ALL
# MAGIC   SELECT nation_id, iso_week, new_coaches FROM world_weekly_new
# MAGIC ),
# MAGIC
# MAGIC -- Cumulative sum via window
# MAGIC cumulative AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     iso_week,
# MAGIC     new_coaches,
# MAGIC     CAST(SUM(new_coaches) OVER (
# MAGIC       PARTITION BY nation_id
# MAGIC       ORDER BY iso_week
# MAGIC       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC     ) AS INT)                                                 AS cumulative_coaches
# MAGIC   FROM combined
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   nation_id,
# MAGIC   iso_week,
# MAGIC   new_coaches,
# MAGIC   cumulative_coaches,
# MAGIC   CURRENT_TIMESTAMP()                                         AS load_timestamp
# MAGIC FROM cumulative;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TABLE: naf_catalog.gold_summary.nation_results_cumulative_series
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Per-game cumulative W/D/L record for each nation against
# MAGIC --                external opponents (intra-nation games excluded).
# MAGIC -- LAYER        : GOLD_SUMMARY
# MAGIC -- CONTRACT TYPE: SERIES (time series with cumulative windows)
# MAGIC -- GRAIN        : 1 row per (nation_id, game_sequence_number)
# MAGIC -- PRIMARY KEY  : (nation_id, game_sequence_number)
# MAGIC -- SOURCES      : naf_catalog.gold_fact.coach_games_fact,
# MAGIC --                naf_catalog.gold_dim.coach_dim (both sides for nation IDs)
# MAGIC -- NOTES        : Each individual game where a coach from nation N plays a
# MAGIC --                coach from a different nation produces one row for nation N.
# MAGIC --                Intra-nation games (same nation on both sides) are excluded.
# MAGIC --                Ordering: event_timestamp ASC, game_id ASC within nation.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_summary.nation_results_cumulative_series
# MAGIC USING DELTA AS
# MAGIC
# MAGIC WITH international_games AS (
# MAGIC   SELECT
# MAGIC     cgf.game_id,
# MAGIC     cgf.game_date,
# MAGIC     cgf.date_id,
# MAGIC     cgf.event_timestamp,
# MAGIC     cgf.tournament_id,
# MAGIC     c.nation_id,
# MAGIC     opp.nation_id         AS opponent_nation_id,
# MAGIC     cgf.result_numeric
# MAGIC   FROM naf_catalog.gold_fact.coach_games_fact cgf
# MAGIC   INNER JOIN naf_catalog.gold_dim.coach_dim c
# MAGIC     ON cgf.coach_id = c.coach_id
# MAGIC   INNER JOIN naf_catalog.gold_dim.coach_dim opp
# MAGIC     ON cgf.opponent_coach_id = opp.coach_id
# MAGIC   WHERE c.nation_id <> opp.nation_id          -- exclude intra-nation
# MAGIC     AND c.nation_id <> 0                       -- exclude Unknown-nation coaches
# MAGIC     AND opp.nation_id <> 0                     -- exclude Unknown-nation opponents
# MAGIC ),
# MAGIC
# MAGIC sequenced AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     game_id,
# MAGIC     game_date,
# MAGIC     date_id,
# MAGIC     event_timestamp,
# MAGIC     tournament_id,
# MAGIC     opponent_nation_id,
# MAGIC     result_numeric,
# MAGIC
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY nation_id
# MAGIC       ORDER BY event_timestamp ASC NULLS LAST, game_id ASC
# MAGIC     ) AS game_sequence_number
# MAGIC   FROM international_games
# MAGIC ),
# MAGIC
# MAGIC cumulative AS (
# MAGIC   SELECT
# MAGIC     s.*,
# MAGIC
# MAGIC     CAST(SUM(CASE WHEN result_numeric = 1.0 THEN 1 ELSE 0 END)
# MAGIC       OVER (PARTITION BY nation_id
# MAGIC             ORDER BY game_sequence_number
# MAGIC             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
# MAGIC     AS INT) AS cum_wins,
# MAGIC
# MAGIC     CAST(SUM(CASE WHEN result_numeric = 0.5 THEN 1 ELSE 0 END)
# MAGIC       OVER (PARTITION BY nation_id
# MAGIC             ORDER BY game_sequence_number
# MAGIC             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
# MAGIC     AS INT) AS cum_draws,
# MAGIC
# MAGIC     CAST(SUM(CASE WHEN result_numeric = 0.0 THEN 1 ELSE 0 END)
# MAGIC       OVER (PARTITION BY nation_id
# MAGIC             ORDER BY game_sequence_number
# MAGIC             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
# MAGIC     AS INT) AS cum_losses
# MAGIC   FROM sequenced s
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   nation_id,
# MAGIC   game_sequence_number,
# MAGIC   game_id,
# MAGIC   game_date,
# MAGIC   date_id,
# MAGIC   event_timestamp,
# MAGIC   tournament_id,
# MAGIC   opponent_nation_id,
# MAGIC   result_numeric,
# MAGIC
# MAGIC   cum_wins,
# MAGIC   cum_draws,
# MAGIC   cum_losses,
# MAGIC   (cum_wins + cum_draws + cum_losses)   AS cum_games,
# MAGIC
# MAGIC   CASE WHEN game_sequence_number > 0
# MAGIC     THEN CAST(cum_wins AS DOUBLE) / game_sequence_number
# MAGIC     ELSE NULL
# MAGIC   END AS cum_win_frac,
# MAGIC
# MAGIC   CASE WHEN game_sequence_number > 0
# MAGIC     THEN (cum_wins + 0.5 * cum_draws) / game_sequence_number
# MAGIC     ELSE NULL
# MAGIC   END AS cum_points_frac,
# MAGIC
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM cumulative;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TABLE: naf_catalog.gold_summary.nation_domestic_summary
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE:
# MAGIC --   Home/abroad performance split by tournament location and opponent origin.
# MAGIC --   Supports nation dashboard features showing domestic vs international patterns.
# MAGIC -- LAYER:
# MAGIC --   GOLD_SUMMARY
# MAGIC -- GRAIN:
# MAGIC --   1 row per nation_id
# MAGIC -- PRIMARY KEY:
# MAGIC --   (nation_id)
# MAGIC -- SOURCES:
# MAGIC --   - naf_catalog.gold_fact.coach_games_fact
# MAGIC --   - naf_catalog.gold_dim.coach_dim
# MAGIC --   - naf_catalog.gold_dim.tournament_dim
# MAGIC -- NOTES:
# MAGIC --   - Excludes Unknown nation (nation_id = 0)
# MAGIC --   - Home/away based on tournament_dim.nation_id vs coach_dim.nation_id
# MAGIC --   - Domestic/foreign based on opponent coach_dim.nation_id
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_summary.nation_domestic_summary
# MAGIC USING DELTA AS
# MAGIC
# MAGIC WITH game_context AS (
# MAGIC   SELECT
# MAGIC     cgf.game_id,
# MAGIC     cgf.coach_id,
# MAGIC     cgf.opponent_coach_id,
# MAGIC     cgf.tournament_id,
# MAGIC     cgf.result_numeric,
# MAGIC     c.nation_id AS coach_nation_id,
# MAGIC     t.nation_id AS tournament_nation_id,
# MAGIC     opp_c.nation_id AS opponent_nation_id
# MAGIC   FROM naf_catalog.gold_fact.coach_games_fact cgf
# MAGIC   INNER JOIN naf_catalog.gold_dim.coach_dim c
# MAGIC     ON cgf.coach_id = c.coach_id
# MAGIC   INNER JOIN naf_catalog.gold_dim.tournament_dim t
# MAGIC     ON cgf.tournament_id = t.tournament_id
# MAGIC   INNER JOIN naf_catalog.gold_dim.coach_dim opp_c
# MAGIC     ON cgf.opponent_coach_id = opp_c.coach_id
# MAGIC   WHERE c.nation_id <> 0
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   coach_nation_id AS nation_id,
# MAGIC
# MAGIC   -- Tournament location dimension
# MAGIC   CAST(SUM(CASE WHEN tournament_nation_id = coach_nation_id THEN 1 ELSE 0 END) AS INT) AS games_home,
# MAGIC   CAST(SUM(CASE WHEN tournament_nation_id <> coach_nation_id THEN 1 ELSE 0 END) AS INT) AS games_away,
# MAGIC   CAST(SUM(CASE WHEN tournament_nation_id = coach_nation_id THEN result_numeric ELSE 0 END) AS DOUBLE) /
# MAGIC     NULLIF(SUM(CASE WHEN tournament_nation_id = coach_nation_id THEN 1 ELSE 0 END), 0) AS win_frac_home,
# MAGIC   CAST(SUM(CASE WHEN tournament_nation_id <> coach_nation_id THEN result_numeric ELSE 0 END) AS DOUBLE) /
# MAGIC     NULLIF(SUM(CASE WHEN tournament_nation_id <> coach_nation_id THEN 1 ELSE 0 END), 0) AS win_frac_away,
# MAGIC
# MAGIC   -- Opponent origin dimension
# MAGIC   CAST(SUM(CASE WHEN opponent_nation_id = coach_nation_id THEN 1 ELSE 0 END) AS INT) AS games_vs_domestic,
# MAGIC   CAST(SUM(CASE WHEN opponent_nation_id <> coach_nation_id THEN 1 ELSE 0 END) AS INT) AS games_vs_foreign,
# MAGIC   CAST(SUM(CASE WHEN opponent_nation_id = coach_nation_id THEN result_numeric ELSE 0 END) AS DOUBLE) /
# MAGIC     NULLIF(SUM(CASE WHEN opponent_nation_id = coach_nation_id THEN 1 ELSE 0 END), 0) AS win_frac_vs_domestic,
# MAGIC   CAST(SUM(CASE WHEN opponent_nation_id <> coach_nation_id THEN result_numeric ELSE 0 END) AS DOUBLE) /
# MAGIC     NULLIF(SUM(CASE WHEN opponent_nation_id <> coach_nation_id THEN 1 ELSE 0 END), 0) AS win_frac_vs_foreign,
# MAGIC
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM game_context
# MAGIC GROUP BY coach_nation_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TABLE: naf_catalog.gold_summary.nation_overview_comparison_summary
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE:
# MAGIC --   - For each focus_nation_id, provide NATION vs REST_OF_WORLD vs WORLD
# MAGIC -- GRAIN:
# MAGIC --   - One row per (focus_nation_id, comparison_group)
# MAGIC -- NOTES:
# MAGIC --   - REST_OF_WORLD is computed as WORLD minus NATION (where valid)
# MAGIC --   - glo_peak_median for REST_OF_WORLD is not subtractable -> NULL
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_summary.nation_overview_comparison_summary
# MAGIC USING DELTA AS
# MAGIC
# MAGIC WITH n AS (
# MAGIC   SELECT *
# MAGIC   FROM naf_catalog.gold_summary.nation_overview_summary
# MAGIC ),
# MAGIC
# MAGIC -- Coach participation spine (same dedup logic as nation_overview_summary)
# MAGIC coach_base AS (
# MAGIC   SELECT
# MAGIC     cg.game_id,
# MAGIC     cg.tournament_id,
# MAGIC     cg.coach_id,
# MAGIC     cg.result_numeric,
# MAGIC     cd.nation_id
# MAGIC   FROM naf_catalog.gold_fact.coach_games_fact cg
# MAGIC   INNER JOIN naf_catalog.gold_dim.coach_dim cd
# MAGIC     ON cg.coach_id = cd.coach_id
# MAGIC   WHERE cd.nation_id IS NOT NULL
# MAGIC     AND cg.game_id   IS NOT NULL
# MAGIC     AND cg.coach_id  IS NOT NULL
# MAGIC   QUALIFY ROW_NUMBER() OVER (
# MAGIC     PARTITION BY cg.game_id, cg.coach_id
# MAGIC     ORDER BY cg.event_timestamp DESC NULLS LAST,
# MAGIC              cg.game_date       DESC NULLS LAST,
# MAGIC              cg.tournament_id   DESC NULLS LAST
# MAGIC   ) = 1
# MAGIC ),
# MAGIC
# MAGIC points_by_nation AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     SUM(result_numeric)  AS points_sum,
# MAGIC     COUNT(result_numeric) AS points_n
# MAGIC   FROM coach_base
# MAGIC   GROUP BY nation_id
# MAGIC ),
# MAGIC
# MAGIC points_global AS (
# MAGIC   SELECT
# MAGIC     SUM(result_numeric)  AS points_sum,
# MAGIC     COUNT(result_numeric) AS points_n
# MAGIC   FROM coach_base
# MAGIC ),
# MAGIC
# MAGIC -- Elo distribution spine (assumes nation_coach_glo_metrics has glo_peak per coach+nation)
# MAGIC glo_valid AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     glo_peak
# MAGIC   FROM naf_catalog.gold_summary.nation_coach_glo_metrics
# MAGIC   WHERE glo_peak IS NOT NULL
# MAGIC     AND COALESCE(is_valid_glo, TRUE) = TRUE
# MAGIC ),
# MAGIC
# MAGIC glo_by_nation AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     SUM(glo_peak) AS glo_peak_sum,
# MAGIC     COUNT(*)      AS glo_peak_n,
# MAGIC     MAX(glo_peak) AS glo_peak_max,
# MAGIC     PERCENTILE_APPROX(glo_peak, 0.5) AS glo_peak_median
# MAGIC   FROM glo_valid
# MAGIC   GROUP BY nation_id
# MAGIC ),
# MAGIC
# MAGIC glo_global AS (
# MAGIC   SELECT
# MAGIC     SUM(glo_peak) AS glo_peak_sum,
# MAGIC     COUNT(*)      AS glo_peak_n,
# MAGIC     MAX(glo_peak) AS glo_peak_max,
# MAGIC     PERCENTILE_APPROX(glo_peak, 0.5) AS glo_peak_median
# MAGIC   FROM glo_valid
# MAGIC ),
# MAGIC
# MAGIC -- REST_OF_WORLD max is doable without scanning "all coaches minus nation" per nation
# MAGIC glo_max_stats AS (
# MAGIC   SELECT
# MAGIC     gg.glo_peak_max AS global_max,
# MAGIC     MAX(CASE WHEN gb.glo_peak_max < gg.glo_peak_max THEN gb.glo_peak_max END) AS global_second_max,
# MAGIC     SUM(CASE WHEN gb.glo_peak_max = gg.glo_peak_max THEN 1 ELSE 0 END) AS nations_with_global_max
# MAGIC   FROM glo_by_nation gb
# MAGIC   CROSS JOIN glo_global gg
# MAGIC   GROUP BY gg.glo_peak_max
# MAGIC )
# MAGIC ,
# MAGIC
# MAGIC all_focus_nations AS (
# MAGIC   SELECT nation_id AS focus_nation_id FROM n
# MAGIC )
# MAGIC
# MAGIC -- =========================
# MAGIC -- NATION row
# MAGIC -- =========================
# MAGIC SELECT
# MAGIC   f.focus_nation_id,
# MAGIC   'NATION' AS comparison_group,
# MAGIC
# MAGIC   n.coaches_count,
# MAGIC   n.coach_participations_count,
# MAGIC   n.game_representations_count,
# MAGIC   n.tournaments_attended_count,
# MAGIC   n.avg_points_per_game,
# MAGIC
# MAGIC   n.coaches_global_pct,
# MAGIC   n.coach_participations_global_pct,
# MAGIC   n.game_representations_global_pct,
# MAGIC   n.tournaments_attended_global_pct,
# MAGIC
# MAGIC   n.games_hosted_count,
# MAGIC   n.tournaments_hosted_count,
# MAGIC   n.games_hosted_global_pct,
# MAGIC   n.tournaments_hosted_global_pct,
# MAGIC
# MAGIC   n.glo_peak_max,
# MAGIC   n.glo_peak_mean,
# MAGIC   n.glo_peak_median,
# MAGIC
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM all_focus_nations f
# MAGIC LEFT JOIN n
# MAGIC   ON f.focus_nation_id = n.nation_id
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- =========================
# MAGIC -- REST_OF_WORLD row
# MAGIC -- =========================
# MAGIC SELECT
# MAGIC   f.focus_nation_id,
# MAGIC   'REST_OF_WORLD' AS comparison_group,
# MAGIC
# MAGIC   -- disjoint counts (valid subtraction)
# MAGIC   (n_total.coaches_count_global - COALESCE(n.coaches_count, 0))              AS coaches_count,
# MAGIC   (n_total.coach_participations_count_global - COALESCE(n.coach_participations_count, 0)) AS coach_participations_count,
# MAGIC   (n_total.game_representations_count_global - COALESCE(n.game_representations_count, 0)) AS game_representations_count,
# MAGIC   (n_total.tournaments_attended_count_global - COALESCE(n.tournaments_attended_count, 0)) AS tournaments_attended_count,
# MAGIC
# MAGIC   -- weighted mean from sums/counts (robust to NULL result_numeric)
# MAGIC   ((pg.points_sum - COALESCE(pn.points_sum, 0.0))
# MAGIC     / NULLIF((pg.points_n - COALESCE(pn.points_n, 0)), 0)
# MAGIC   ) AS avg_points_per_game,
# MAGIC
# MAGIC   (100.0 - COALESCE(n.coaches_global_pct, 0.0))                   AS coaches_global_pct,
# MAGIC   (100.0 - COALESCE(n.coach_participations_global_pct, 0.0))      AS coach_participations_global_pct,
# MAGIC   (100.0 - COALESCE(n.game_representations_global_pct, 0.0))      AS game_representations_global_pct,
# MAGIC   (100.0 - COALESCE(n.tournaments_attended_global_pct, 0.0))      AS tournaments_attended_global_pct,
# MAGIC
# MAGIC   (h_total.games_hosted_count_global - COALESCE(n.games_hosted_count, 0))       AS games_hosted_count,
# MAGIC   (h_total.tournaments_hosted_count_global - COALESCE(n.tournaments_hosted_count, 0)) AS tournaments_hosted_count,
# MAGIC   (100.0 - COALESCE(n.games_hosted_global_pct, 0.0))               AS games_hosted_global_pct,
# MAGIC   (100.0 - COALESCE(n.tournaments_hosted_global_pct, 0.0))         AS tournaments_hosted_global_pct,
# MAGIC
# MAGIC   -- Rest-of-world Elo: mean + max are possible; median is not subtractable
# MAGIC   CASE
# MAGIC     WHEN (g.glo_peak_n - COALESCE(gn.glo_peak_n, 0)) > 0
# MAGIC     THEN (g.glo_peak_sum - COALESCE(gn.glo_peak_sum, 0.0)) / (g.glo_peak_n - COALESCE(gn.glo_peak_n, 0))
# MAGIC   END AS glo_peak_mean,
# MAGIC
# MAGIC   CASE
# MAGIC     WHEN COALESCE(gn.glo_peak_max, -1) = ms.global_max AND ms.nations_with_global_max = 1
# MAGIC       THEN ms.global_second_max
# MAGIC     ELSE ms.global_max
# MAGIC   END AS glo_peak_max,
# MAGIC
# MAGIC   CAST(NULL AS DOUBLE) AS glo_peak_median,
# MAGIC
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM all_focus_nations f
# MAGIC LEFT JOIN n
# MAGIC   ON f.focus_nation_id = n.nation_id
# MAGIC CROSS JOIN (
# MAGIC   SELECT
# MAGIC     MAX(coaches_count_global)              AS coaches_count_global,
# MAGIC     MAX(coach_participations_count_global) AS coach_participations_count_global,
# MAGIC     MAX(game_representations_count_global) AS game_representations_count_global,
# MAGIC     MAX(tournaments_attended_count_global) AS tournaments_attended_count_global
# MAGIC   FROM (
# MAGIC     SELECT
# MAGIC       -- invert from your existing nation_overview_summary global pct denominators is messy,
# MAGIC       -- so we compute these by summing the base table once:
# MAGIC       COUNT(DISTINCT coach_id)      AS coaches_count_global,
# MAGIC       COUNT(*)                      AS coach_participations_count_global,
# MAGIC       COUNT(DISTINCT game_id)       AS game_representations_count_global,
# MAGIC       COUNT(DISTINCT tournament_id) AS tournaments_attended_count_global
# MAGIC     FROM coach_base
# MAGIC   )
# MAGIC ) n_total
# MAGIC CROSS JOIN (
# MAGIC   SELECT
# MAGIC     COUNT(DISTINCT g.game_id)       AS games_hosted_count_global,
# MAGIC     COUNT(DISTINCT g.tournament_id) AS tournaments_hosted_count_global
# MAGIC   FROM naf_catalog.gold_fact.games_fact g
# MAGIC   INNER JOIN naf_catalog.gold_dim.tournament_dim td
# MAGIC     ON g.tournament_id = td.tournament_id
# MAGIC   WHERE td.nation_id IS NOT NULL
# MAGIC     AND g.game_id IS NOT NULL
# MAGIC     AND g.tournament_id IS NOT NULL
# MAGIC ) h_total
# MAGIC CROSS JOIN points_global pg
# MAGIC LEFT JOIN points_by_nation pn
# MAGIC   ON f.focus_nation_id = pn.nation_id
# MAGIC CROSS JOIN glo_global g
# MAGIC LEFT JOIN glo_by_nation gn
# MAGIC   ON f.focus_nation_id = gn.nation_id
# MAGIC CROSS JOIN glo_max_stats ms
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- =========================
# MAGIC -- WORLD row (repeated per focus nation)
# MAGIC -- =========================
# MAGIC SELECT
# MAGIC   f.focus_nation_id,
# MAGIC   'WORLD' AS comparison_group,
# MAGIC
# MAGIC   n_total.coaches_count_global              AS coaches_count,
# MAGIC   n_total.coach_participations_count_global AS coach_participations_count,
# MAGIC   n_total.game_representations_count_global AS game_representations_count,
# MAGIC   n_total.tournaments_attended_count_global AS tournaments_attended_count,
# MAGIC
# MAGIC   (pg.points_sum / NULLIF(pg.points_n, 0))  AS avg_points_per_game,
# MAGIC
# MAGIC   100.0 AS coaches_global_pct,
# MAGIC   100.0 AS coach_participations_global_pct,
# MAGIC   100.0 AS game_representations_global_pct,
# MAGIC   100.0 AS tournaments_attended_global_pct,
# MAGIC
# MAGIC   h_total.games_hosted_count_global       AS games_hosted_count,
# MAGIC   h_total.tournaments_hosted_count_global AS tournaments_hosted_count,
# MAGIC   100.0 AS games_hosted_global_pct,
# MAGIC   100.0 AS tournaments_hosted_global_pct,
# MAGIC
# MAGIC   g.glo_peak_max,
# MAGIC   (g.glo_peak_sum / NULLIF(g.glo_peak_n, 0)) AS glo_peak_mean,
# MAGIC   g.glo_peak_median,
# MAGIC
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM all_focus_nations f
# MAGIC CROSS JOIN (
# MAGIC   SELECT
# MAGIC     COUNT(DISTINCT coach_id)      AS coaches_count_global,
# MAGIC     COUNT(*)                      AS coach_participations_count_global,
# MAGIC     COUNT(DISTINCT game_id)       AS game_representations_count_global,
# MAGIC     COUNT(DISTINCT tournament_id) AS tournaments_attended_count_global
# MAGIC   FROM coach_base
# MAGIC ) n_total
# MAGIC CROSS JOIN (
# MAGIC   SELECT
# MAGIC     COUNT(DISTINCT g.game_id)       AS games_hosted_count_global,
# MAGIC     COUNT(DISTINCT g.tournament_id) AS tournaments_hosted_count_global
# MAGIC   FROM naf_catalog.gold_fact.games_fact g
# MAGIC   INNER JOIN naf_catalog.gold_dim.tournament_dim td
# MAGIC     ON g.tournament_id = td.tournament_id
# MAGIC   WHERE td.nation_id IS NOT NULL
# MAGIC     AND g.game_id IS NOT NULL
# MAGIC     AND g.tournament_id IS NOT NULL
# MAGIC ) h_total
# MAGIC CROSS JOIN points_global pg
# MAGIC CROSS JOIN glo_global g;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TABLE: naf_catalog.gold_summary.nation_overview_comparison
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE:
# MAGIC --   - For each focus nation_id, produce comparison rows for:
# MAGIC --       * NATION
# MAGIC --       * REST_OF_WORLD
# MAGIC --       * WORLD
# MAGIC -- GRAIN:
# MAGIC --   - One row per (focus_nation_id, comparison_group)
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_summary.nation_overview_comparison
# MAGIC USING DELTA AS
# MAGIC
# MAGIC WITH nation_base AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC
# MAGIC     COALESCE(coaches_count, 0)              AS coaches_count,
# MAGIC     COALESCE(coach_participations_count, 0) AS coach_participations_count,
# MAGIC     avg_points_per_game,
# MAGIC
# MAGIC     COALESCE(coaches_global_pct, 0.0)              AS coaches_global_pct,
# MAGIC     COALESCE(coach_participations_global_pct, 0.0) AS coach_participations_global_pct,
# MAGIC
# MAGIC     COALESCE(games_hosted_count, 0)       AS games_hosted_count,
# MAGIC     COALESCE(tournaments_hosted_count, 0) AS tournaments_hosted_count,
# MAGIC     COALESCE(games_hosted_global_pct, 0.0)       AS games_hosted_global_pct,
# MAGIC     COALESCE(tournaments_hosted_global_pct, 0.0) AS tournaments_hosted_global_pct,
# MAGIC
# MAGIC     glo_peak_max,
# MAGIC     glo_peak_mean,
# MAGIC     glo_peak_median
# MAGIC   FROM naf_catalog.gold_summary.nation_overview_summary
# MAGIC ),
# MAGIC
# MAGIC glo_counts AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     COUNT(*)      AS glo_coaches_count,
# MAGIC     SUM(glo_peak) AS glo_peak_sum
# MAGIC   FROM naf_catalog.gold_summary.nation_coach_glo_metrics
# MAGIC   WHERE is_valid_glo = TRUE
# MAGIC     AND glo_peak IS NOT NULL
# MAGIC   GROUP BY nation_id
# MAGIC ),
# MAGIC
# MAGIC nation_enriched AS (
# MAGIC   SELECT
# MAGIC     nb.*,
# MAGIC     COALESCE(gc.glo_coaches_count, 0) AS glo_coaches_count,
# MAGIC     COALESCE(gc.glo_peak_sum, 0.0)    AS glo_peak_sum,
# MAGIC     (COALESCE(nb.avg_points_per_game, 0.0) * CAST(nb.coach_participations_count AS DOUBLE)) AS points_sum
# MAGIC   FROM nation_base nb
# MAGIC   LEFT JOIN glo_counts gc
# MAGIC     ON nb.nation_id = gc.nation_id
# MAGIC ),
# MAGIC
# MAGIC world_counts AS (
# MAGIC   SELECT
# MAGIC     CAST(SUM(coaches_count) AS BIGINT)              AS coaches_count,
# MAGIC     CAST(SUM(coach_participations_count) AS BIGINT) AS coach_participations_count,
# MAGIC     SUM(points_sum)                                  AS points_sum,
# MAGIC
# MAGIC     CAST(SUM(games_hosted_count) AS BIGINT)         AS games_hosted_count,
# MAGIC     CAST(SUM(tournaments_hosted_count) AS BIGINT)   AS tournaments_hosted_count
# MAGIC   FROM nation_enriched
# MAGIC ),
# MAGIC
# MAGIC world_glo AS (
# MAGIC   SELECT
# MAGIC     CAST(COUNT(*) AS BIGINT)                AS glo_coaches_count,
# MAGIC     SUM(glo_peak)                           AS glo_peak_sum,
# MAGIC     MAX(glo_peak)                           AS glo_peak_max,
# MAGIC     PERCENTILE_APPROX(glo_peak, 0.5)        AS glo_peak_median
# MAGIC   FROM naf_catalog.gold_summary.nation_coach_glo_metrics
# MAGIC   WHERE is_valid_glo = TRUE
# MAGIC     AND glo_peak IS NOT NULL
# MAGIC ),
# MAGIC
# MAGIC world_final AS (
# MAGIC   SELECT
# MAGIC     wc.*,
# MAGIC     wg.glo_coaches_count,
# MAGIC     wg.glo_peak_sum,
# MAGIC     wg.glo_peak_max,
# MAGIC     wg.glo_peak_median,
# MAGIC
# MAGIC     (wc.points_sum / NULLIF(wc.coach_participations_count, 0)) AS avg_points_per_game,
# MAGIC     (wg.glo_peak_sum / NULLIF(wg.glo_coaches_count, 0))        AS glo_peak_mean
# MAGIC   FROM world_counts wc
# MAGIC   CROSS JOIN world_glo wg
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   ne.nation_id AS focus_nation_id,
# MAGIC   'NATION'     AS comparison_group,
# MAGIC
# MAGIC   CAST(ne.coaches_count AS BIGINT)              AS coaches_count,
# MAGIC   CAST(ne.coach_participations_count AS BIGINT) AS coach_participations_count,
# MAGIC   ne.avg_points_per_game                        AS avg_points_per_game,
# MAGIC
# MAGIC   ne.coaches_global_pct                         AS coaches_global_pct,
# MAGIC   ne.coach_participations_global_pct            AS coach_participations_global_pct,
# MAGIC
# MAGIC   CAST(ne.games_hosted_count AS BIGINT)         AS games_hosted_count,
# MAGIC   CAST(ne.tournaments_hosted_count AS BIGINT)   AS tournaments_hosted_count,
# MAGIC   ne.games_hosted_global_pct                    AS games_hosted_global_pct,
# MAGIC   ne.tournaments_hosted_global_pct              AS tournaments_hosted_global_pct,
# MAGIC
# MAGIC   CAST(ne.glo_coaches_count AS BIGINT)          AS glo_coaches_count,
# MAGIC   ne.glo_peak_max,
# MAGIC   ne.glo_peak_mean,
# MAGIC   ne.glo_peak_median,
# MAGIC
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM nation_enriched ne
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   ne.nation_id AS focus_nation_id,
# MAGIC   'REST_OF_WORLD' AS comparison_group,
# MAGIC
# MAGIC   (wf.coaches_count - CAST(ne.coaches_count AS BIGINT)) AS coaches_count,
# MAGIC   (wf.coach_participations_count - CAST(ne.coach_participations_count AS BIGINT)) AS coach_participations_count,
# MAGIC
# MAGIC   ((wf.points_sum - ne.points_sum)
# MAGIC      / NULLIF((wf.coach_participations_count - CAST(ne.coach_participations_count AS BIGINT)), 0)
# MAGIC   ) AS avg_points_per_game,
# MAGIC
# MAGIC   (100.0 - COALESCE(ne.coaches_global_pct, 0.0))              AS coaches_global_pct,
# MAGIC   (100.0 - COALESCE(ne.coach_participations_global_pct, 0.0)) AS coach_participations_global_pct,
# MAGIC
# MAGIC   (wf.games_hosted_count - CAST(ne.games_hosted_count AS BIGINT)) AS games_hosted_count,
# MAGIC   (wf.tournaments_hosted_count - CAST(ne.tournaments_hosted_count AS BIGINT)) AS tournaments_hosted_count,
# MAGIC   (100.0 - COALESCE(ne.games_hosted_global_pct, 0.0))             AS games_hosted_global_pct,
# MAGIC   (100.0 - COALESCE(ne.tournaments_hosted_global_pct, 0.0))       AS tournaments_hosted_global_pct,
# MAGIC
# MAGIC   (wf.glo_coaches_count - CAST(ne.glo_coaches_count AS BIGINT)) AS glo_coaches_count,
# MAGIC
# MAGIC   CAST(NULL AS DOUBLE) AS glo_peak_max,
# MAGIC   ((wf.glo_peak_sum - ne.glo_peak_sum)
# MAGIC      / NULLIF((wf.glo_coaches_count - CAST(ne.glo_coaches_count AS BIGINT)), 0)
# MAGIC   ) AS glo_peak_mean,
# MAGIC   CAST(NULL AS DOUBLE) AS glo_peak_median,
# MAGIC
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM nation_enriched ne
# MAGIC CROSS JOIN world_final wf
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   ne.nation_id AS focus_nation_id,
# MAGIC   'WORLD'      AS comparison_group,
# MAGIC
# MAGIC   wf.coaches_count,
# MAGIC   wf.coach_participations_count,
# MAGIC   wf.avg_points_per_game,
# MAGIC
# MAGIC   100.0 AS coaches_global_pct,
# MAGIC   100.0 AS coach_participations_global_pct,
# MAGIC
# MAGIC   wf.games_hosted_count,
# MAGIC   wf.tournaments_hosted_count,
# MAGIC   100.0 AS games_hosted_global_pct,
# MAGIC   100.0 AS tournaments_hosted_global_pct,
# MAGIC
# MAGIC   wf.glo_coaches_count,
# MAGIC   wf.glo_peak_max,
# MAGIC   wf.glo_peak_mean,
# MAGIC   wf.glo_peak_median,
# MAGIC
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM nation_enriched ne
# MAGIC CROSS JOIN world_final wf;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TABLE: naf_catalog.gold_summary.nation_race_summary
# MAGIC -- =============================================================================
# MAGIC -- PURPOSE:
# MAGIC --   - Nation x Race performance + composition (based on coach nationality)
# MAGIC -- GRAIN:
# MAGIC --   - One row per (nation_id, race_id)
# MAGIC -- =============================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_summary.nation_race_summary
# MAGIC USING DELTA AS
# MAGIC
# MAGIC WITH base_games AS (
# MAGIC   SELECT
# MAGIC     cg.tournament_id,
# MAGIC     cg.game_id,
# MAGIC     cg.coach_id,
# MAGIC     cg.race_id,
# MAGIC     cg.result_numeric,
# MAGIC     cd.nation_id
# MAGIC   FROM naf_catalog.gold_fact.coach_games_fact AS cg
# MAGIC   INNER JOIN naf_catalog.gold_dim.coach_dim AS cd
# MAGIC     ON cg.coach_id = cd.coach_id
# MAGIC   WHERE cd.nation_id IS NOT NULL
# MAGIC     AND cg.game_id IS NOT NULL
# MAGIC     AND cg.coach_id IS NOT NULL
# MAGIC     AND cg.tournament_id IS NOT NULL
# MAGIC     AND cg.race_id IS NOT NULL
# MAGIC     AND cg.race_id <> 0
# MAGIC     AND cg.result_numeric IN (0.0, 0.5, 1.0)
# MAGIC   QUALIFY ROW_NUMBER() OVER (
# MAGIC     PARTITION BY cg.game_id, cg.coach_id
# MAGIC     ORDER BY cg.event_timestamp DESC NULLS LAST,
# MAGIC              cg.game_date       DESC NULLS LAST,
# MAGIC              cg.tournament_id   DESC NULLS LAST
# MAGIC   ) = 1
# MAGIC ),
# MAGIC
# MAGIC nation_totals AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     CAST(COUNT(DISTINCT coach_id)      AS INT) AS coaches_count,
# MAGIC     CAST(COUNT(*)                      AS INT) AS coach_participations_count,
# MAGIC     CAST(COUNT(DISTINCT tournament_id) AS INT) AS tournaments_attended_count
# MAGIC   FROM base_games
# MAGIC   GROUP BY nation_id
# MAGIC ),
# MAGIC
# MAGIC nation_race_agg AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     race_id,
# MAGIC     CAST(COUNT(DISTINCT coach_id)      AS INT) AS coaches_count,
# MAGIC     CAST(COUNT(*)                      AS INT) AS coach_participations_count,
# MAGIC     CAST(COUNT(DISTINCT tournament_id) AS INT) AS tournaments_attended_count,
# MAGIC     AVG(result_numeric)                        AS avg_points_per_game
# MAGIC   FROM base_games
# MAGIC   GROUP BY nation_id, race_id
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   nra.nation_id,
# MAGIC   nra.race_id,
# MAGIC
# MAGIC   nra.coaches_count,
# MAGIC   nra.coach_participations_count,
# MAGIC   nra.tournaments_attended_count,
# MAGIC   nra.avg_points_per_game,
# MAGIC
# MAGIC   100.0 * nra.coaches_count
# MAGIC     / NULLIF(nt.coaches_count, 0) AS coaches_pct_nation,
# MAGIC
# MAGIC   100.0 * nra.coach_participations_count
# MAGIC     / NULLIF(nt.coach_participations_count, 0) AS coach_participations_pct_nation,
# MAGIC
# MAGIC   100.0 * nra.tournaments_attended_count
# MAGIC     / NULLIF(nt.tournaments_attended_count, 0) AS tournaments_attended_pct_nation,
# MAGIC
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM nation_race_agg AS nra
# MAGIC INNER JOIN nation_totals AS nt
# MAGIC   ON nra.nation_id = nt.nation_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TABLE: naf_catalog.gold_summary.nation_glo_metric_quantiles
# MAGIC -- =============================================================================
# MAGIC -- PURPOSE:
# MAGIC --   - Nation-level quantiles (boxplot stats) for GLOBAL (race-agnostic) NAF_ELO metrics
# MAGIC -- GRAIN:
# MAGIC --   - One row per (nation_id, metric_type)
# MAGIC -- SOURCES:
# MAGIC --   - naf_catalog.gold_summary.nation_coach_glo_metrics
# MAGIC -- NOTES:
# MAGIC --   - Uses is_valid_glo = TRUE as “reportable/stable sample” filter
# MAGIC -- =============================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_summary.nation_glo_metric_quantiles
# MAGIC USING DELTA AS
# MAGIC
# MAGIC WITH metric_long AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     coach_id,
# MAGIC     stack(
# MAGIC       3,
# MAGIC       'PEAK',   glo_peak,
# MAGIC       'MEAN',   glo_mean,
# MAGIC       'MEDIAN', glo_median
# MAGIC     ) AS (metric_type, metric_value)
# MAGIC   FROM naf_catalog.gold_summary.nation_coach_glo_metrics
# MAGIC   WHERE is_valid_glo = TRUE
# MAGIC ),
# MAGIC
# MAGIC agg AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     metric_type,
# MAGIC     CAST(COUNT(DISTINCT coach_id) AS INT) AS coaches_count,
# MAGIC     MIN(metric_value)                     AS value_min,
# MAGIC     PERCENTILE_APPROX(metric_value, 0.10) AS value_p10,
# MAGIC     PERCENTILE_APPROX(metric_value, 0.25) AS value_p25,
# MAGIC     PERCENTILE_APPROX(metric_value, 0.50) AS value_p50,
# MAGIC     PERCENTILE_APPROX(metric_value, 0.75) AS value_p75,
# MAGIC     PERCENTILE_APPROX(metric_value, 0.90) AS value_p90,
# MAGIC     MAX(metric_value)                     AS value_max
# MAGIC   FROM metric_long
# MAGIC   WHERE metric_value IS NOT NULL
# MAGIC   GROUP BY nation_id, metric_type
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   nation_id,
# MAGIC   metric_type,
# MAGIC   coaches_count,
# MAGIC   value_min,
# MAGIC   value_p10,
# MAGIC   value_p25,
# MAGIC   value_p50,
# MAGIC   value_p75,
# MAGIC   value_p90,
# MAGIC   value_max,
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM agg;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TABLE: naf_catalog.gold_summary.nation_glo_binned_distribution
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Histogram of per-coach GLO values binned into fixed-width
# MAGIC --                buckets. Supports PEAK and MEDIAN metric types.
# MAGIC --                Used for density/histogram charts comparing nations vs World.
# MAGIC -- LAYER        : GOLD_SUMMARY
# MAGIC -- GRAIN        : 1 row per (nation_id, metric_type, glo_bin)
# MAGIC -- PRIMARY KEY  : (nation_id, metric_type, glo_bin)
# MAGIC -- SOURCES      : naf_catalog.gold_summary.nation_coach_glo_metrics
# MAGIC -- NOTES
# MAGIC --   - Bin width = 25 GLO points (e.g. 100–125, 125–150, ...).
# MAGIC --   - Only eligible coaches (is_valid_glo = TRUE).
# MAGIC --   - density = count / total coaches in that nation+metric (sums to 1).
# MAGIC --   - cumulative_density = running sum of density up to this bin.
# MAGIC --   - A World aggregate (nation_id = 0) is included for comparison.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_summary.nation_glo_binned_distribution
# MAGIC USING DELTA AS
# MAGIC
# MAGIC WITH metric_long AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     coach_id,
# MAGIC     stack(
# MAGIC       2,
# MAGIC       'PEAK',   glo_peak,
# MAGIC       'MEDIAN', glo_median
# MAGIC     ) AS (metric_type, metric_value)
# MAGIC   FROM naf_catalog.gold_summary.nation_coach_glo_metrics
# MAGIC   WHERE is_valid_glo = TRUE
# MAGIC ),
# MAGIC
# MAGIC binned AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     metric_type,
# MAGIC     CAST(FLOOR(metric_value / 25.0) * 25 AS INT) AS glo_bin,
# MAGIC     COUNT(*) AS coach_count
# MAGIC   FROM metric_long
# MAGIC   WHERE metric_value IS NOT NULL
# MAGIC     AND nation_id <> 0   -- World handled separately in world_binned
# MAGIC   GROUP BY nation_id, metric_type, FLOOR(metric_value / 25.0) * 25
# MAGIC ),
# MAGIC
# MAGIC nation_totals AS (
# MAGIC   SELECT nation_id, metric_type, SUM(coach_count) AS total_coaches
# MAGIC   FROM binned
# MAGIC   GROUP BY nation_id, metric_type
# MAGIC ),
# MAGIC
# MAGIC nation_density AS (
# MAGIC   SELECT
# MAGIC     b.nation_id,
# MAGIC     b.metric_type,
# MAGIC     b.glo_bin,
# MAGIC     b.coach_count,
# MAGIC     CAST(t.total_coaches AS INT) AS total_coaches,
# MAGIC     CAST(b.coach_count AS DOUBLE) / t.total_coaches AS density,
# MAGIC     SUM(CAST(b.coach_count AS DOUBLE) / t.total_coaches) OVER (
# MAGIC       PARTITION BY b.nation_id, b.metric_type
# MAGIC       ORDER BY b.glo_bin
# MAGIC       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC     ) AS cumulative_density
# MAGIC   FROM binned b
# MAGIC   JOIN nation_totals t
# MAGIC     ON b.nation_id = t.nation_id AND b.metric_type = t.metric_type
# MAGIC ),
# MAGIC
# MAGIC -- World aggregate (nation_id = 0)
# MAGIC world_binned AS (
# MAGIC   SELECT
# MAGIC     metric_type,
# MAGIC     CAST(FLOOR(metric_value / 25.0) * 25 AS INT) AS glo_bin,
# MAGIC     COUNT(*) AS coach_count
# MAGIC   FROM metric_long
# MAGIC   WHERE metric_value IS NOT NULL
# MAGIC   GROUP BY metric_type, FLOOR(metric_value / 25.0) * 25
# MAGIC ),
# MAGIC
# MAGIC world_totals AS (
# MAGIC   SELECT metric_type, SUM(coach_count) AS total_coaches
# MAGIC   FROM world_binned
# MAGIC   GROUP BY metric_type
# MAGIC ),
# MAGIC
# MAGIC world_density AS (
# MAGIC   SELECT
# MAGIC     0 AS nation_id,
# MAGIC     b.metric_type,
# MAGIC     b.glo_bin,
# MAGIC     b.coach_count,
# MAGIC     CAST(t.total_coaches AS INT) AS total_coaches,
# MAGIC     CAST(b.coach_count AS DOUBLE) / t.total_coaches AS density,
# MAGIC     SUM(CAST(b.coach_count AS DOUBLE) / t.total_coaches) OVER (
# MAGIC       PARTITION BY b.metric_type
# MAGIC       ORDER BY b.glo_bin
# MAGIC       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC     ) AS cumulative_density
# MAGIC   FROM world_binned b
# MAGIC   JOIN world_totals t
# MAGIC     ON b.metric_type = t.metric_type
# MAGIC )
# MAGIC
# MAGIC SELECT nation_id, metric_type, glo_bin, coach_count, total_coaches,
# MAGIC        density, cumulative_density,
# MAGIC        CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM nation_density
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT nation_id, metric_type, glo_bin, coach_count, total_coaches,
# MAGIC        density, cumulative_density,
# MAGIC        CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM world_density;
# MAGIC

# COMMAND ----------

# MAGIC %sql -- TABLE: naf_catalog.gold_summary.nation_vs_nation_summary
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE:
# MAGIC --   - Nation vs Nation matchup summary (coach nationality)
# MAGIC --   - Results from games_fact (home/away), exchange from rating_history_fact
# MAGIC -- GRAIN:
# MAGIC --   - One row per (nation_id, opponent_nation_id)
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_summary.nation_vs_nation_summary
# MAGIC USING DELTA AS
# MAGIC
# MAGIC WITH game_base AS (
# MAGIC   SELECT
# MAGIC     g.game_id,
# MAGIC     g.home_coach_id,
# MAGIC     g.away_coach_id,
# MAGIC     g.td_home,
# MAGIC     g.td_away,
# MAGIC
# MAGIC     ch.nation_id AS home_nation_id,
# MAGIC     ca.nation_id AS away_nation_id,
# MAGIC
# MAGIC     CASE
# MAGIC       WHEN g.td_home > g.td_away THEN 1.0
# MAGIC       WHEN g.td_home = g.td_away THEN 0.5
# MAGIC       WHEN g.td_home < g.td_away THEN 0.0
# MAGIC     END AS home_score,
# MAGIC
# MAGIC     CASE
# MAGIC       WHEN g.td_away > g.td_home THEN 1.0
# MAGIC       WHEN g.td_away = g.td_home THEN 0.5
# MAGIC       WHEN g.td_away < g.td_home THEN 0.0
# MAGIC     END AS away_score
# MAGIC
# MAGIC   FROM naf_catalog.gold_fact.games_fact g
# MAGIC   INNER JOIN naf_catalog.gold_dim.coach_dim ch
# MAGIC     ON g.home_coach_id = ch.coach_id
# MAGIC   INNER JOIN naf_catalog.gold_dim.coach_dim ca
# MAGIC     ON g.away_coach_id = ca.coach_id
# MAGIC   WHERE g.game_id IS NOT NULL
# MAGIC     AND g.home_coach_id IS NOT NULL
# MAGIC     AND g.away_coach_id IS NOT NULL
# MAGIC     AND g.home_coach_id <> g.away_coach_id
# MAGIC     AND ch.nation_id IS NOT NULL
# MAGIC     AND ca.nation_id IS NOT NULL
# MAGIC     AND ch.nation_id <> ca.nation_id
# MAGIC ),
# MAGIC
# MAGIC directional_results AS (
# MAGIC   SELECT
# MAGIC     home_nation_id AS nation_id,
# MAGIC     away_nation_id AS opponent_nation_id,
# MAGIC     home_score     AS score_for,
# MAGIC     away_score     AS score_against
# MAGIC   FROM game_base
# MAGIC
# MAGIC   UNION ALL
# MAGIC
# MAGIC   SELECT
# MAGIC     away_nation_id AS nation_id,
# MAGIC     home_nation_id AS opponent_nation_id,
# MAGIC     away_score     AS score_for,
# MAGIC     home_score     AS score_against
# MAGIC   FROM game_base
# MAGIC ),
# MAGIC
# MAGIC result_summary AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     opponent_nation_id,
# MAGIC
# MAGIC     COUNT(*) AS games_played,
# MAGIC
# MAGIC     SUM(CASE WHEN score_for > score_against THEN 1 ELSE 0 END) AS wins,
# MAGIC     SUM(CASE WHEN score_for = score_against THEN 1 ELSE 0 END) AS draws,
# MAGIC     SUM(CASE WHEN score_for < score_against THEN 1 ELSE 0 END) AS losses,
# MAGIC
# MAGIC     AVG(score_for)     AS avg_score_for,
# MAGIC     AVG(score_against) AS avg_score_against,
# MAGIC
# MAGIC     100.0 * SUM(CASE WHEN score_for > score_against THEN 1 ELSE 0 END)
# MAGIC       / NULLIF(COUNT(*), 0) AS win_pct
# MAGIC   FROM directional_results
# MAGIC   GROUP BY nation_id, opponent_nation_id
# MAGIC ),
# MAGIC
# MAGIC -- keep your existing glo_base + glo_summary logic here (unchanged)
# MAGIC glo_base AS (
# MAGIC   SELECT
# MAGIC     cd.nation_id  AS nation_id,
# MAGIC     cdo.nation_id AS opponent_nation_id,
# MAGIC
# MAGIC     rh.rating_delta AS glo_exchange
# MAGIC   FROM naf_catalog.gold_fact.rating_history_fact AS rh
# MAGIC   INNER JOIN naf_catalog.gold_dim.coach_dim AS cd
# MAGIC     ON rh.coach_id = cd.coach_id
# MAGIC   INNER JOIN naf_catalog.gold_dim.coach_dim AS cdo
# MAGIC     ON rh.opponent_coach_id = cdo.coach_id
# MAGIC   WHERE rh.scope = 'GLOBAL'
# MAGIC     AND rh.rating_system = 'NAF_ELO'
# MAGIC     AND COALESCE(rh.race_id, 0) = 0
# MAGIC     AND rh.coach_id IS NOT NULL
# MAGIC     AND rh.opponent_coach_id IS NOT NULL
# MAGIC     AND cd.nation_id IS NOT NULL
# MAGIC     AND cdo.nation_id IS NOT NULL
# MAGIC     AND cd.nation_id <> cdo.nation_id
# MAGIC   QUALIFY ROW_NUMBER() OVER (
# MAGIC     PARTITION BY rh.coach_id, rh.game_id
# MAGIC     ORDER BY rh.event_timestamp DESC, rh.game_index DESC, rh.game_id DESC
# MAGIC   ) = 1
# MAGIC ),
# MAGIC
# MAGIC glo_summary AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     opponent_nation_id,
# MAGIC
# MAGIC     SUM(glo_exchange) AS glo_exchange_total,
# MAGIC     AVG(glo_exchange) AS glo_exchange_mean
# MAGIC   FROM glo_base
# MAGIC   GROUP BY nation_id, opponent_nation_id
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   r.nation_id,
# MAGIC   r.opponent_nation_id,
# MAGIC
# MAGIC   r.games_played,
# MAGIC   r.wins,
# MAGIC   r.draws,
# MAGIC   r.losses,
# MAGIC
# MAGIC   r.avg_score_for,
# MAGIC   r.avg_score_against,
# MAGIC   r.win_pct,
# MAGIC
# MAGIC   g.glo_exchange_total,
# MAGIC   g.glo_exchange_mean,
# MAGIC
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM result_summary r
# MAGIC LEFT JOIN glo_summary g
# MAGIC   ON  r.nation_id          = g.nation_id
# MAGIC   AND r.opponent_nation_id = g.opponent_nation_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TABLE: naf_catalog.gold_summary.nation_rivalry_summary
# MAGIC -- =============================================================================
# MAGIC -- PURPOSE:
# MAGIC --   - Rank-based rivalry scoring between nations (based on coach nationality)
# MAGIC --   - Each nation's opponents ranked by games_played and PPG closeness to 0.5
# MAGIC --   - rivalry_score = (games_rank + closeness_rank) / 2  (lower = stronger rivalry)
# MAGIC -- GRAIN:
# MAGIC --   - One row per (nation_id, opponent_nation_id)
# MAGIC -- SOURCES:
# MAGIC --   - naf_catalog.gold_summary.nation_vs_nation_summary
# MAGIC -- =============================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_summary.nation_rivalry_summary
# MAGIC USING DELTA AS
# MAGIC
# MAGIC WITH base AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     opponent_nation_id,
# MAGIC     CAST(games_played AS BIGINT) AS games_played,
# MAGIC     CAST(avg_score_for AS DOUBLE) AS avg_score_for,
# MAGIC     ABS(CAST(avg_score_for AS DOUBLE) - 0.5) AS ppg_closeness
# MAGIC   FROM naf_catalog.gold_summary.nation_vs_nation_summary
# MAGIC   WHERE nation_id IS NOT NULL
# MAGIC     AND opponent_nation_id IS NOT NULL
# MAGIC     AND nation_id <> opponent_nation_id
# MAGIC     AND nation_id <> 0
# MAGIC     AND opponent_nation_id <> 0
# MAGIC ),
# MAGIC
# MAGIC ranked AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     -- Rank within each nation's set of opponents
# MAGIC     CAST(DENSE_RANK() OVER (PARTITION BY nation_id ORDER BY games_played DESC) AS INT) AS games_rank,
# MAGIC     CAST(DENSE_RANK() OVER (PARTITION BY nation_id ORDER BY ppg_closeness ASC)  AS INT) AS closeness_rank
# MAGIC   FROM base
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   nation_id,
# MAGIC   opponent_nation_id,
# MAGIC   games_played,
# MAGIC   avg_score_for,
# MAGIC   ppg_closeness,
# MAGIC   games_rank,
# MAGIC   closeness_rank,
# MAGIC   ROUND((games_rank + closeness_rank) / 2.0, 1) AS rivalry_score_raw,
# MAGIC   ROUND(100 * (1.0 - PERCENT_RANK() OVER (
# MAGIC     PARTITION BY nation_id
# MAGIC     ORDER BY (games_rank + closeness_rank) / 2.0 ASC
# MAGIC   )), 1) AS rivalry_score,
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM ranked;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TABLE: naf_catalog.gold_summary.nation_elite_rivalry_summary
# MAGIC -- =============================================================================
# MAGIC -- PURPOSE:
# MAGIC --   - Elite rivalry scoring: only games where BOTH coaches have 200+ GLO median.
# MAGIC --   - Same rank-based rivalry_score as nation_rivalry_summary.
# MAGIC --   - Separate table for future "Elite H2H" dashboard widget.
# MAGIC -- GRAIN:
# MAGIC --   - One row per (nation_id, opponent_nation_id)
# MAGIC -- SOURCES:
# MAGIC --   - naf_catalog.gold_fact.games_fact
# MAGIC --   - naf_catalog.gold_dim.coach_dim
# MAGIC --   - naf_catalog.gold_summary.nation_coach_glo_metrics (for GLO median filter)
# MAGIC -- =============================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_summary.nation_elite_rivalry_summary
# MAGIC USING DELTA AS
# MAGIC
# MAGIC WITH elite_games AS (
# MAGIC   SELECT
# MAGIC     g.game_id,
# MAGIC     ch.nation_id  AS home_nation_id,
# MAGIC     ca.nation_id  AS away_nation_id,
# MAGIC     CASE
# MAGIC       WHEN g.td_home > g.td_away THEN 1.0
# MAGIC       WHEN g.td_home = g.td_away THEN 0.5
# MAGIC       ELSE 0.0
# MAGIC     END AS home_score,
# MAGIC     CASE
# MAGIC       WHEN g.td_away > g.td_home THEN 1.0
# MAGIC       WHEN g.td_away = g.td_home THEN 0.5
# MAGIC       ELSE 0.0
# MAGIC     END AS away_score
# MAGIC   FROM naf_catalog.gold_fact.games_fact g
# MAGIC   INNER JOIN naf_catalog.gold_dim.coach_dim ch ON g.home_coach_id = ch.coach_id
# MAGIC   INNER JOIN naf_catalog.gold_dim.coach_dim ca ON g.away_coach_id = ca.coach_id
# MAGIC   INNER JOIN naf_catalog.gold_summary.nation_coach_glo_metrics mh
# MAGIC     ON g.home_coach_id = mh.coach_id AND mh.glo_median >= 200
# MAGIC   INNER JOIN naf_catalog.gold_summary.nation_coach_glo_metrics ma
# MAGIC     ON g.away_coach_id = ma.coach_id AND ma.glo_median >= 200
# MAGIC   WHERE ch.nation_id IS NOT NULL AND ca.nation_id IS NOT NULL
# MAGIC     AND ch.nation_id <> ca.nation_id
# MAGIC     AND ch.nation_id <> 0 AND ca.nation_id <> 0
# MAGIC ),
# MAGIC
# MAGIC directional AS (
# MAGIC   SELECT home_nation_id AS nation_id, away_nation_id AS opponent_nation_id,
# MAGIC          home_score AS score_for, away_score AS score_against FROM elite_games
# MAGIC   UNION ALL
# MAGIC   SELECT away_nation_id, home_nation_id,
# MAGIC          away_score, home_score FROM elite_games
# MAGIC ),
# MAGIC
# MAGIC result_summary AS (
# MAGIC   SELECT
# MAGIC     nation_id, opponent_nation_id,
# MAGIC     COUNT(*) AS games_played,
# MAGIC     SUM(CASE WHEN score_for > score_against THEN 1 ELSE 0 END) AS wins,
# MAGIC     SUM(CASE WHEN score_for = score_against THEN 1 ELSE 0 END) AS draws,
# MAGIC     SUM(CASE WHEN score_for < score_against THEN 1 ELSE 0 END) AS losses,
# MAGIC     AVG(score_for) AS avg_score_for,
# MAGIC     ABS(AVG(score_for) - 0.5) AS ppg_closeness
# MAGIC   FROM directional
# MAGIC   GROUP BY nation_id, opponent_nation_id
# MAGIC ),
# MAGIC
# MAGIC ranked AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     CAST(DENSE_RANK() OVER (PARTITION BY nation_id ORDER BY games_played DESC) AS INT) AS games_rank,
# MAGIC     CAST(DENSE_RANK() OVER (PARTITION BY nation_id ORDER BY ppg_closeness ASC)  AS INT) AS closeness_rank
# MAGIC   FROM result_summary
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   nation_id, opponent_nation_id,
# MAGIC   games_played, wins, draws, losses,
# MAGIC   avg_score_for, ppg_closeness,
# MAGIC   games_rank, closeness_rank,
# MAGIC   ROUND((games_rank + closeness_rank) / 2.0, 1) AS rivalry_score,
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM ranked;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- VIEW: naf_catalog.gold_summary.nation_coach_race_elo_peak
# MAGIC -- =============================================================================
# MAGIC -- PURPOSE:
# MAGIC --   - One row per (coach_id, race_id) with coach nationality (nation_id)
# MAGIC --   - Uses coach_rating_race_summary as the single source of Elo peak truth
# MAGIC -- GRAIN:
# MAGIC --   - (coach_id, race_id)
# MAGIC -- =============================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_summary.nation_coach_race_elo_peak AS
# MAGIC SELECT
# MAGIC   crs.coach_id,
# MAGIC   crs.race_id,
# MAGIC   cd.nation_id,
# MAGIC
# MAGIC   -- Post-threshold peak (already NULL if coach hasn't reached burn-in)
# MAGIC   crs.elo_peak_post_threshold AS elo_peak,
# MAGIC   -- Post-threshold median (career median after burn-in)
# MAGIC   crs.elo_median_post_threshold AS elo_median,
# MAGIC   crs.games_with_race,
# MAGIC   crs.threshold_games,
# MAGIC   (crs.games_with_race >= crs.threshold_games) AS is_valid_elo,
# MAGIC
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM naf_catalog.gold_summary.coach_rating_race_summary crs
# MAGIC INNER JOIN naf_catalog.gold_dim.coach_dim cd
# MAGIC   ON crs.coach_id = cd.coach_id
# MAGIC WHERE cd.nation_id IS NOT NULL
# MAGIC   AND crs.race_id IS NOT NULL
# MAGIC   AND crs.race_id <> 0;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TABLE: naf_catalog.gold_summary.nation_race_elo_peak_summary
# MAGIC -- =============================================================================
# MAGIC -- PURPOSE:
# MAGIC --   - One row per (nation_id, race_id)
# MAGIC --   - Distribution summary of race ELO peak across coaches (stable sample only)
# MAGIC -- =============================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_summary.nation_race_elo_peak_summary
# MAGIC USING DELTA AS
# MAGIC
# MAGIC SELECT
# MAGIC   ncr.nation_id,
# MAGIC   ncr.race_id,
# MAGIC
# MAGIC   CAST(COUNT(DISTINCT ncr.coach_id) AS INT) AS coaches_with_race_count,
# MAGIC
# MAGIC   MIN(ncr.elo_peak)                     AS elo_peak_min,
# MAGIC   AVG(ncr.elo_peak)                     AS elo_peak_mean,
# MAGIC   PERCENTILE_APPROX(ncr.elo_peak, 0.50) AS elo_peak_median,
# MAGIC   PERCENTILE_APPROX(ncr.elo_peak, 0.90) AS elo_peak_p90,
# MAGIC   PERCENTILE_APPROX(ncr.elo_peak, 0.10) AS elo_peak_p10,
# MAGIC   MAX(ncr.elo_peak)                     AS elo_peak_max,
# MAGIC
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM naf_catalog.gold_summary.nation_coach_race_elo_peak AS ncr
# MAGIC WHERE ncr.elo_peak IS NOT NULL
# MAGIC   AND ncr.is_valid_elo = TRUE
# MAGIC GROUP BY
# MAGIC   ncr.nation_id,
# MAGIC   ncr.race_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- VIEW: naf_catalog.gold_summary.world_race_elo_quantiles
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : World-level race Elo peak distribution (all coaches globally).
# MAGIC --                Provides the boxplot baseline for nation vs world race comparisons.
# MAGIC -- LAYER        : GOLD_SUMMARY
# MAGIC -- GRAIN        : 1 row per race_id
# MAGIC -- PRIMARY KEY  : (race_id)
# MAGIC -- SOURCES      : naf_catalog.gold_summary.nation_coach_race_elo_peak
# MAGIC -- NOTES        : PEAK only (post-threshold). Filtered by is_valid_elo (25+ games).
# MAGIC --                Mirrors the pattern of world_glo_metric_quantiles but at race level.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_summary.world_race_elo_quantiles AS
# MAGIC SELECT
# MAGIC   race_id,
# MAGIC   CAST(COUNT(DISTINCT coach_id) AS INT) AS coaches_count,
# MAGIC   MIN(elo_peak)                         AS elo_peak_min,
# MAGIC   PERCENTILE_APPROX(elo_peak, 0.10)     AS elo_peak_p10,
# MAGIC   PERCENTILE_APPROX(elo_peak, 0.25)     AS elo_peak_p25,
# MAGIC   PERCENTILE_APPROX(elo_peak, 0.50)     AS elo_peak_p50,
# MAGIC   PERCENTILE_APPROX(elo_peak, 0.75)     AS elo_peak_p75,
# MAGIC   PERCENTILE_APPROX(elo_peak, 0.90)     AS elo_peak_p90,
# MAGIC   MAX(elo_peak)                         AS elo_peak_max,
# MAGIC   CURRENT_TIMESTAMP()                   AS load_timestamp
# MAGIC FROM naf_catalog.gold_summary.nation_coach_race_elo_peak
# MAGIC WHERE is_valid_elo = TRUE
# MAGIC   AND elo_peak IS NOT NULL
# MAGIC GROUP BY race_id;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 17
# MAGIC %sql
# MAGIC -- =====================================================================
# MAGIC -- SUMMARY: World quantiles (so dashboards don't fake "World" rows)
# MAGIC -- =====================================================================
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_summary.world_glo_metric_quantiles AS
# MAGIC WITH metric_long AS (
# MAGIC   SELECT
# MAGIC     m.coach_id,
# MAGIC     stack(
# MAGIC       3,
# MAGIC       'PEAK',   m.glo_peak,
# MAGIC       'MEAN',   m.glo_mean,
# MAGIC       'MEDIAN', m.glo_median
# MAGIC     ) AS (metric_type, metric_value)
# MAGIC   FROM naf_catalog.gold_summary.nation_coach_glo_metrics m
# MAGIC   WHERE m.is_valid_glo = TRUE
# MAGIC )
# MAGIC SELECT
# MAGIC   metric_type,
# MAGIC   CAST(COUNT(DISTINCT coach_id) AS INT) AS coaches_count,
# MAGIC   MIN(metric_value)                     AS value_min,
# MAGIC   PERCENTILE_APPROX(metric_value, 0.10) AS value_p10,
# MAGIC   PERCENTILE_APPROX(metric_value, 0.25) AS value_p25,
# MAGIC   PERCENTILE_APPROX(metric_value, 0.50) AS value_p50,
# MAGIC   PERCENTILE_APPROX(metric_value, 0.75) AS value_p75,
# MAGIC   PERCENTILE_APPROX(metric_value, 0.90) AS value_p90,
# MAGIC   MAX(metric_value)                     AS value_max,
# MAGIC   CURRENT_TIMESTAMP()                   AS load_timestamp
# MAGIC FROM metric_long
# MAGIC WHERE metric_value IS NOT NULL
# MAGIC GROUP BY metric_type;
# MAGIC
# MAGIC
# MAGIC -- NOTE: Presentation views (nation_glo_metric_quantiles, nation_glo_peak_card_long)
# MAGIC -- are defined in 342_NAF_gold_presentation_nation.py (correct notebook).
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_summary.coach_opponent_glo_bin_summary AS
# MAGIC WITH params AS (
# MAGIC   SELECT
# MAGIC     CAST(100.0 AS DOUBLE) AS min_glo,
# MAGIC     CAST(350.0 AS DOUBLE) AS max_glo,
# MAGIC     CAST(5     AS INT)    AS num_bins,
# MAGIC     CAST((350.0 - 100.0) / 5 AS DOUBLE) AS bin_width
# MAGIC ),
# MAGIC bucketed AS (
# MAGIC   SELECT
# MAGIC     s.coach_id,
# MAGIC     s.opponent_coach_id,
# MAGIC     pks.glo_peak AS opponent_glo_peak,
# MAGIC     s.games_played,
# MAGIC     s.points_total,
# MAGIC     p.min_glo,
# MAGIC     p.max_glo,
# MAGIC     p.num_bins,
# MAGIC     p.bin_width,
# MAGIC     CASE
# MAGIC       WHEN pks.glo_peak <  p.min_glo THEN 0
# MAGIC       WHEN pks.glo_peak >= p.max_glo THEN p.num_bins
# MAGIC       ELSE WIDTH_BUCKET(pks.glo_peak, p.min_glo, p.max_glo, p.num_bins)
# MAGIC     END AS bin_index
# MAGIC   FROM naf_catalog.gold_summary.coach_opponent_summary AS s
# MAGIC   INNER JOIN naf_catalog.gold_summary.nation_coach_glo_metrics AS pks
# MAGIC     ON s.opponent_coach_id = pks.coach_id
# MAGIC     AND pks.is_valid_glo = TRUE
# MAGIC   CROSS JOIN params AS p
# MAGIC ),
# MAGIC agg AS (
# MAGIC   SELECT
# MAGIC     coach_id,
# MAGIC     bin_index,
# MAGIC     MIN(min_glo + (bin_index - 1) * bin_width) AS bin_lower,
# MAGIC     MIN(min_glo + (bin_index)     * bin_width) AS bin_upper,
# MAGIC     COUNT(DISTINCT opponent_coach_id) AS opponents_count,
# MAGIC     SUM(games_played) AS games_played,
# MAGIC     SUM(points_total) AS points_total,
# MAGIC     CASE
# MAGIC       WHEN SUM(games_played) > 0 THEN SUM(points_total) * 1.0 / SUM(games_played)
# MAGIC       ELSE NULL
# MAGIC     END AS win_points_per_game
# MAGIC   FROM bucketed
# MAGIC   WHERE bin_index BETWEEN 1 AND num_bins
# MAGIC   GROUP BY coach_id, bin_index
# MAGIC )
# MAGIC SELECT
# MAGIC   coach_id,
# MAGIC   bin_index,
# MAGIC   bin_lower,
# MAGIC   bin_upper,
# MAGIC   opponents_count,
# MAGIC   games_played,
# MAGIC   points_total,
# MAGIC   win_points_per_game
# MAGIC FROM agg;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Note**: `nation_rivalry_summary` was previously duplicated here as a VIEW that
# MAGIC overwrote the TABLE defined above. The duplicate has been removed.
# MAGIC The TABLE definition (above) now uses the improved two-component closeness formula
# MAGIC (average of win-balance closeness and score-margin closeness).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TABLE: naf_catalog.gold_summary.nation_opponent_elo_bin_wdl
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Nation-aggregate W/D/L by opponent GLO rating bins.
# MAGIC --                Answers: "How does this nation perform against weak/mid/strong opponents?"
# MAGIC --                Supports PEAK and MEDIAN metric types for GLO Metric filter.
# MAGIC -- LAYER        : GOLD_SUMMARY
# MAGIC -- GRAIN        : 1 row per (nation_id, metric_type, bin_scheme_id, bin_index)
# MAGIC -- PRIMARY KEY  : (nation_id, metric_type, bin_scheme_id, bin_index)
# MAGIC -- SOURCES      : naf_catalog.gold_fact.coach_games_fact,
# MAGIC --                naf_catalog.gold_dim.coach_dim,
# MAGIC --                naf_catalog.gold_summary.nation_coach_glo_metrics (opponent GLO),
# MAGIC --                naf_catalog.gold_summary.global_elo_bin_scheme
# MAGIC -- NOTES        : - Excludes intra-nation games (opponent from same nation).
# MAGIC --                - Excludes Unknown nation (nation_id = 0).
# MAGIC --                - Bins by opponent's GLO (peak or median per metric_type).
# MAGIC --                - Uses COALESCE(stable, full-history) GLO so all opponents
# MAGIC --                  with any rating are included (not just is_valid_glo).
# MAGIC --                - Fixed bins: 0-150, 150-200, 200-250, 250-300, 300+.
# MAGIC --                - Spine ensures all bins present per nation+metric (zero-filled).
# MAGIC -- PHASE        : 5
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_summary.nation_opponent_elo_bin_wdl
# MAGIC USING DELTA AS
# MAGIC
# MAGIC -- Fixed bin definitions: 0-150, 150-200, 200-250, 250+
# MAGIC WITH bin_def AS (
# MAGIC   SELECT * FROM (VALUES
# MAGIC     (1, 0.0,    150.0, '0–150'),
# MAGIC     (2, 150.0,  200.0, '150–200'),
# MAGIC     (3, 200.0,  250.0, '200–250'),
# MAGIC     (4, 250.0, 9999.0, '250+')
# MAGIC   ) AS t(bin_index, bin_min, bin_max, bin_label)
# MAGIC ),
# MAGIC
# MAGIC nation_spine AS (
# MAGIC   SELECT DISTINCT nation_id
# MAGIC   FROM naf_catalog.gold_dim.nation_dim
# MAGIC   WHERE nation_id <> 0
# MAGIC ),
# MAGIC
# MAGIC metric_spine AS (
# MAGIC   SELECT EXPLODE(ARRAY('PEAK', 'MEDIAN')) AS metric_type
# MAGIC ),
# MAGIC
# MAGIC full_spine AS (
# MAGIC   SELECT
# MAGIC     ns.nation_id,
# MAGIC     ms.metric_type,
# MAGIC     bd.bin_index,
# MAGIC     bd.bin_min,
# MAGIC     bd.bin_max,
# MAGIC     bd.bin_label
# MAGIC   FROM nation_spine ns
# MAGIC   CROSS JOIN metric_spine ms
# MAGIC   CROSS JOIN bin_def bd
# MAGIC ),
# MAGIC
# MAGIC game_data AS (
# MAGIC   SELECT
# MAGIC     c.nation_id        AS coach_nation_id,
# MAGIC     opp_c.nation_id    AS opponent_nation_id,
# MAGIC     cgf.result_numeric,
# MAGIC     -- Use stable GLO if available, else full-history GLO
# MAGIC     COALESCE(opp_glo.glo_peak, opp_glo.glo_peak_all)     AS opponent_glo_peak,
# MAGIC     COALESCE(opp_glo.glo_median, opp_glo.glo_median_all) AS opponent_glo_median
# MAGIC   FROM naf_catalog.gold_fact.coach_games_fact cgf
# MAGIC   INNER JOIN naf_catalog.gold_dim.coach_dim c
# MAGIC     ON cgf.coach_id = c.coach_id
# MAGIC   INNER JOIN naf_catalog.gold_dim.coach_dim opp_c
# MAGIC     ON cgf.opponent_coach_id = opp_c.coach_id
# MAGIC   INNER JOIN naf_catalog.gold_summary.nation_coach_glo_metrics opp_glo
# MAGIC     ON cgf.opponent_coach_id = opp_glo.coach_id
# MAGIC   WHERE c.nation_id <> 0
# MAGIC     AND opp_c.nation_id <> 0
# MAGIC     AND c.nation_id <> opp_c.nation_id  -- exclude intra-nation
# MAGIC ),
# MAGIC
# MAGIC -- Unpivot: one row per game per metric_type
# MAGIC game_metric AS (
# MAGIC   SELECT
# MAGIC     coach_nation_id,
# MAGIC     result_numeric,
# MAGIC     stack(2,
# MAGIC       'PEAK',   opponent_glo_peak,
# MAGIC       'MEDIAN', opponent_glo_median
# MAGIC     ) AS (metric_type, opponent_glo_value)
# MAGIC   FROM game_data
# MAGIC ),
# MAGIC
# MAGIC bucketed AS (
# MAGIC   SELECT
# MAGIC     gm.coach_nation_id AS nation_id,
# MAGIC     gm.metric_type,
# MAGIC     bd.bin_index,
# MAGIC     gm.result_numeric
# MAGIC   FROM game_metric gm
# MAGIC   INNER JOIN bin_def bd
# MAGIC     ON gm.opponent_glo_value >= bd.bin_min
# MAGIC     AND gm.opponent_glo_value < bd.bin_max
# MAGIC   WHERE gm.opponent_glo_value IS NOT NULL
# MAGIC ),
# MAGIC
# MAGIC agg AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     metric_type,
# MAGIC     bin_index,
# MAGIC     CAST(COUNT(*) AS INT) AS games,
# MAGIC     CAST(SUM(CASE WHEN result_numeric = 1.0 THEN 1 ELSE 0 END) AS INT)   AS wins,
# MAGIC     CAST(SUM(CASE WHEN result_numeric = 0.5 THEN 1 ELSE 0 END) AS INT)   AS draws,
# MAGIC     CAST(SUM(CASE WHEN result_numeric = 0.0 THEN 1 ELSE 0 END) AS INT)   AS losses,
# MAGIC     ROUND(CAST(SUM(result_numeric) AS DOUBLE) / NULLIF(COUNT(*), 0), 3)   AS ppg
# MAGIC   FROM bucketed
# MAGIC   GROUP BY nation_id, metric_type, bin_index
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   sp.nation_id,
# MAGIC   sp.metric_type,
# MAGIC   sp.bin_index,
# MAGIC   sp.bin_min,
# MAGIC   sp.bin_max,
# MAGIC   sp.bin_label,
# MAGIC   COALESCE(a.games, 0)    AS games,
# MAGIC   COALESCE(a.wins, 0)     AS wins,
# MAGIC   COALESCE(a.draws, 0)    AS draws,
# MAGIC   COALESCE(a.losses, 0)   AS losses,
# MAGIC   a.ppg,
# MAGIC   CURRENT_TIMESTAMP()     AS load_timestamp
# MAGIC FROM full_spine sp
# MAGIC LEFT JOIN agg a
# MAGIC   ON sp.nation_id = a.nation_id
# MAGIC   AND sp.metric_type = a.metric_type
# MAGIC   AND sp.bin_index = a.bin_index;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TABLE: naf_catalog.gold_summary.nation_game_quality_bin_wdl
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Nation-aggregate W/D/L by game quality bins.
# MAGIC --                Game quality = average of coach's and opponent's GLO.
# MAGIC --                Answers: "How does this nation do in high-quality vs low-quality matches?"
# MAGIC --                Supports PEAK and MEDIAN metric types for GLO Metric filter.
# MAGIC -- LAYER        : GOLD_SUMMARY
# MAGIC -- GRAIN        : 1 row per (nation_id, metric_type, bin_scheme_id, bin_index)
# MAGIC -- PRIMARY KEY  : (nation_id, metric_type, bin_scheme_id, bin_index)
# MAGIC -- SOURCES      : naf_catalog.gold_fact.coach_games_fact,
# MAGIC --                naf_catalog.gold_dim.coach_dim,
# MAGIC --                naf_catalog.gold_summary.nation_coach_glo_metrics (both sides),
# MAGIC --                naf_catalog.gold_summary.global_elo_bin_scheme
# MAGIC -- NOTES        : - Same bin scheme as opponent bins (same scale, same edges).
# MAGIC --                - Excludes intra-nation games.
# MAGIC --                - Excludes Unknown nation.
# MAGIC --                - Spine ensures all bins present per nation+metric (zero-filled).
# MAGIC --                - Both coach and opponent must have valid GLO.
# MAGIC -- PHASE        : 5
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_summary.nation_game_quality_bin_wdl
# MAGIC USING DELTA AS
# MAGIC
# MAGIC WITH bin_spine AS (
# MAGIC   SELECT
# MAGIC     bin_scheme_id,
# MAGIC     bin_index,
# MAGIC     bin_min_global_elo,
# MAGIC     bin_max_global_elo,
# MAGIC     num_bins
# MAGIC   FROM naf_catalog.gold_summary.global_elo_bin_scheme
# MAGIC ),
# MAGIC
# MAGIC nation_spine AS (
# MAGIC   SELECT DISTINCT nation_id
# MAGIC   FROM naf_catalog.gold_dim.nation_dim
# MAGIC   WHERE nation_id <> 0
# MAGIC ),
# MAGIC
# MAGIC metric_spine AS (
# MAGIC   SELECT EXPLODE(ARRAY('PEAK', 'MEDIAN')) AS metric_type
# MAGIC ),
# MAGIC
# MAGIC full_spine AS (
# MAGIC   SELECT
# MAGIC     ns.nation_id,
# MAGIC     ms.metric_type,
# MAGIC     bs.bin_scheme_id,
# MAGIC     bs.bin_index,
# MAGIC     bs.bin_min_global_elo,
# MAGIC     bs.bin_max_global_elo,
# MAGIC     bs.num_bins
# MAGIC   FROM nation_spine ns
# MAGIC   CROSS JOIN metric_spine ms
# MAGIC   CROSS JOIN bin_spine bs
# MAGIC ),
# MAGIC
# MAGIC game_data AS (
# MAGIC   SELECT
# MAGIC     c.nation_id         AS coach_nation_id,
# MAGIC     opp_c.nation_id     AS opponent_nation_id,
# MAGIC     cgf.result_numeric,
# MAGIC     coach_glo.glo_peak   AS coach_glo_peak,
# MAGIC     coach_glo.glo_median AS coach_glo_median,
# MAGIC     opp_glo.glo_peak     AS opp_glo_peak,
# MAGIC     opp_glo.glo_median   AS opp_glo_median
# MAGIC   FROM naf_catalog.gold_fact.coach_games_fact cgf
# MAGIC   INNER JOIN naf_catalog.gold_dim.coach_dim c
# MAGIC     ON cgf.coach_id = c.coach_id
# MAGIC   INNER JOIN naf_catalog.gold_dim.coach_dim opp_c
# MAGIC     ON cgf.opponent_coach_id = opp_c.coach_id
# MAGIC   INNER JOIN naf_catalog.gold_summary.nation_coach_glo_metrics coach_glo
# MAGIC     ON cgf.coach_id = coach_glo.coach_id
# MAGIC     AND coach_glo.is_valid_glo = TRUE
# MAGIC   INNER JOIN naf_catalog.gold_summary.nation_coach_glo_metrics opp_glo
# MAGIC     ON cgf.opponent_coach_id = opp_glo.coach_id
# MAGIC     AND opp_glo.is_valid_glo = TRUE
# MAGIC   WHERE c.nation_id <> 0
# MAGIC     AND opp_c.nation_id <> 0
# MAGIC     AND c.nation_id <> opp_c.nation_id  -- exclude intra-nation
# MAGIC ),
# MAGIC
# MAGIC -- Unpivot: one row per game per metric_type, compute game quality
# MAGIC game_metric AS (
# MAGIC   SELECT
# MAGIC     coach_nation_id,
# MAGIC     result_numeric,
# MAGIC     stack(2,
# MAGIC       'PEAK',   (coach_glo_peak   + opp_glo_peak)   / 2.0,
# MAGIC       'MEDIAN', (coach_glo_median  + opp_glo_median) / 2.0
# MAGIC     ) AS (metric_type, game_quality)
# MAGIC   FROM game_data
# MAGIC ),
# MAGIC
# MAGIC bucketed AS (
# MAGIC   SELECT
# MAGIC     gm.coach_nation_id AS nation_id,
# MAGIC     gm.metric_type,
# MAGIC     bs.bin_scheme_id,
# MAGIC     bs.bin_index,
# MAGIC     gm.result_numeric
# MAGIC   FROM game_metric gm
# MAGIC   INNER JOIN bin_spine bs
# MAGIC     ON gm.game_quality >= bs.bin_min_global_elo
# MAGIC     AND gm.game_quality < bs.bin_max_global_elo
# MAGIC   WHERE gm.game_quality IS NOT NULL
# MAGIC ),
# MAGIC
# MAGIC agg AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     metric_type,
# MAGIC     bin_scheme_id,
# MAGIC     bin_index,
# MAGIC     CAST(COUNT(*) AS INT) AS games,
# MAGIC     CAST(SUM(CASE WHEN result_numeric = 1.0 THEN 1 ELSE 0 END) AS INT)   AS wins,
# MAGIC     CAST(SUM(CASE WHEN result_numeric = 0.5 THEN 1 ELSE 0 END) AS INT)   AS draws,
# MAGIC     CAST(SUM(CASE WHEN result_numeric = 0.0 THEN 1 ELSE 0 END) AS INT)   AS losses,
# MAGIC     ROUND(CAST(SUM(result_numeric) AS DOUBLE) / NULLIF(COUNT(*), 0), 3)   AS ppg
# MAGIC   FROM bucketed
# MAGIC   GROUP BY nation_id, metric_type, bin_scheme_id, bin_index
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   sp.nation_id,
# MAGIC   sp.metric_type,
# MAGIC   sp.bin_scheme_id,
# MAGIC   sp.bin_index,
# MAGIC   sp.bin_min_global_elo,
# MAGIC   sp.bin_max_global_elo,
# MAGIC   COALESCE(a.games, 0)    AS games,
# MAGIC   COALESCE(a.wins, 0)     AS wins,
# MAGIC   COALESCE(a.draws, 0)    AS draws,
# MAGIC   COALESCE(a.losses, 0)   AS losses,
# MAGIC   a.ppg,
# MAGIC   CURRENT_TIMESTAMP()     AS load_timestamp
# MAGIC FROM full_spine sp
# MAGIC LEFT JOIN agg a
# MAGIC   ON sp.nation_id = a.nation_id
# MAGIC   AND sp.metric_type = a.metric_type
# MAGIC   AND sp.bin_scheme_id = a.bin_scheme_id
# MAGIC   AND sp.bin_index = a.bin_index;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TABLE: naf_catalog.gold_summary.nation_team_candidate_scores
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Rank-based 3-component candidate scoring for national team selection.
# MAGIC --                Components: median GLO, median race ELO, opponent GLO strength.
# MAGIC --                Each component is a DENSE_RANK (1 = best, lower = better).
# MAGIC --                selector_score = weighted average of component ranks (lower = better).
# MAGIC --                Pre-computes 4 selector_focus weighting variants so the dashboard
# MAGIC --                can filter without re-computing.
# MAGIC -- LAYER        : GOLD_SUMMARY
# MAGIC -- GRAIN        : 1 row per (nation_id, selector_focus, coach_id)
# MAGIC -- PRIMARY KEY  : (nation_id, selector_focus, coach_id)
# MAGIC -- SOURCES      : naf_catalog.gold_summary.nation_coach_glo_metrics  (glo_median, avg_opp)
# MAGIC --                naf_catalog.gold_summary.nation_coach_race_elo_peak (race ELO peaks)
# MAGIC --                naf_catalog.gold_summary.coach_form_summary         (supplementary)
# MAGIC --                naf_catalog.gold_summary.coach_race_relative_strength (supplementary)
# MAGIC -- NOTES        : - Eligibility gate: is_valid_glo = TRUE (50+ global games)
# MAGIC --                  AND active (played in current or previous calendar year).
# MAGIC --                - race_elo_median = MEAN of elo_peak across ALL races (from race_dim).
# MAGIC --                  Unplayed or invalid-ELO races count as 150 (starting Elo).
# MAGIC --                - Computes both within-nation AND global rankings:
# MAGIC --                  glo_rank / race_rank / opponent_rank             = within-nation
# MAGIC --                  glo_rank_global / race_rank_global / opponent_rank_global = across all nations
# MAGIC --                - Selector focus weights:
# MAGIC --                  GLO:      50/25/25
# MAGIC --                  RACE:     25/50/25
# MAGIC --                  OPPONENT: 25/25/50
# MAGIC --                  BALANCED: 33.3/33.3/33.3
# MAGIC --                - selector_score = weighted average of ranks (lower = better).
# MAGIC --                  e.g. BALANCED score of 3.0 means average rank of 3rd across components.
# MAGIC --                - Supplementary columns (not in score): form_score, versatility.
# MAGIC -- PHASE        : 6
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_summary.nation_team_candidate_scores
# MAGIC USING DELTA AS
# MAGIC
# MAGIC -- 1. Eligible coaches per nation (valid GLO, active, excluding Unknown nation)
# MAGIC WITH eligible AS (
# MAGIC   SELECT
# MAGIC     m.nation_id,
# MAGIC     m.coach_id,
# MAGIC     m.glo_peak,
# MAGIC     m.glo_median,
# MAGIC     COALESCE(m.avg_opponent_glo_peak, 0.0) AS avg_opponent_glo
# MAGIC   FROM naf_catalog.gold_summary.nation_coach_glo_metrics AS m
# MAGIC   INNER JOIN naf_catalog.gold_summary.coach_performance_summary cps
# MAGIC     ON m.coach_id = cps.coach_id
# MAGIC   WHERE m.is_valid_glo = TRUE
# MAGIC     AND m.nation_id <> 0
# MAGIC     -- Activity filter: played in current or previous calendar year
# MAGIC     AND CAST(cps.date_last_game_id / 10000 AS INT) >= YEAR(CURRENT_DATE()) - 1
# MAGIC ),
# MAGIC
# MAGIC -- 2. Mean race ELO per coach across ALL races (unplayed/invalid races count as 150)
# MAGIC race_elo_data AS (
# MAGIC   SELECT
# MAGIC     e.coach_id,
# MAGIC     ROUND(AVG(
# MAGIC       CASE WHEN ncr.is_valid_elo = TRUE AND ncr.elo_peak IS NOT NULL
# MAGIC            THEN ncr.elo_peak
# MAGIC            ELSE 150.0
# MAGIC       END
# MAGIC     ), 1) AS race_elo_median
# MAGIC   FROM eligible e
# MAGIC   CROSS JOIN (
# MAGIC     SELECT race_id FROM naf_catalog.gold_dim.race_dim WHERE race_id <> 0
# MAGIC   ) all_races
# MAGIC   LEFT JOIN naf_catalog.gold_summary.nation_coach_race_elo_peak ncr
# MAGIC     ON e.coach_id = ncr.coach_id
# MAGIC     AND all_races.race_id = ncr.race_id
# MAGIC   GROUP BY e.coach_id
# MAGIC ),
# MAGIC
# MAGIC -- 3. Supplementary: form score (display only, not in selector)
# MAGIC form_data AS (
# MAGIC   SELECT coach_id, form_score
# MAGIC   FROM naf_catalog.gold_summary.coach_form_summary
# MAGIC ),
# MAGIC
# MAGIC -- 4. Supplementary: versatility (display only, not in selector)
# MAGIC versatility_data AS (
# MAGIC   SELECT
# MAGIC     coach_id,
# MAGIC     CAST(COUNT(*) AS INT)                                                      AS races_played_eligible,
# MAGIC     CAST(SUM(CASE WHEN elo_peak >= world_median_elo THEN 1 ELSE 0 END) AS INT) AS races_above_world_median
# MAGIC   FROM naf_catalog.gold_summary.coach_race_relative_strength
# MAGIC   GROUP BY coach_id
# MAGIC ),
# MAGIC
# MAGIC -- 5. Combine: 3 core components + supplementary columns
# MAGIC combined AS (
# MAGIC   SELECT
# MAGIC     e.nation_id,
# MAGIC     e.coach_id,
# MAGIC     e.glo_peak,
# MAGIC     e.glo_median,
# MAGIC     COALESCE(r.race_elo_median, 0.0)      AS race_elo_median,
# MAGIC     e.avg_opponent_glo,
# MAGIC     COALESCE(f.form_score, 0.0)            AS form_score,
# MAGIC     COALESCE(v.races_above_world_median, 0) AS races_above_world_median,
# MAGIC     COALESCE(v.races_played_eligible, 0)    AS races_played_eligible
# MAGIC   FROM eligible e
# MAGIC   LEFT JOIN race_elo_data r    ON e.coach_id = r.coach_id
# MAGIC   LEFT JOIN form_data f        ON e.coach_id = f.coach_id
# MAGIC   LEFT JOIN versatility_data v ON e.coach_id = v.coach_id
# MAGIC ),
# MAGIC
# MAGIC -- 6. Within-nation AND global DENSE_RANK for the 3 core components (1 = best, lower = better)
# MAGIC ranked AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     -- Within-nation ranks (for team selection within a nation)
# MAGIC     CAST(DENSE_RANK() OVER (PARTITION BY nation_id ORDER BY glo_median        DESC) AS INT) AS glo_rank,
# MAGIC     CAST(DENSE_RANK() OVER (PARTITION BY nation_id ORDER BY race_elo_median   DESC) AS INT) AS race_rank,
# MAGIC     CAST(DENSE_RANK() OVER (PARTITION BY nation_id ORDER BY avg_opponent_glo  DESC) AS INT) AS opponent_rank,
# MAGIC     -- Global ranks (for cross-nation power ranking)
# MAGIC     CAST(DENSE_RANK() OVER (ORDER BY glo_median        DESC) AS INT) AS glo_rank_global,
# MAGIC     CAST(DENSE_RANK() OVER (ORDER BY race_elo_median   DESC) AS INT) AS race_rank_global,
# MAGIC     CAST(DENSE_RANK() OVER (ORDER BY avg_opponent_glo  DESC) AS INT) AS opponent_rank_global
# MAGIC   FROM combined
# MAGIC ),
# MAGIC
# MAGIC -- 7. Focus weighting spine
# MAGIC focus_weights AS (
# MAGIC   SELECT * FROM (VALUES
# MAGIC     ('GLO',      0.50, 0.25, 0.25),
# MAGIC     ('RACE',     0.25, 0.50, 0.25),
# MAGIC     ('OPPONENT', 0.25, 0.25, 0.50),
# MAGIC     ('BALANCED', 1.0/3, 1.0/3, 1.0/3)
# MAGIC   ) AS t(selector_focus, w_glo, w_race, w_opponent)
# MAGIC ),
# MAGIC
# MAGIC -- 8. Cross join ranked × focus to produce 4 variants per coach
# MAGIC --    selector_score = weighted average of ranks (lower = better)
# MAGIC selector AS (
# MAGIC   SELECT
# MAGIC     r.nation_id,
# MAGIC     fw.selector_focus,
# MAGIC     r.coach_id,
# MAGIC     r.glo_peak,
# MAGIC     r.glo_median,
# MAGIC     r.race_elo_median,
# MAGIC     r.avg_opponent_glo,
# MAGIC     r.form_score,
# MAGIC     r.races_above_world_median,
# MAGIC     r.races_played_eligible,
# MAGIC     -- Within-nation ranks + weighted score
# MAGIC     r.glo_rank,
# MAGIC     r.race_rank,
# MAGIC     r.opponent_rank,
# MAGIC     ROUND(fw.w_glo * r.glo_rank + fw.w_race * r.race_rank + fw.w_opponent * r.opponent_rank, 2) AS selector_score_national_raw,
# MAGIC     -- Global ranks + weighted score
# MAGIC     r.glo_rank_global,
# MAGIC     r.race_rank_global,
# MAGIC     r.opponent_rank_global,
# MAGIC     ROUND(fw.w_glo * r.glo_rank_global + fw.w_race * r.race_rank_global + fw.w_opponent * r.opponent_rank_global, 2) AS selector_score_global_raw
# MAGIC   FROM ranked r
# MAGIC   CROSS JOIN focus_weights fw
# MAGIC ),
# MAGIC
# MAGIC -- 9. Normalise selector scores to 0–100 (100 = best) using PERCENT_RANK
# MAGIC normalised AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     -- National normalised score (within nation+focus)
# MAGIC     ROUND(100 * (1.0 - PERCENT_RANK() OVER (
# MAGIC       PARTITION BY nation_id, selector_focus
# MAGIC       ORDER BY selector_score_national_raw ASC
# MAGIC     )), 1) AS selector_score_national,
# MAGIC     -- Global normalised score (across all nations, per focus)
# MAGIC     ROUND(100 * (1.0 - PERCENT_RANK() OVER (
# MAGIC       PARTITION BY selector_focus
# MAGIC       ORDER BY selector_score_global_raw ASC
# MAGIC     )), 1) AS selector_score_global
# MAGIC   FROM selector
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   nation_id,
# MAGIC   selector_focus,
# MAGIC   coach_id,
# MAGIC   glo_peak,
# MAGIC   glo_median,
# MAGIC   race_elo_median,
# MAGIC   avg_opponent_glo,
# MAGIC   form_score,
# MAGIC   races_above_world_median,
# MAGIC   races_played_eligible,
# MAGIC   glo_rank,
# MAGIC   race_rank,
# MAGIC   opponent_rank,
# MAGIC   selector_score_national,
# MAGIC   CAST(DENSE_RANK() OVER (PARTITION BY nation_id, selector_focus ORDER BY selector_score_national DESC) AS INT) AS selector_rank_national,
# MAGIC   glo_rank_global,
# MAGIC   race_rank_global,
# MAGIC   opponent_rank_global,
# MAGIC   selector_score_global,
# MAGIC   CAST(DENSE_RANK() OVER (PARTITION BY nation_id, selector_focus ORDER BY selector_score_global DESC) AS INT) AS selector_rank_global,
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM normalised;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TABLE: naf_catalog.gold_summary.nation_power_ranking
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE      : Ranks nations by the average GLOBAL selector_score of their top 8 candidates.
# MAGIC --                Uses selector_score_global (rank-based, lower = better).
# MAGIC --                Answers: "Which nation could field the strongest team?"
# MAGIC --                Pre-computes per selector_focus variant (GLO/RACE/OPPONENT/BALANCED).
# MAGIC -- LAYER        : GOLD_SUMMARY
# MAGIC -- GRAIN        : 1 row per (nation_id, selector_focus)
# MAGIC -- PRIMARY KEY  : (nation_id, selector_focus)
# MAGIC -- SOURCES      : naf_catalog.gold_summary.nation_team_candidate_scores
# MAGIC -- NOTES        : - "Top 8" = coaches ranked by selector_score_global DESC (higher = better, normalised 0–100).
# MAGIC --                - Nations with fewer than 8 eligible coaches are EXCLUDED.
# MAGIC --                - power_rank: DENSE_RANK by top_8_avg_selector_score_global DESC per focus.
# MAGIC -- PHASE        : 6
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_summary.nation_power_ranking
# MAGIC USING DELTA AS
# MAGIC
# MAGIC -- Rank candidates within each nation by global selector score (ASC = lower rank is better)
# MAGIC WITH ranked AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     selector_focus,
# MAGIC     coach_id,
# MAGIC     selector_score_national,
# MAGIC     selector_score_global,
# MAGIC     glo_peak,
# MAGIC     glo_median,
# MAGIC     DENSE_RANK() OVER (
# MAGIC       PARTITION BY nation_id, selector_focus
# MAGIC       ORDER BY selector_score_global DESC
# MAGIC     ) AS global_rank_within_nation
# MAGIC   FROM naf_catalog.gold_summary.nation_team_candidate_scores
# MAGIC ),
# MAGIC
# MAGIC top_candidates AS (
# MAGIC   SELECT *
# MAGIC   FROM ranked
# MAGIC   WHERE global_rank_within_nation <= 8
# MAGIC ),
# MAGIC
# MAGIC agg AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     selector_focus,
# MAGIC     CAST(COUNT(*)            AS INT)          AS coaches_in_top_8,
# MAGIC     ROUND(AVG(selector_score_national), 2)     AS top_8_avg_selector_score_national,
# MAGIC     ROUND(AVG(selector_score_global), 2)      AS top_8_avg_selector_score_global,
# MAGIC     ROUND(AVG(glo_peak), 1)                   AS top_8_avg_glo_peak,
# MAGIC     ROUND(AVG(glo_median), 1)                 AS top_8_avg_glo_median
# MAGIC   FROM top_candidates
# MAGIC   GROUP BY nation_id, selector_focus
# MAGIC ),
# MAGIC
# MAGIC eligible_counts AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     CAST(COUNT(*) / 4 AS INT) AS coaches_eligible  -- divide by 4 focus variants
# MAGIC   FROM naf_catalog.gold_summary.nation_team_candidate_scores
# MAGIC   GROUP BY nation_id
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   a.nation_id,
# MAGIC   a.selector_focus,
# MAGIC   a.coaches_in_top_8,
# MAGIC   a.top_8_avg_selector_score_national,
# MAGIC   a.top_8_avg_selector_score_global,
# MAGIC   a.top_8_avg_glo_peak,
# MAGIC   a.top_8_avg_glo_median,
# MAGIC   ec.coaches_eligible,
# MAGIC   CAST(DENSE_RANK() OVER (PARTITION BY a.selector_focus ORDER BY a.top_8_avg_selector_score_global DESC) AS INT) AS power_rank,
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM agg a
# MAGIC LEFT JOIN eligible_counts ec
# MAGIC   ON a.nation_id = ec.nation_id
# MAGIC WHERE ec.coaches_eligible >= 8;
