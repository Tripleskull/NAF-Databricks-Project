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
# MAGIC --   - Rivalry scoring between nations (based on coach nationality)
# MAGIC --   - Directional: rivalry_score is "for nation_id vs opponent_nation_id"
# MAGIC -- GRAIN:
# MAGIC --   - One row per (nation_id, opponent_nation_id)
# MAGIC -- SOURCES:
# MAGIC --   - naf_catalog.gold_summary.nation_vs_nation_summary
# MAGIC -- =============================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_summary.nation_rivalry_summary
# MAGIC USING DELTA AS
# MAGIC
# MAGIC WITH params AS (
# MAGIC   SELECT
# MAGIC     min_games_nation_race  AS min_games,
# MAGIC     rivalry_games_cap      AS games_cap,
# MAGIC     rivalry_w_games        AS w_games,
# MAGIC     rivalry_w_closeness    AS w_close,
# MAGIC     rivalry_w_share        AS w_share
# MAGIC   FROM naf_catalog.gold_dim.analytical_config
# MAGIC ),
# MAGIC
# MAGIC base AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     opponent_nation_id,
# MAGIC     CAST(games_played AS BIGINT) AS games_played,
# MAGIC     CASE
# MAGIC       WHEN win_pct IS NULL THEN NULL
# MAGIC       WHEN win_pct > 1 THEN CAST(win_pct AS DOUBLE) / 100.0
# MAGIC       ELSE CAST(win_pct AS DOUBLE)
# MAGIC     END AS win_pct,
# MAGIC     CAST(avg_score_for     AS DOUBLE) AS avg_score_for,
# MAGIC     CAST(avg_score_against AS DOUBLE) AS avg_score_against,
# MAGIC     CAST(glo_exchange_total AS DOUBLE) AS glo_exchange_total
# MAGIC   FROM naf_catalog.gold_summary.nation_vs_nation_summary
# MAGIC   WHERE nation_id IS NOT NULL
# MAGIC     AND opponent_nation_id IS NOT NULL
# MAGIC     AND nation_id <> opponent_nation_id
# MAGIC ),
# MAGIC
# MAGIC filtered AS (
# MAGIC   SELECT b.*
# MAGIC   FROM base b
# MAGIC   CROSS JOIN params p
# MAGIC   WHERE b.games_played >= p.min_games
# MAGIC ),
# MAGIC
# MAGIC totals AS (
# MAGIC   SELECT
# MAGIC     nation_id,
# MAGIC     SUM(games_played) AS total_games
# MAGIC   FROM filtered
# MAGIC   GROUP BY nation_id
# MAGIC ),
# MAGIC
# MAGIC scored AS (
# MAGIC   SELECT
# MAGIC     b.nation_id,
# MAGIC     b.opponent_nation_id,
# MAGIC     b.games_played,
# MAGIC     b.win_pct,
# MAGIC     b.avg_score_for,
# MAGIC     b.avg_score_against,
# MAGIC     (b.avg_score_for - b.avg_score_against) AS avg_score_diff,
# MAGIC     b.glo_exchange_total,
# MAGIC
# MAGIC     t.total_games,
# MAGIC
# MAGIC     -- Volume score: fraction of cap, clamped to [0, 1]
# MAGIC     LEAST(b.games_played / p.games_cap, 1.0) AS games_score,
# MAGIC
# MAGIC     -- Closeness: average of win-balance closeness and score-margin closeness, both [0, 1]
# MAGIC     ( GREATEST(0.0, LEAST(1.0, 1.0 - (ABS(b.win_pct - 0.5) / 0.5)))
# MAGIC     + GREATEST(0.0, LEAST(1.0, 1.0 - LEAST(ABS(b.avg_score_for - b.avg_score_against), 1.0)))
# MAGIC     ) / 2.0 AS closeness_score,
# MAGIC
# MAGIC     CASE
# MAGIC       WHEN t.total_games > 0 THEN (b.games_played * 1.0) / t.total_games
# MAGIC       ELSE NULL
# MAGIC     END AS games_share
# MAGIC   FROM filtered b
# MAGIC   LEFT JOIN totals t ON b.nation_id = t.nation_id
# MAGIC   CROSS JOIN params p
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   nation_id,
# MAGIC   opponent_nation_id,
# MAGIC   games_played,
# MAGIC   win_pct,
# MAGIC   avg_score_for,
# MAGIC   avg_score_against,
# MAGIC   avg_score_diff,
# MAGIC   glo_exchange_total,
# MAGIC   games_score,
# MAGIC   closeness_score,
# MAGIC   games_share,
# MAGIC   (w_games * games_score) + (w_close * closeness_score) + (w_share * games_share) AS rivalry_score,
# MAGIC   CURRENT_TIMESTAMP() AS load_timestamp
# MAGIC FROM scored
# MAGIC CROSS JOIN params;
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
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_summary.world_glo_metric_quantiles AS
# MAGIC WITH metric_long AS (
# MAGIC   SELECT
# MAGIC     coach_id,
# MAGIC     stack(
# MAGIC       3,
# MAGIC       'PEAK',   glo_peak,
# MAGIC       'MEAN',   glo_mean,
# MAGIC       'MEDIAN', glo_median
# MAGIC     ) AS (metric_type, metric_value)
# MAGIC   FROM naf_catalog.gold_summary.nation_coach_glo_metrics
# MAGIC   WHERE is_valid_glo = TRUE
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

# COMMAND ----------

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
# MAGIC -- =====================================================================
# MAGIC -- PRESENTATION: Nation + World quantiles (joins nation_dim for display)
# MAGIC -- =====================================================================
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.nation_glo_metric_quantiles AS
# MAGIC SELECT
# MAGIC   q.nation_id,
# MAGIC   n.nation_name,
# MAGIC   n.nation_name_display,
# MAGIC   q.metric_type,
# MAGIC   q.coaches_count,
# MAGIC   q.value_min,
# MAGIC   q.value_p10,
# MAGIC   q.value_p25,
# MAGIC   q.value_p50,
# MAGIC   q.value_p75,
# MAGIC   q.value_p90,
# MAGIC   q.value_max,
# MAGIC   q.load_timestamp
# MAGIC FROM naf_catalog.gold_summary.nation_glo_metric_quantiles q
# MAGIC LEFT JOIN naf_catalog.gold_dim.nation_dim n
# MAGIC   ON q.nation_id = n.nation_id
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   CAST(NULL AS INT) AS nation_id,
# MAGIC   'World'           AS nation_name,
# MAGIC   'World'           AS nation_name_display,
# MAGIC   w.metric_type,
# MAGIC   w.coaches_count,
# MAGIC   w.value_min,
# MAGIC   w.value_p10,
# MAGIC   w.value_p25,
# MAGIC   w.value_p50,
# MAGIC   w.value_p75,
# MAGIC   w.value_p90,
# MAGIC   w.value_max,
# MAGIC   w.load_timestamp
# MAGIC FROM naf_catalog.gold_summary.world_glo_metric_quantiles w;
# MAGIC
# MAGIC -- =====================================================================
# MAGIC -- PRESENTATION: Boxplot-ready stats (canonical columns, no weird aliases)
# MAGIC -- =====================================================================
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.nation_glo_peak_boxplot AS
# MAGIC SELECT
# MAGIC   nation_id,
# MAGIC   nation_name,
# MAGIC   nation_name_display,
# MAGIC   coaches_count,
# MAGIC   value_min,
# MAGIC   value_p25,
# MAGIC   value_p50,
# MAGIC   value_p75,
# MAGIC   value_max,
# MAGIC   load_timestamp
# MAGIC FROM naf_catalog.gold_presentation.nation_glo_metric_quantiles
# MAGIC WHERE metric_type = 'PEAK';
# MAGIC
# MAGIC -- =====================================================================
# MAGIC -- PRESENTATION: Long-format "card" metrics for PEAK (no dashboard unpivot)
# MAGIC -- =====================================================================
# MAGIC CREATE OR REPLACE VIEW naf_catalog.gold_presentation.nation_glo_peak_card_long AS
# MAGIC SELECT
# MAGIC   nation_id,
# MAGIC   nation_name,
# MAGIC   nation_name_display,
# MAGIC   metric,
# MAGIC   metric_sort,
# MAGIC   value,
# MAGIC   load_timestamp
# MAGIC FROM (
# MAGIC   SELECT nation_id, nation_name, nation_name_display, 'Median Peak GLO' AS metric, 1 AS metric_sort, value_p50 AS value, load_timestamp
# MAGIC   FROM naf_catalog.gold_presentation.nation_glo_metric_quantiles WHERE metric_type = 'PEAK'
# MAGIC   UNION ALL
# MAGIC   SELECT nation_id, nation_name, nation_name_display, '25th Percentile GLO', 2, value_p25, load_timestamp
# MAGIC   FROM naf_catalog.gold_presentation.nation_glo_metric_quantiles WHERE metric_type = 'PEAK'
# MAGIC   UNION ALL
# MAGIC   SELECT nation_id, nation_name, nation_name_display, '75th Percentile GLO', 3, value_p75, load_timestamp
# MAGIC   FROM naf_catalog.gold_presentation.nation_glo_metric_quantiles WHERE metric_type = 'PEAK'
# MAGIC   UNION ALL
# MAGIC   SELECT nation_id, nation_name, nation_name_display, 'IQR (P75-P25)', 4, (value_p75 - value_p25), load_timestamp
# MAGIC   FROM naf_catalog.gold_presentation.nation_glo_metric_quantiles WHERE metric_type = 'PEAK'
# MAGIC   UNION ALL
# MAGIC   SELECT nation_id, nation_name, nation_name_display, 'Number of Coaches', 5, CAST(coaches_count AS DOUBLE), load_timestamp
# MAGIC   FROM naf_catalog.gold_presentation.nation_glo_metric_quantiles WHERE metric_type = 'PEAK'
# MAGIC   UNION ALL
# MAGIC   SELECT nation_id, nation_name, nation_name_display, 'Highest Peak GLO', 6, value_max, load_timestamp
# MAGIC   FROM naf_catalog.gold_presentation.nation_glo_metric_quantiles WHERE metric_type = 'PEAK'
# MAGIC ) x;
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
