# Databricks notebook source
# MAGIC %sql
# MAGIC -- =====================================================================
# MAGIC -- NAF MODEL TEST SCRIPT (Core + optional Coach Dashboard objects)
# MAGIC -- =====================================================================
# MAGIC -- PURPOSE  : Contract + integrity tests for Silver / Gold_dim / Gold_fact,
# MAGIC --            plus existence + metadata checks for Gold_summary / Gold_presentation.
# MAGIC -- OUTPUT   : Returns ONLY failing checks (fail_rows > 0). Empty result = PASS.
# MAGIC --
# MAGIC -- POLICIES ENFORCED
# MAGIC -- - Unknown nation uses nation_id = 0 (single row, labeled 'Unknown')
# MAGIC -- - silver.coaches_clean is authoritative (games must have both coaches present)
# MAGIC -- - tournaments with no games can be excluded, but tournaments must cover games_clean tournament_ids
# MAGIC -- - Gold_fact maintains FK integrity to Gold_dim
# MAGIC -- =====================================================================
# MAGIC
# MAGIC WITH params AS (
# MAGIC   SELECT array(1, 13, 15) AS allowed_variant_ids
# MAGIC ),
# MAGIC
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- 0) Existence checks (won't error even if objects aren't built yet)
# MAGIC -- ---------------------------------------------------------------------
# MAGIC required_objects AS (
# MAGIC   SELECT * FROM VALUES
# MAGIC     ('silver',           'nations_entity',         'CORE', 'TABLE'),
# MAGIC     ('silver',           'coaches_clean',          'CORE', 'TABLE'),
# MAGIC     ('silver',           'games_clean',            'CORE', 'TABLE'),
# MAGIC     ('silver',           'tournaments_clean',      'CORE', 'TABLE'),
# MAGIC
# MAGIC     ('gold_dim',         'nation_dim',             'CORE', 'TABLE'),
# MAGIC     ('gold_dim',         'coach_dim',              'CORE', 'TABLE'),
# MAGIC     ('gold_dim',         'tournament_dim',         'CORE', 'TABLE'),
# MAGIC     ('gold_dim',         'race_dim',               'CORE', 'TABLE'),
# MAGIC     ('gold_dim',         'date_dim',               'CORE', 'TABLE'),
# MAGIC
# MAGIC     ('gold_fact',        'games_fact',             'CORE', 'TABLE'),
# MAGIC     ('gold_fact',        'coach_games_fact',       'CORE', 'TABLE'),
# MAGIC     ('gold_fact',        'rating_history_fact',    'CORE', 'TABLE'),
# MAGIC
# MAGIC     -- Optional (coach dashboard). Marked PLAN so it won't block core readiness.
# MAGIC     ('gold_summary',     'coach_game_spine_v',                     'PLAN', 'VIEW'),
# MAGIC     ('gold_summary',     'coach_performance_summary',              'PLAN', 'TABLE'),
# MAGIC     ('gold_summary',     'coach_race_summary',                     'PLAN', 'TABLE'),
# MAGIC     ('gold_summary',     'coach_streak_detail',                  'PLAN', 'TABLE'),
# MAGIC     ('gold_summary',     'coach_streak_overview',                   'PLAN', 'TABLE'),
# MAGIC     ('gold_summary',     'coach_tournament_performance_summary',   'PLAN', 'TABLE'),
# MAGIC     ('gold_summary',     'coach_form_summary',                     'PLAN', 'TABLE'),
# MAGIC     ('gold_summary',     'coach_opponent_median_glo_bin_summary',  'PLAN', 'TABLE'),
# MAGIC
# MAGIC     ('gold_presentation','coach_profile',                          'PLAN', 'VIEW'),
# MAGIC     ('gold_presentation','coach_race_performance',                 'PLAN', 'VIEW'),
# MAGIC     ('gold_presentation','coach_tournament_performance',           'PLAN', 'VIEW'),
# MAGIC     ('gold_presentation','coach_opponent_median_glo_bin_results_long', 'PLAN', 'VIEW')
# MAGIC   AS required_objects(table_schema, table_name, tier, expected_kind)
# MAGIC ),
# MAGIC object_existence AS (
# MAGIC   SELECT
# MAGIC     CASE WHEN r.tier = 'CORE' THEN 'ERROR' ELSE 'WARN' END AS severity,
# MAGIC     CONCAT('object_exists: ', r.table_schema, '.', r.table_name) AS check_name,
# MAGIC     CAST(CASE WHEN t.table_name IS NULL THEN 1 ELSE 0 END AS BIGINT) AS fail_rows
# MAGIC   FROM required_objects r
# MAGIC   LEFT JOIN naf_catalog.information_schema.tables t
# MAGIC     ON t.table_schema = r.table_schema
# MAGIC    AND t.table_name   = r.table_name
# MAGIC ),
# MAGIC
# MAGIC -- ---------------------------------------------------------------------
# MAGIC -- 1) Core data integrity checks (assumes core objects exist)
# MAGIC -- ---------------------------------------------------------------------
# MAGIC checks AS (
# MAGIC
# MAGIC   -- Silver: nations_entity (Unknown sentinel)
# MAGIC   SELECT 'ERROR' AS severity,
# MAGIC          'silver.nations_entity: nation_id=0 row count != 1' AS check_name,
# MAGIC          CAST(ABS(COUNT(*) - 1) AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.silver.nations_entity
# MAGIC   WHERE nation_id = 0
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR' AS severity,
# MAGIC          'silver.nations_entity: nation_id=0 row not labeled Unknown' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.silver.nations_entity
# MAGIC   WHERE nation_id = 0
# MAGIC     AND NOT (LOWER(nation_name) = 'unknown' AND LOWER(nation_name_display) = 'unknown')
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR' AS severity,
# MAGIC          'silver.nations_entity: nation_id duplicates' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM (
# MAGIC     SELECT nation_id
# MAGIC     FROM naf_catalog.silver.nations_entity
# MAGIC     GROUP BY nation_id
# MAGIC     HAVING COUNT(*) > 1
# MAGIC   )
# MAGIC
# MAGIC   -- Silver: coaches_clean (authoritative + FK to nations)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR' AS severity,
# MAGIC          'silver.coaches_clean: PK duplicates (coach_id)' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM (
# MAGIC     SELECT coach_id
# MAGIC     FROM naf_catalog.silver.coaches_clean
# MAGIC     GROUP BY coach_id
# MAGIC     HAVING COUNT(*) > 1
# MAGIC   )
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR' AS severity,
# MAGIC          'silver.coaches_clean: orphan nation_id' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.silver.coaches_clean c
# MAGIC   LEFT JOIN naf_catalog.silver.nations_entity n
# MAGIC     ON c.nation_id = n.nation_id
# MAGIC   WHERE n.nation_id IS NULL
# MAGIC
# MAGIC   -- Silver: games_clean (PK + required keys + authoritative coaches + domain rules)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR' AS severity,
# MAGIC          'silver.games_clean: PK duplicates (game_id)' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM (
# MAGIC     SELECT game_id
# MAGIC     FROM naf_catalog.silver.games_clean
# MAGIC     GROUP BY game_id
# MAGIC     HAVING COUNT(*) > 1
# MAGIC   )
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR' AS severity,
# MAGIC          'silver.games_clean: required key(s) NULL' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.silver.games_clean
# MAGIC   WHERE game_id IS NULL
# MAGIC      OR tournament_id IS NULL
# MAGIC      OR variant_id IS NULL
# MAGIC      OR date_id IS NULL
# MAGIC      OR home_coach_id IS NULL
# MAGIC      OR away_coach_id IS NULL
# MAGIC      OR home_race_id IS NULL
# MAGIC      OR away_race_id IS NULL
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR' AS severity,
# MAGIC          'silver.games_clean: variant_id not in allowed set (1,13)' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.silver.games_clean g
# MAGIC   CROSS JOIN params p
# MAGIC   WHERE array_contains(p.allowed_variant_ids, g.variant_id) = FALSE
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR' AS severity,
# MAGIC          'silver.games_clean: self-play (home_coach_id = away_coach_id)' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.silver.games_clean
# MAGIC   WHERE home_coach_id = away_coach_id
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR' AS severity,
# MAGIC          'silver.games_clean: race_id = 0 used in games (not allowed)' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.silver.games_clean
# MAGIC   WHERE home_race_id = 0 OR away_race_id = 0
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR' AS severity,
# MAGIC          'silver.games_clean: home/away coach_id not in silver.coaches_clean (authoritative)' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.silver.games_clean g
# MAGIC   LEFT JOIN naf_catalog.silver.coaches_clean ch
# MAGIC     ON g.home_coach_id = ch.coach_id
# MAGIC   LEFT JOIN naf_catalog.silver.coaches_clean ca
# MAGIC     ON g.away_coach_id = ca.coach_id
# MAGIC   WHERE ch.coach_id IS NULL OR ca.coach_id IS NULL
# MAGIC
# MAGIC   -- Silver: tournaments_clean (PK + policy: tournaments exactly = tournaments-with-games)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR' AS severity,
# MAGIC          'silver.tournaments_clean: PK duplicates (tournament_id)' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM (
# MAGIC     SELECT tournament_id
# MAGIC     FROM naf_catalog.silver.tournaments_clean
# MAGIC     GROUP BY tournament_id
# MAGIC     HAVING COUNT(*) > 1
# MAGIC   )
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR' AS severity,
# MAGIC          'silver.tournaments_clean: is_major_tournament is NULL (must be NOT NULL)' AS check_name,
# MAGIC          CAST(SUM(CASE WHEN is_major_tournament IS NULL THEN 1 ELSE 0 END) AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.silver.tournaments_clean
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR' AS severity,
# MAGIC          'silver.tournaments_clean: tournament_ids missing for games_clean' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM (
# MAGIC     SELECT DISTINCT tournament_id
# MAGIC     FROM naf_catalog.silver.games_clean
# MAGIC     WHERE tournament_id IS NOT NULL
# MAGIC     EXCEPT
# MAGIC     SELECT DISTINCT tournament_id
# MAGIC     FROM naf_catalog.silver.tournaments_clean
# MAGIC     WHERE tournament_id IS NOT NULL
# MAGIC   )
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR' AS severity,
# MAGIC          'silver.tournaments_clean: contains tournaments with no games (should be excluded)' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM (
# MAGIC     SELECT DISTINCT tournament_id
# MAGIC     FROM naf_catalog.silver.tournaments_clean
# MAGIC     WHERE tournament_id IS NOT NULL
# MAGIC     EXCEPT
# MAGIC     SELECT DISTINCT tournament_id
# MAGIC     FROM naf_catalog.silver.games_clean
# MAGIC     WHERE tournament_id IS NOT NULL
# MAGIC   )
# MAGIC
# MAGIC   -- Gold_dim: required sentinels + NOT NULLs + basic FK integrity
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR' AS severity,
# MAGIC          'gold_dim.nation_dim: missing nation_id=0 row' AS check_name,
# MAGIC          CAST(CASE WHEN SUM(CASE WHEN nation_id = 0 THEN 1 ELSE 0 END) = 1 THEN 0 ELSE 1 END AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.gold_dim.nation_dim
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR' AS severity,
# MAGIC          'gold_dim.race_dim: missing race_id=0 row' AS check_name,
# MAGIC          CAST(CASE WHEN SUM(CASE WHEN race_id = 0 THEN 1 ELSE 0 END) = 1 THEN 0 ELSE 1 END AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.gold_dim.race_dim
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR' AS severity,
# MAGIC          'gold_dim.coach_dim: orphan nation_id' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.gold_dim.coach_dim c
# MAGIC   LEFT JOIN naf_catalog.gold_dim.nation_dim n
# MAGIC     ON c.nation_id = n.nation_id
# MAGIC   WHERE n.nation_id IS NULL
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR' AS severity,
# MAGIC          'gold_dim.tournament_dim: is_major_tournament is NULL (must be NOT NULL)' AS check_name,
# MAGIC          CAST(SUM(CASE WHEN is_major_tournament IS NULL THEN 1 ELSE 0 END) AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.gold_dim.tournament_dim
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR' AS severity,
# MAGIC          'gold_dim.tournament_dim: missing tournaments referenced by silver.games_clean' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM (
# MAGIC     SELECT DISTINCT g.tournament_id
# MAGIC     FROM naf_catalog.silver.games_clean g
# MAGIC     LEFT JOIN naf_catalog.gold_dim.tournament_dim t
# MAGIC       ON g.tournament_id = t.tournament_id
# MAGIC     WHERE g.tournament_id IS NOT NULL
# MAGIC       AND t.tournament_id IS NULL
# MAGIC   )
# MAGIC
# MAGIC   -- Gold_fact: games_fact PK + FKs
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR' AS severity,
# MAGIC          'gold_fact.games_fact: PK duplicates (game_id)' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM (
# MAGIC     SELECT game_id
# MAGIC     FROM naf_catalog.gold_fact.games_fact
# MAGIC     GROUP BY game_id
# MAGIC     HAVING COUNT(*) > 1
# MAGIC   )
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR' AS severity,
# MAGIC          'gold_fact.games_fact: orphan tournament_id' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.gold_fact.games_fact g
# MAGIC   LEFT JOIN naf_catalog.gold_dim.tournament_dim t
# MAGIC     ON g.tournament_id = t.tournament_id
# MAGIC   WHERE t.tournament_id IS NULL
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR' AS severity,
# MAGIC          'gold_fact.games_fact: orphan home/away coach_id' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.gold_fact.games_fact g
# MAGIC   LEFT JOIN naf_catalog.gold_dim.coach_dim ch
# MAGIC     ON g.home_coach_id = ch.coach_id
# MAGIC   LEFT JOIN naf_catalog.gold_dim.coach_dim ca
# MAGIC     ON g.away_coach_id = ca.coach_id
# MAGIC   WHERE ch.coach_id IS NULL OR ca.coach_id IS NULL
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR' AS severity,
# MAGIC          'gold_fact.games_fact: orphan date_id' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.gold_fact.games_fact g
# MAGIC   LEFT JOIN naf_catalog.gold_dim.date_dim d
# MAGIC     ON g.date_id = d.date_id
# MAGIC   WHERE d.date_id IS NULL
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR' AS severity,
# MAGIC          'gold_fact.games_fact: orphan home/away race_id' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.gold_fact.games_fact g
# MAGIC   LEFT JOIN naf_catalog.gold_dim.race_dim rh
# MAGIC     ON g.home_race_id = rh.race_id
# MAGIC   LEFT JOIN naf_catalog.gold_dim.race_dim ra
# MAGIC     ON g.away_race_id = ra.race_id
# MAGIC   WHERE rh.race_id IS NULL OR ra.race_id IS NULL
# MAGIC
# MAGIC   -- Gold_fact: coach_games_fact sanity
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR' AS severity,
# MAGIC          'gold_fact.coach_games_fact: PK duplicates (game_id, coach_id, home_away)' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM (
# MAGIC     SELECT game_id, coach_id, home_away
# MAGIC     FROM naf_catalog.gold_fact.coach_games_fact
# MAGIC     GROUP BY game_id, coach_id, home_away
# MAGIC     HAVING COUNT(*) > 1
# MAGIC   )
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR' AS severity,
# MAGIC          'gold_fact.coach_games_fact: home_away not in (H,A)' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.gold_fact.coach_games_fact
# MAGIC   WHERE home_away NOT IN ('H','A') OR home_away IS NULL
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR' AS severity,
# MAGIC          'gold_fact.coach_games_fact: result_numeric not in (0,0.5,1)' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.gold_fact.coach_games_fact
# MAGIC   WHERE result_numeric NOT IN (0.0, 0.5, 1.0) OR result_numeric IS NULL
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR' AS severity,
# MAGIC          'gold_fact.coach_games_fact: rowcount != 2x games_fact' AS check_name,
# MAGIC          CAST(ABS(
# MAGIC            (SELECT COUNT(*) FROM naf_catalog.gold_fact.coach_games_fact)
# MAGIC            - 2 * (SELECT COUNT(*) FROM naf_catalog.gold_fact.games_fact)
# MAGIC          ) AS BIGINT) AS fail_rows
# MAGIC
# MAGIC   -- Gold_fact: rating_history_fact rules
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR' AS severity,
# MAGIC          'gold_fact.rating_history_fact: PK duplicates (scope,rating_system,game_id,coach_id,race_id)' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM (
# MAGIC     SELECT scope, rating_system, game_id, coach_id, race_id
# MAGIC     FROM naf_catalog.gold_fact.rating_history_fact
# MAGIC     GROUP BY scope, rating_system, game_id, coach_id, race_id
# MAGIC     HAVING COUNT(*) > 1
# MAGIC   )
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR' AS severity,
# MAGIC          'gold_fact.rating_history_fact: GLOBAL scope with race_id != 0' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.gold_fact.rating_history_fact
# MAGIC   WHERE scope = 'GLOBAL' AND race_id <> 0
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR' AS severity,
# MAGIC          'gold_fact.rating_history_fact: RACE scope with race_id = 0' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.gold_fact.rating_history_fact
# MAGIC   WHERE scope = 'RACE' AND race_id = 0
# MAGIC
# MAGIC   -- Gold_summary: metadata-only checks (won't error if tables missing)
# MAGIC   UNION ALL
# MAGIC   SELECT 'WARN' AS severity,
# MAGIC          'gold_summary: forbidden display columns present (coach_name/race_name/nation_name_display/flag_code/tournament_name)' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.information_schema.columns
# MAGIC   WHERE table_schema = 'gold_summary'
# MAGIC     AND column_name IN ('coach_name','race_name','nation_name_display','flag_code','tournament_name')
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'WARN' AS severity,
# MAGIC          'gold_summary: game_date column present (should use date_id)' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.information_schema.columns
# MAGIC   WHERE table_schema = 'gold_summary'
# MAGIC     AND column_name = 'game_date'
# MAGIC
# MAGIC )
# MAGIC
# MAGIC SELECT *
# MAGIC FROM (
# MAGIC   SELECT * FROM object_existence
# MAGIC   UNION ALL
# MAGIC   SELECT * FROM checks
# MAGIC )
# MAGIC WHERE fail_rows <> 0
# MAGIC ORDER BY severity DESC, check_name;

# COMMAND ----------

# DBTITLE 1,Coach pipeline PK/FK checks
# MAGIC %sql
# MAGIC -- =====================================================================
# MAGIC -- COACH PIPELINE: PK UNIQUENESS + FK COVERAGE
# MAGIC -- =====================================================================
# MAGIC -- Returns ONLY failing checks. Empty result = all pass.
# MAGIC
# MAGIC -- PK: coach_performance_summary (coach_id)
# MAGIC SELECT 'coach_performance_summary: PK duplicates' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM (SELECT coach_id FROM naf_catalog.gold_summary.coach_performance_summary GROUP BY coach_id HAVING COUNT(*) > 1)
# MAGIC UNION ALL
# MAGIC -- PK: coach_rating_global_elo_summary (rating_system, coach_id)
# MAGIC SELECT 'coach_rating_global_elo_summary: PK duplicates' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM (SELECT rating_system, coach_id FROM naf_catalog.gold_summary.coach_rating_global_elo_summary GROUP BY rating_system, coach_id HAVING COUNT(*) > 1)
# MAGIC UNION ALL
# MAGIC -- PK: coach_rating_race_summary (rating_system, coach_id, race_id)
# MAGIC SELECT 'coach_rating_race_summary: PK duplicates' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM (SELECT rating_system, coach_id, race_id FROM naf_catalog.gold_summary.coach_rating_race_summary GROUP BY rating_system, coach_id, race_id HAVING COUNT(*) > 1)
# MAGIC UNION ALL
# MAGIC -- PK: coach_race_summary (rating_system, coach_id, race_id)
# MAGIC SELECT 'coach_race_summary: PK duplicates' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM (SELECT rating_system, coach_id, race_id FROM naf_catalog.gold_summary.coach_race_summary GROUP BY rating_system, coach_id, race_id HAVING COUNT(*) > 1)
# MAGIC UNION ALL
# MAGIC -- PK: coach_opponent_summary (rating_system, coach_id, opponent_coach_id)
# MAGIC SELECT 'coach_opponent_summary: PK duplicates' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM (SELECT rating_system, coach_id, opponent_coach_id FROM naf_catalog.gold_summary.coach_opponent_summary GROUP BY rating_system, coach_id, opponent_coach_id HAVING COUNT(*) > 1)
# MAGIC UNION ALL
# MAGIC -- PK: coach_streak_overview (scope, coach_id, scope_race_id)
# MAGIC SELECT 'coach_streak_overview: PK duplicates' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM (SELECT scope, coach_id, scope_race_id FROM naf_catalog.gold_summary.coach_streak_overview GROUP BY scope, coach_id, scope_race_id HAVING COUNT(*) > 1)
# MAGIC UNION ALL
# MAGIC -- PK: coach_tournament_performance_summary (rating_system, coach_id, tournament_id)
# MAGIC SELECT 'coach_tournament_performance_summary: PK duplicates' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM (SELECT rating_system, coach_id, tournament_id FROM naf_catalog.gold_summary.coach_tournament_performance_summary GROUP BY rating_system, coach_id, tournament_id HAVING COUNT(*) > 1)
# MAGIC UNION ALL
# MAGIC -- PK: coach_profile (coach_id)
# MAGIC SELECT 'coach_profile: PK duplicates' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM (SELECT coach_id FROM naf_catalog.gold_presentation.coach_profile GROUP BY coach_id HAVING COUNT(*) > 1)
# MAGIC UNION ALL
# MAGIC -- FK: coach_performance_summary.coach_id → coach_dim
# MAGIC SELECT 'coach_performance_summary: orphan coach_id' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.coach_performance_summary s
# MAGIC LEFT JOIN naf_catalog.gold_dim.coach_dim d ON s.coach_id = d.coach_id
# MAGIC WHERE d.coach_id IS NULL
# MAGIC UNION ALL
# MAGIC -- FK: coach_rating_global_elo_summary.coach_id → coach_dim
# MAGIC SELECT 'coach_rating_global_elo_summary: orphan coach_id' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.coach_rating_global_elo_summary s
# MAGIC LEFT JOIN naf_catalog.gold_dim.coach_dim d ON s.coach_id = d.coach_id
# MAGIC WHERE d.coach_id IS NULL
# MAGIC
# MAGIC HAVING fail_rows > 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =====================================================================
# MAGIC -- PHASE 1: COACH FORM SUMMARY + OPPONENT STRENGTH TESTS
# MAGIC -- =====================================================================
# MAGIC -- Returns ONLY failing checks. Empty result = all pass.
# MAGIC
# MAGIC -- PK: coach_form_summary (coach_id)
# MAGIC SELECT 'coach_form_summary: PK duplicates' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM (SELECT coach_id FROM naf_catalog.gold_summary.coach_form_summary GROUP BY coach_id HAVING COUNT(*) > 1)
# MAGIC UNION ALL
# MAGIC -- Non-empty check
# MAGIC SELECT 'coach_form_summary: table is empty' AS check_name, CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.coach_form_summary
# MAGIC UNION ALL
# MAGIC -- Form score range: warn if any outside [-20, +20] (generous bound)
# MAGIC SELECT 'coach_form_summary: form_score outside [-20, +20]' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.coach_form_summary
# MAGIC WHERE form_score IS NOT NULL AND (form_score < -20 OR form_score > 20)
# MAGIC UNION ALL
# MAGIC -- Percentile range: form_pctl must be 0–100 for eligible coaches
# MAGIC SELECT 'coach_form_summary: form_pctl outside [0, 100]' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.coach_form_summary
# MAGIC WHERE form_pctl IS NOT NULL AND (form_pctl < 0 OR form_pctl > 100)
# MAGIC UNION ALL
# MAGIC -- Label consistency: non-NULL pctl must have non-NULL label
# MAGIC SELECT 'coach_form_summary: form_pctl set but form_label NULL' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.coach_form_summary
# MAGIC WHERE form_pctl IS NOT NULL AND form_label IS NULL
# MAGIC UNION ALL
# MAGIC -- Label consistency: non-NULL label must have non-NULL pctl
# MAGIC SELECT 'coach_form_summary: form_label set but form_pctl NULL' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.coach_form_summary
# MAGIC WHERE form_label IS NOT NULL AND form_pctl IS NULL
# MAGIC UNION ALL
# MAGIC -- Opponent strength: avg_opponent_glo_peak should be between 100 and 400 for coaches with 10+ games
# MAGIC SELECT 'coach_performance_summary: avg_opponent_glo_peak outside [100, 400] for 10+ game coaches' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.coach_performance_summary
# MAGIC WHERE games_played >= 10
# MAGIC   AND avg_opponent_glo_peak IS NOT NULL
# MAGIC   AND (avg_opponent_glo_peak < 100 OR avg_opponent_glo_peak > 400)
# MAGIC UNION ALL
# MAGIC -- Opponent strength last 50: should also be in reasonable range
# MAGIC SELECT 'coach_performance_summary: avg_opponent_glo_peak_last_50 outside [100, 400] for 50+ game coaches' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.coach_performance_summary
# MAGIC WHERE games_played >= 50
# MAGIC   AND avg_opponent_glo_peak_last_50 IS NOT NULL
# MAGIC   AND (avg_opponent_glo_peak_last_50 < 100 OR avg_opponent_glo_peak_last_50 > 400)
# MAGIC UNION ALL
# MAGIC -- FK: coach_form_summary.coach_id → coach_dim
# MAGIC SELECT 'coach_form_summary: orphan coach_id' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.coach_form_summary s
# MAGIC LEFT JOIN naf_catalog.gold_dim.coach_dim d ON s.coach_id = d.coach_id
# MAGIC WHERE d.coach_id IS NULL
# MAGIC
# MAGIC HAVING fail_rows > 0;

# COMMAND ----------

# DBTITLE 1,Nation pipeline PK/FK/consistency checks
# MAGIC %sql
# MAGIC -- =====================================================================
# MAGIC -- NATION PIPELINE: PK UNIQUENESS + FK COVERAGE + CONSISTENCY
# MAGIC -- =====================================================================
# MAGIC -- Returns ONLY failing checks. Empty result = all pass.
# MAGIC
# MAGIC -- PK: nation_coach_glo_metrics (nation_id, coach_id)
# MAGIC SELECT 'nation_coach_glo_metrics: PK duplicates' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM (SELECT nation_id, coach_id FROM naf_catalog.gold_summary.nation_coach_glo_metrics GROUP BY nation_id, coach_id HAVING COUNT(*) > 1)
# MAGIC UNION ALL
# MAGIC -- PK: nation_overview_summary (nation_id)
# MAGIC SELECT 'nation_overview_summary: PK duplicates' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM (SELECT nation_id FROM naf_catalog.gold_summary.nation_overview_summary GROUP BY nation_id HAVING COUNT(*) > 1)
# MAGIC UNION ALL
# MAGIC -- PK: nation_race_summary (nation_id, race_id)
# MAGIC SELECT 'nation_race_summary: PK duplicates' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM (SELECT nation_id, race_id FROM naf_catalog.gold_summary.nation_race_summary GROUP BY nation_id, race_id HAVING COUNT(*) > 1)
# MAGIC UNION ALL
# MAGIC -- PK: nation_vs_nation_summary (nation_id, opponent_nation_id)
# MAGIC SELECT 'nation_vs_nation_summary: PK duplicates' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM (SELECT nation_id, opponent_nation_id FROM naf_catalog.gold_summary.nation_vs_nation_summary GROUP BY nation_id, opponent_nation_id HAVING COUNT(*) > 1)
# MAGIC UNION ALL
# MAGIC -- PK: nation_rivalry_summary (nation_id, opponent_nation_id)
# MAGIC SELECT 'nation_rivalry_summary: PK duplicates' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM (SELECT nation_id, opponent_nation_id FROM naf_catalog.gold_summary.nation_rivalry_summary GROUP BY nation_id, opponent_nation_id HAVING COUNT(*) > 1)
# MAGIC UNION ALL
# MAGIC -- PK: nation_glo_metric_quantiles (nation_id, metric_type)
# MAGIC SELECT 'nation_glo_metric_quantiles: PK duplicates' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM (SELECT nation_id, metric_type FROM naf_catalog.gold_summary.nation_glo_metric_quantiles GROUP BY nation_id, metric_type HAVING COUNT(*) > 1)
# MAGIC UNION ALL
# MAGIC -- PK: nation_race_elo_peak_summary (nation_id, race_id)
# MAGIC SELECT 'nation_race_elo_peak_summary: PK duplicates' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM (SELECT nation_id, race_id FROM naf_catalog.gold_summary.nation_race_elo_peak_summary GROUP BY nation_id, race_id HAVING COUNT(*) > 1)
# MAGIC UNION ALL
# MAGIC -- REMOVED: nation_overview_comparison_summary (table dropped — redundant with nation_overview_comparison)
# MAGIC -- FK: nation_coach_glo_metrics.nation_id → nation_dim
# MAGIC SELECT 'nation_coach_glo_metrics: orphan nation_id' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.nation_coach_glo_metrics s
# MAGIC LEFT JOIN naf_catalog.gold_dim.nation_dim d ON s.nation_id = d.nation_id
# MAGIC WHERE d.nation_id IS NULL
# MAGIC UNION ALL
# MAGIC -- FK: nation_coach_glo_metrics.coach_id → coach_dim
# MAGIC SELECT 'nation_coach_glo_metrics: orphan coach_id' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.nation_coach_glo_metrics s
# MAGIC LEFT JOIN naf_catalog.gold_dim.coach_dim d ON s.coach_id = d.coach_id
# MAGIC WHERE d.coach_id IS NULL
# MAGIC UNION ALL
# MAGIC -- Consistency: metric_type values should be uppercase only
# MAGIC SELECT 'nation_glo_metric_quantiles: non-uppercase metric_type' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.nation_glo_metric_quantiles
# MAGIC WHERE metric_type <> UPPER(metric_type)
# MAGIC UNION ALL
# MAGIC -- Consistency: rivalry_summary should not be a VIEW overwriting a TABLE
# MAGIC -- (verify it's a TABLE by checking information_schema)
# MAGIC SELECT 'nation_rivalry_summary: should be TABLE not VIEW' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.information_schema.tables
# MAGIC WHERE table_schema = 'gold_summary'
# MAGIC   AND table_name = 'nation_rivalry_summary'
# MAGIC   AND table_type = 'VIEW'
# MAGIC
# MAGIC HAVING fail_rows > 0;

# COMMAND ----------

# DBTITLE 1,Nation cumulative WDL series tests
# MAGIC %sql
# MAGIC -- =====================================================================
# MAGIC -- PHASE 2: NATION CUMULATIVE WDL SERIES TESTS
# MAGIC -- =====================================================================
# MAGIC -- Returns ONLY failing checks. Empty result = all pass.
# MAGIC
# MAGIC -- PK: nation_results_cumulative_series (nation_id, game_sequence_number)
# MAGIC SELECT 'nation_results_cumulative_series: PK duplicates' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM (SELECT nation_id, game_sequence_number FROM naf_catalog.gold_summary.nation_results_cumulative_series GROUP BY nation_id, game_sequence_number HAVING COUNT(*) > 1)
# MAGIC UNION ALL
# MAGIC -- Non-empty check
# MAGIC SELECT 'nation_results_cumulative_series: table is empty' AS check_name, CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.nation_results_cumulative_series
# MAGIC UNION ALL
# MAGIC -- Cumulative consistency: cum_wins + cum_draws + cum_losses = cum_games
# MAGIC SELECT 'nation_results_cumulative_series: cum_wins+draws+losses != cum_games' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.nation_results_cumulative_series
# MAGIC WHERE (cum_wins + cum_draws + cum_losses) <> cum_games
# MAGIC UNION ALL
# MAGIC -- No intra-nation games (opponent_nation_id must differ from nation_id)
# MAGIC SELECT 'nation_results_cumulative_series: intra-nation game found' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.nation_results_cumulative_series
# MAGIC WHERE opponent_nation_id = nation_id
# MAGIC UNION ALL
# MAGIC -- No Unknown nation rows
# MAGIC SELECT 'nation_results_cumulative_series: Unknown nation_id=0 found' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.nation_results_cumulative_series
# MAGIC WHERE nation_id = 0 OR opponent_nation_id = 0
# MAGIC UNION ALL
# MAGIC -- cum_win_frac between 0 and 1
# MAGIC SELECT 'nation_results_cumulative_series: cum_win_frac outside [0,1]' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.nation_results_cumulative_series
# MAGIC WHERE cum_win_frac IS NOT NULL AND (cum_win_frac < 0 OR cum_win_frac > 1)
# MAGIC UNION ALL
# MAGIC -- cum_points_frac between 0 and 1
# MAGIC SELECT 'nation_results_cumulative_series: cum_points_frac outside [0,1]' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.nation_results_cumulative_series
# MAGIC WHERE cum_points_frac IS NOT NULL AND (cum_points_frac < 0 OR cum_points_frac > 1)
# MAGIC UNION ALL
# MAGIC -- Sequence starts at 1 for each nation
# MAGIC SELECT 'nation_results_cumulative_series: min sequence != 1 for some nation' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM (SELECT nation_id, MIN(game_sequence_number) AS min_seq FROM naf_catalog.gold_summary.nation_results_cumulative_series GROUP BY nation_id HAVING MIN(game_sequence_number) <> 1)
# MAGIC
# MAGIC HAVING fail_rows > 0;

# COMMAND ----------

# DBTITLE 1,Race strength comparison tests
# MAGIC %sql
# MAGIC -- =====================================================================
# MAGIC -- PHASE 3: RACE STRENGTH COMPARISON TESTS
# MAGIC -- =====================================================================
# MAGIC -- Returns ONLY failing checks. Empty result = all pass.
# MAGIC
# MAGIC -- Non-empty: world_race_elo_quantiles
# MAGIC SELECT 'world_race_elo_quantiles: view is empty' AS check_name, CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.world_race_elo_quantiles
# MAGIC UNION ALL
# MAGIC -- PK: world_race_elo_quantiles (race_id)
# MAGIC SELECT 'world_race_elo_quantiles: PK duplicates' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM (SELECT race_id FROM naf_catalog.gold_summary.world_race_elo_quantiles GROUP BY race_id HAVING COUNT(*) > 1)
# MAGIC UNION ALL
# MAGIC -- Quantile ordering: p10 <= p25 <= p50 <= p75 <= p90
# MAGIC SELECT 'world_race_elo_quantiles: quantile ordering violated' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.world_race_elo_quantiles
# MAGIC WHERE elo_peak_p10 > elo_peak_p25
# MAGIC    OR elo_peak_p25 > elo_peak_p50
# MAGIC    OR elo_peak_p50 > elo_peak_p75
# MAGIC    OR elo_peak_p75 > elo_peak_p90
# MAGIC UNION ALL
# MAGIC -- Non-empty: nation_race_strength_comparison
# MAGIC SELECT 'nation_race_strength_comparison: view is empty' AS check_name, CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS fail_rows
# MAGIC FROM naf_catalog.gold_presentation.nation_race_strength_comparison
# MAGIC UNION ALL
# MAGIC -- Both scopes present
# MAGIC SELECT 'nation_race_strength_comparison: missing NATION scope' AS check_name, CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS fail_rows
# MAGIC FROM naf_catalog.gold_presentation.nation_race_strength_comparison WHERE scope = 'NATION'
# MAGIC UNION ALL
# MAGIC SELECT 'nation_race_strength_comparison: missing WORLD scope' AS check_name, CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS fail_rows
# MAGIC FROM naf_catalog.gold_presentation.nation_race_strength_comparison WHERE scope = 'WORLD'
# MAGIC
# MAGIC HAVING fail_rows > 0;

# COMMAND ----------

# DBTITLE 1,Phase 4: Home/Abroad + Specialist Badges tests
# MAGIC %sql
# MAGIC -- =====================================================================
# MAGIC -- PHASE 4: HOME/ABROAD PERFORMANCE + SPECIALIST BADGES TESTS
# MAGIC -- =====================================================================
# MAGIC -- Returns ONLY failing checks. Empty result = all pass.
# MAGIC
# MAGIC -- PK: nation_domestic_summary (nation_id)
# MAGIC SELECT 'nation_domestic_summary: PK duplicates' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM (SELECT nation_id FROM naf_catalog.gold_summary.nation_domestic_summary GROUP BY nation_id HAVING COUNT(*) > 1)
# MAGIC UNION ALL
# MAGIC -- Non-empty check
# MAGIC SELECT 'nation_domestic_summary: table is empty' AS check_name, CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.nation_domestic_summary
# MAGIC UNION ALL
# MAGIC -- FK: nation_id → nation_dim
# MAGIC SELECT 'nation_domestic_summary: orphan nation_id' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.nation_domestic_summary s
# MAGIC LEFT JOIN naf_catalog.gold_dim.nation_dim d ON s.nation_id = d.nation_id
# MAGIC WHERE d.nation_id IS NULL
# MAGIC UNION ALL
# MAGIC -- Win fractions between 0-1
# MAGIC SELECT 'nation_domestic_summary: win_frac_home outside [0,1]' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.nation_domestic_summary
# MAGIC WHERE win_frac_home IS NOT NULL AND (win_frac_home < 0 OR win_frac_home > 1)
# MAGIC UNION ALL
# MAGIC SELECT 'nation_domestic_summary: win_frac_away outside [0,1]' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.nation_domestic_summary
# MAGIC WHERE win_frac_away IS NOT NULL AND (win_frac_away < 0 OR win_frac_away > 1)
# MAGIC UNION ALL
# MAGIC SELECT 'nation_domestic_summary: win_frac_vs_domestic outside [0,1]' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.nation_domestic_summary
# MAGIC WHERE win_frac_vs_domestic IS NOT NULL AND (win_frac_vs_domestic < 0 OR win_frac_vs_domestic > 1)
# MAGIC UNION ALL
# MAGIC SELECT 'nation_domestic_summary: win_frac_vs_foreign outside [0,1]' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.nation_domestic_summary
# MAGIC WHERE win_frac_vs_foreign IS NOT NULL AND (win_frac_vs_foreign < 0 OR win_frac_vs_foreign > 1)
# MAGIC UNION ALL
# MAGIC -- No Unknown nation
# MAGIC SELECT 'nation_domestic_summary: Unknown nation_id=0 found' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.nation_domestic_summary
# MAGIC WHERE nation_id = 0
# MAGIC UNION ALL
# MAGIC -- PK: coach_race_relative_strength (coach_id, race_id)
# MAGIC SELECT 'coach_race_relative_strength: PK duplicates' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM (SELECT coach_id, race_id FROM naf_catalog.gold_summary.coach_race_relative_strength GROUP BY coach_id, race_id HAVING COUNT(*) > 1)
# MAGIC UNION ALL
# MAGIC -- Non-empty check
# MAGIC SELECT 'coach_race_relative_strength: table is empty' AS check_name, CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.coach_race_relative_strength
# MAGIC UNION ALL
# MAGIC -- FK: coach_id → coach_dim
# MAGIC SELECT 'coach_race_relative_strength: orphan coach_id' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.coach_race_relative_strength s
# MAGIC LEFT JOIN naf_catalog.gold_dim.coach_dim d ON s.coach_id = d.coach_id
# MAGIC WHERE d.coach_id IS NULL
# MAGIC UNION ALL
# MAGIC -- FK: race_id → race_dim
# MAGIC SELECT 'coach_race_relative_strength: orphan race_id' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.coach_race_relative_strength s
# MAGIC LEFT JOIN naf_catalog.gold_dim.race_dim d ON s.race_id = d.race_id
# MAGIC WHERE d.race_id IS NULL
# MAGIC UNION ALL
# MAGIC -- World percentile between 0-100
# MAGIC SELECT 'coach_race_relative_strength: world_percentile outside [0,100]' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.coach_race_relative_strength
# MAGIC WHERE world_percentile IS NOT NULL AND (world_percentile < 0 OR world_percentile > 100)
# MAGIC UNION ALL
# MAGIC -- Specialist flag logic: must meet all criteria
# MAGIC SELECT 'coach_race_relative_strength: world_specialist=TRUE but criteria not met' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.coach_race_relative_strength
# MAGIC WHERE is_world_specialist = TRUE
# MAGIC   AND NOT (world_percentile >= 98.0 AND elo_peak >= world_median_elo AND games_with_race >= 25)
# MAGIC UNION ALL
# MAGIC -- Minimum games threshold
# MAGIC SELECT 'coach_race_relative_strength: games_with_race < 25' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.coach_race_relative_strength
# MAGIC WHERE games_with_race < 25
# MAGIC UNION ALL
# MAGIC -- PK: coach_race_nation_rank (coach_id, race_id)
# MAGIC SELECT 'coach_race_nation_rank: PK duplicates' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM (SELECT coach_id, race_id FROM naf_catalog.gold_summary.coach_race_nation_rank GROUP BY coach_id, race_id HAVING COUNT(*) > 1)
# MAGIC UNION ALL
# MAGIC -- Non-empty check
# MAGIC SELECT 'coach_race_nation_rank: table is empty' AS check_name, CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.coach_race_nation_rank
# MAGIC UNION ALL
# MAGIC -- FK: coach_id → coach_dim
# MAGIC SELECT 'coach_race_nation_rank: orphan coach_id' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.coach_race_nation_rank s
# MAGIC LEFT JOIN naf_catalog.gold_dim.coach_dim d ON s.coach_id = d.coach_id
# MAGIC WHERE d.coach_id IS NULL
# MAGIC UNION ALL
# MAGIC -- FK: race_id → race_dim
# MAGIC SELECT 'coach_race_nation_rank: orphan race_id' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.coach_race_nation_rank s
# MAGIC LEFT JOIN naf_catalog.gold_dim.race_dim d ON s.race_id = d.race_id
# MAGIC WHERE d.race_id IS NULL
# MAGIC UNION ALL
# MAGIC -- FK: nation_id → nation_dim
# MAGIC SELECT 'coach_race_nation_rank: orphan nation_id' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.coach_race_nation_rank s
# MAGIC LEFT JOIN naf_catalog.gold_dim.nation_dim d ON s.nation_id = d.nation_id
# MAGIC WHERE d.nation_id IS NULL
# MAGIC UNION ALL
# MAGIC -- Nation rank >= 1
# MAGIC SELECT 'coach_race_nation_rank: nation_rank < 1' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.coach_race_nation_rank
# MAGIC WHERE nation_rank < 1
# MAGIC UNION ALL
# MAGIC -- No Unknown nation
# MAGIC SELECT 'coach_race_nation_rank: Unknown nation_id=0 found' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.coach_race_nation_rank
# MAGIC WHERE nation_id = 0
# MAGIC UNION ALL
# MAGIC -- National specialist flag logic: must meet all criteria
# MAGIC SELECT 'coach_race_nation_rank: national_specialist=TRUE but criteria not met' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.coach_race_nation_rank
# MAGIC WHERE is_national_specialist = TRUE
# MAGIC   AND NOT (nation_rank <= 3 AND elo_peak >= world_median_elo AND games_with_race >= 25)
# MAGIC UNION ALL
# MAGIC -- Minimum games threshold
# MAGIC SELECT 'coach_race_nation_rank: games_with_race < 25' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.coach_race_nation_rank
# MAGIC WHERE games_with_race < 25
# MAGIC
# MAGIC HAVING fail_rows > 0;

# COMMAND ----------

# DBTITLE 1,Phase 5: Opponent-adjusted win rates tests
# MAGIC %sql
# MAGIC -- =====================================================================
# MAGIC -- PHASE 5: OPPONENT-ADJUSTED WIN RATES (BIN WDL) TESTS
# MAGIC -- =====================================================================
# MAGIC -- Returns ONLY failing checks. Empty result = all pass.
# MAGIC
# MAGIC -- PK: nation_opponent_elo_bin_wdl (nation_id, metric_type, bin_index) — fixed bins, no bin_scheme_id
# MAGIC SELECT 'nation_opponent_elo_bin_wdl: PK duplicates' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM (SELECT nation_id, metric_type, bin_index FROM naf_catalog.gold_summary.nation_opponent_elo_bin_wdl GROUP BY nation_id, metric_type, bin_index HAVING COUNT(*) > 1)
# MAGIC UNION ALL
# MAGIC -- Non-empty check
# MAGIC SELECT 'nation_opponent_elo_bin_wdl: table is empty' AS check_name, CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.nation_opponent_elo_bin_wdl
# MAGIC UNION ALL
# MAGIC -- W+D+L = games for non-zero rows
# MAGIC SELECT 'nation_opponent_elo_bin_wdl: wins+draws+losses != games' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.nation_opponent_elo_bin_wdl
# MAGIC WHERE games > 0 AND (wins + draws + losses) <> games
# MAGIC UNION ALL
# MAGIC -- ppg between 0-1
# MAGIC SELECT 'nation_opponent_elo_bin_wdl: ppg outside [0,1]' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.nation_opponent_elo_bin_wdl
# MAGIC WHERE ppg IS NOT NULL AND (ppg < 0 OR ppg > 1)
# MAGIC UNION ALL
# MAGIC -- No Unknown nation
# MAGIC SELECT 'nation_opponent_elo_bin_wdl: Unknown nation_id=0 found' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.nation_opponent_elo_bin_wdl
# MAGIC WHERE nation_id = 0
# MAGIC UNION ALL
# MAGIC -- Spine completeness: every nation should have 4 bins per metric_type
# MAGIC SELECT 'nation_opponent_elo_bin_wdl: inconsistent bin count per nation' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM (
# MAGIC   SELECT nation_id, metric_type, COUNT(*) AS bin_count
# MAGIC   FROM naf_catalog.gold_summary.nation_opponent_elo_bin_wdl
# MAGIC   GROUP BY nation_id, metric_type
# MAGIC   HAVING COUNT(*) <> 4
# MAGIC )
# MAGIC UNION ALL
# MAGIC -- Presentation view non-empty
# MAGIC SELECT 'nation_opponent_elo_bin_wdl_display: view is empty' AS check_name, CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS fail_rows
# MAGIC FROM naf_catalog.gold_presentation.nation_opponent_elo_bin_wdl_display
# MAGIC
# MAGIC HAVING fail_rows > 0;

# COMMAND ----------

# DBTITLE 1,Phase 5b: Coach opponent MEDIAN GLO bin tests
# MAGIC %sql
# MAGIC -- =====================================================================
# MAGIC -- PHASE 5b: COACH OPPONENT MEDIAN GLO BIN TESTS
# MAGIC -- =====================================================================
# MAGIC -- Returns ONLY failing checks. Empty result = all pass.
# MAGIC
# MAGIC -- PK: coach_opponent_median_glo_bin_summary (coach_id, bin_index)
# MAGIC SELECT 'coach_opponent_median_glo_bin_summary: PK duplicates' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM (SELECT coach_id, bin_index FROM naf_catalog.gold_summary.coach_opponent_median_glo_bin_summary GROUP BY coach_id, bin_index HAVING COUNT(*) > 1)
# MAGIC UNION ALL
# MAGIC -- Non-empty check
# MAGIC SELECT 'coach_opponent_median_glo_bin_summary: table is empty' AS check_name, CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.coach_opponent_median_glo_bin_summary
# MAGIC UNION ALL
# MAGIC -- W+D+L = games for non-zero rows
# MAGIC SELECT 'coach_opponent_median_glo_bin_summary: wins+draws+losses != games' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.coach_opponent_median_glo_bin_summary
# MAGIC WHERE games_played > 0 AND (wins + draws + losses) <> games_played
# MAGIC UNION ALL
# MAGIC -- Spine completeness: every coach should have 4 bins
# MAGIC SELECT 'coach_opponent_median_glo_bin_summary: inconsistent bin count per coach' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM (
# MAGIC   SELECT coach_id, COUNT(*) AS bin_count
# MAGIC   FROM naf_catalog.gold_summary.coach_opponent_median_glo_bin_summary
# MAGIC   GROUP BY coach_id
# MAGIC   HAVING COUNT(*) <> 4
# MAGIC )
# MAGIC UNION ALL
# MAGIC -- bin_index range 1-4
# MAGIC SELECT 'coach_opponent_median_glo_bin_summary: bin_index outside [1,4]' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.coach_opponent_median_glo_bin_summary
# MAGIC WHERE bin_index NOT IN (1, 2, 3, 4)
# MAGIC UNION ALL
# MAGIC -- win_frac + draw_frac + loss_frac ≈ 1.0 for non-zero rows
# MAGIC SELECT 'coach_opponent_median_glo_bin_summary: fractions dont sum to 1' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_summary.coach_opponent_median_glo_bin_summary
# MAGIC WHERE games_played > 0 AND ABS((win_frac + draw_frac + loss_frac) - 1.0) > 0.01
# MAGIC UNION ALL
# MAGIC -- Presentation view non-empty
# MAGIC SELECT 'coach_opponent_median_glo_bin_results_long: view is empty' AS check_name, CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS fail_rows
# MAGIC FROM naf_catalog.gold_presentation.coach_opponent_median_glo_bin_results_long
# MAGIC
# MAGIC HAVING fail_rows > 0;

# COMMAND ----------

# DBTITLE 1,Rating system allowed-values test
# MAGIC %sql
# MAGIC -- =====================================================================
# MAGIC -- RATING SYSTEM ALLOWED VALUES
# MAGIC -- =====================================================================
# MAGIC -- Ensures no drift from the canonical 'NAF_ELO' value.
# MAGIC -- Returns ONLY failing checks. Empty result = all pass.
# MAGIC
# MAGIC SELECT 'rating_history_fact: unexpected rating_system value' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM (SELECT DISTINCT rating_system FROM naf_catalog.gold_fact.rating_history_fact WHERE rating_system <> 'NAF_ELO')
# MAGIC UNION ALL
# MAGIC SELECT 'coach_rating_race_summary: unexpected rating_system value' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM (SELECT DISTINCT rating_system FROM naf_catalog.gold_summary.coach_rating_race_summary WHERE rating_system <> 'NAF_ELO')
# MAGIC UNION ALL
# MAGIC SELECT 'coach_rating_global_elo_summary: unexpected rating_system value' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM (SELECT DISTINCT rating_system FROM naf_catalog.gold_summary.coach_rating_global_elo_summary WHERE rating_system <> 'NAF_ELO')
# MAGIC
# MAGIC HAVING fail_rows > 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- SILVER invariants
# MAGIC -- ============================================================
# MAGIC
# MAGIC -- A) games_clean must have no coach_id outside coaches_clean (should return 0 rows)
# MAGIC WITH valid_coaches AS (
# MAGIC   SELECT DISTINCT coach_id
# MAGIC   FROM naf_catalog.silver.coaches_clean
# MAGIC   WHERE coach_id IS NOT NULL
# MAGIC ),
# MAGIC violations AS (
# MAGIC   SELECT
# MAGIC     g.game_id,
# MAGIC     g.home_coach_id,
# MAGIC     g.away_coach_id
# MAGIC   FROM naf_catalog.silver.games_clean g
# MAGIC   LEFT JOIN valid_coaches hc ON g.home_coach_id = hc.coach_id
# MAGIC   LEFT JOIN valid_coaches ac ON g.away_coach_id = ac.coach_id
# MAGIC   WHERE hc.coach_id IS NULL OR ac.coach_id IS NULL
# MAGIC )
# MAGIC SELECT 'silver.games_clean: coach FK violations' AS check_name, COUNT(*) AS fail_rows
# MAGIC FROM violations;
# MAGIC
# MAGIC -- B) tournaments_clean should be exactly the set of tournament_ids present in games_clean
# MAGIC WITH t_in_games AS (
# MAGIC   SELECT DISTINCT tournament_id
# MAGIC   FROM naf_catalog.silver.games_clean
# MAGIC   WHERE tournament_id IS NOT NULL
# MAGIC ),
# MAGIC t_in_table AS (
# MAGIC   SELECT DISTINCT tournament_id
# MAGIC   FROM naf_catalog.silver.tournaments_clean
# MAGIC   WHERE tournament_id IS NOT NULL
# MAGIC )
# MAGIC SELECT 'silver.tournaments_clean: missing tournament_id from games_clean' AS check_name,
# MAGIC        COUNT(*) AS fail_rows
# MAGIC FROM (
# MAGIC   SELECT tournament_id FROM t_in_games
# MAGIC   EXCEPT
# MAGIC   SELECT tournament_id FROM t_in_table
# MAGIC );
# MAGIC
# MAGIC WITH t_in_games AS (
# MAGIC   SELECT DISTINCT tournament_id
# MAGIC   FROM naf_catalog.silver.games_clean
# MAGIC   WHERE tournament_id IS NOT NULL
# MAGIC ),
# MAGIC t_in_table AS (
# MAGIC   SELECT DISTINCT tournament_id
# MAGIC   FROM naf_catalog.silver.tournaments_clean
# MAGIC   WHERE tournament_id IS NOT NULL
# MAGIC )
# MAGIC SELECT 'silver.tournaments_clean: contains tournaments with no games' AS check_name,
# MAGIC        COUNT(*) AS fail_rows
# MAGIC FROM (
# MAGIC   SELECT tournament_id FROM t_in_table
# MAGIC   EXCEPT
# MAGIC   SELECT tournament_id FROM t_in_games
# MAGIC );
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- GOLD sanity (should be 0 after reruns)
# MAGIC -- ============================================================
# MAGIC
# MAGIC SELECT 'gold_fact.games_fact: orphan tournament_id' AS check_name,
# MAGIC        COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_fact.games_fact g
# MAGIC LEFT JOIN naf_catalog.gold_dim.tournament_dim t
# MAGIC   ON g.tournament_id = t.tournament_id
# MAGIC WHERE t.tournament_id IS NULL;
# MAGIC
# MAGIC SELECT 'gold_fact.games_fact: orphan home/away coach_id' AS check_name,
# MAGIC        COUNT(*) AS fail_rows
# MAGIC FROM naf_catalog.gold_fact.games_fact g
# MAGIC LEFT JOIN naf_catalog.gold_dim.coach_dim ch
# MAGIC   ON g.home_coach_id = ch.coach_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.coach_dim ca
# MAGIC   ON g.away_coach_id = ca.coach_id
# MAGIC WHERE ch.coach_id IS NULL OR ca.coach_id IS NULL;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1) Which tournament_ids are missing in gold_dim.tournament_dim?
# MAGIC WITH missing_tournaments AS (
# MAGIC   SELECT DISTINCT g.tournament_id
# MAGIC   FROM naf_catalog.silver.games_clean g
# MAGIC   LEFT JOIN naf_catalog.gold_dim.tournament_dim t
# MAGIC     ON g.tournament_id = t.tournament_id
# MAGIC   WHERE g.tournament_id IS NOT NULL
# MAGIC     AND t.tournament_id IS NULL
# MAGIC )
# MAGIC SELECT mt.tournament_id,
# MAGIC        CASE WHEN st.tournament_id IS NULL THEN 'MISSING_IN_SILVER_TOURNAMENTS_CLEAN' ELSE 'PRESENT_IN_SILVER' END AS silver_status,
# MAGIC        COUNT(*) AS games_cnt
# MAGIC FROM missing_tournaments mt
# MAGIC LEFT JOIN naf_catalog.silver.tournaments_clean st
# MAGIC   ON mt.tournament_id = st.tournament_id
# MAGIC JOIN naf_catalog.silver.games_clean g
# MAGIC   ON mt.tournament_id = g.tournament_id
# MAGIC GROUP BY mt.tournament_id, silver_status
# MAGIC ORDER BY games_cnt DESC, mt.tournament_id;
# MAGIC
# MAGIC -- 2) Which coach_ids are missing in gold_dim.coach_dim?
# MAGIC WITH game_coaches AS (
# MAGIC   SELECT home_coach_id AS coach_id FROM naf_catalog.gold_fact.games_fact
# MAGIC   UNION ALL
# MAGIC   SELECT away_coach_id AS coach_id FROM naf_catalog.gold_fact.games_fact
# MAGIC ),
# MAGIC missing_coaches AS (
# MAGIC   SELECT DISTINCT gc.coach_id
# MAGIC   FROM game_coaches gc
# MAGIC   LEFT JOIN naf_catalog.gold_dim.coach_dim cd
# MAGIC     ON gc.coach_id = cd.coach_id
# MAGIC   WHERE gc.coach_id IS NOT NULL
# MAGIC     AND cd.coach_id IS NULL
# MAGIC )
# MAGIC SELECT mc.coach_id,
# MAGIC        COALESCE(sc.coach_name, 'UNKNOWN') AS coach_name,
# MAGIC        CASE WHEN sc.coach_id IS NULL THEN 'MISSING_IN_SILVER_COACHES_CLEAN' ELSE 'PRESENT_IN_SILVER' END AS silver_status,
# MAGIC        SUM(CASE WHEN gf.home_coach_id = mc.coach_id OR gf.away_coach_id = mc.coach_id THEN 1 ELSE 0 END) AS games_cnt
# MAGIC FROM missing_coaches mc
# MAGIC LEFT JOIN naf_catalog.silver.coaches_clean sc
# MAGIC   ON mc.coach_id = sc.coach_id
# MAGIC LEFT JOIN naf_catalog.gold_fact.games_fact gf
# MAGIC   ON gf.home_coach_id = mc.coach_id OR gf.away_coach_id = mc.coach_id
# MAGIC GROUP BY mc.coach_id, coach_name, silver_status
# MAGIC ORDER BY games_cnt DESC, mc.coach_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH p AS (
# MAGIC   SELECT CAST(PMOD(XXHASH64('UNKNOWN'), 2147483647) AS INT) AS old_unknown_hash
# MAGIC ),
# MAGIC checks AS (
# MAGIC
# MAGIC   -- Silver: UNKNOWN sentinel exists + is correct
# MAGIC   SELECT 'silver.nations_entity: nation_id=0 row count != 1' AS check_name,
# MAGIC          CAST(ABS(COUNT(*) - 1) AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.silver.nations_entity
# MAGIC   WHERE nation_id = 0
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'silver.nations_entity: nation_id=0 not labeled Unknown' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.silver.nations_entity
# MAGIC   WHERE nation_id = 0
# MAGIC     AND NOT (LOWER(nation_name) = 'unknown' AND LOWER(nation_name_display) = 'unknown')
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'silver.nations_entity: non-UNKNOWN row has nation_id=0' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.silver.nations_entity
# MAGIC   WHERE nation_id = 0
# MAGIC     AND NOT (LOWER(nation_name) = 'unknown' AND LOWER(nation_name_display) = 'unknown')
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'silver.nations_entity: old hashed UNKNOWN id still present' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.silver.nations_entity, p
# MAGIC   WHERE nation_id = p.old_unknown_hash
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'silver.nations_entity: duplicate nation_id values' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM (
# MAGIC     SELECT nation_id
# MAGIC     FROM naf_catalog.silver.nations_entity
# MAGIC     GROUP BY nation_id
# MAGIC     HAVING COUNT(*) > 1
# MAGIC   )
# MAGIC
# MAGIC   -- Gold_dim: UNKNOWN sentinel exists + is correct
# MAGIC   UNION ALL
# MAGIC   SELECT 'gold_dim.nation_dim: nation_id=0 row count != 1' AS check_name,
# MAGIC          CAST(ABS(COUNT(*) - 1) AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.gold_dim.nation_dim
# MAGIC   WHERE nation_id = 0
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'gold_dim.nation_dim: nation_id=0 not labeled Unknown' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.gold_dim.nation_dim
# MAGIC   WHERE nation_id = 0
# MAGIC     AND NOT (LOWER(nation_name) = 'unknown' AND LOWER(nation_name_display) = 'unknown')
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'gold_dim.nation_dim: old hashed UNKNOWN id still present' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.gold_dim.nation_dim, p
# MAGIC   WHERE nation_id = p.old_unknown_hash
# MAGIC
# MAGIC   -- Downstream FK sanity: dims reference valid nation_id
# MAGIC   UNION ALL
# MAGIC   SELECT 'gold_dim.coach_dim: orphan nation_id' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.gold_dim.coach_dim c
# MAGIC   LEFT JOIN naf_catalog.gold_dim.nation_dim n
# MAGIC     ON c.nation_id = n.nation_id
# MAGIC   WHERE n.nation_id IS NULL
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'gold_dim.tournament_dim: orphan nation_id' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.gold_dim.tournament_dim t
# MAGIC   LEFT JOIN naf_catalog.gold_dim.nation_dim n
# MAGIC     ON t.nation_id = n.nation_id
# MAGIC   WHERE n.nation_id IS NULL
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM checks
# MAGIC WHERE fail_rows <> 0
# MAGIC ORDER BY check_name;
# MAGIC
# MAGIC -- (Optional) eyeball the two UNKNOWN rows you care about:
# MAGIC -- SELECT * FROM naf_catalog.silver.nations_entity WHERE nation_id=0;
# MAGIC -- SELECT * FROM naf_catalog.gold_dim.nation_dim   WHERE nation_id=0;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 10
# MAGIC %sql
# MAGIC WITH p AS (
# MAGIC   SELECT CAST(PMOD(XXHASH64('UNKNOWN'), 2147483647) AS INT) AS unknown_hash
# MAGIC )
# MAGIC SELECT unknown_hash FROM p;
# MAGIC
# MAGIC MERGE INTO naf_catalog.silver.nations_entity t
# MAGIC USING (
# MAGIC   SELECT
# MAGIC     0 AS nation_id,
# MAGIC     'Unknown' AS nation_name,
# MAGIC     'Unknown' AS nation_name_display,
# MAGIC     CAST(NULL AS STRING) AS ioc_code,
# MAGIC     CAST(NULL AS STRING) AS fifa_code
# MAGIC ) s
# MAGIC ON t.nation_id = s.nation_id
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (nation_id, nation_name, nation_name_display, ioc_code, fifa_code)
# MAGIC   VALUES (s.nation_id, s.nation_name, s.nation_name_display, s.ioc_code, s.fifa_code);
# MAGIC
# MAGIC UPDATE naf_catalog.silver.coaches_clean
# MAGIC SET nation_id = 0
# MAGIC WHERE nation_id = CAST(PMOD(XXHASH64('UNKNOWN'), 2147483647) AS INT);
# MAGIC
# MAGIC UPDATE naf_catalog.silver.tournaments_clean
# MAGIC SET nation_id = 0
# MAGIC WHERE nation_id = CAST(PMOD(XXHASH64('UNKNOWN'), 2147483647) AS INT);
# MAGIC
# MAGIC MERGE INTO naf_catalog.gold_dim.nation_dim t
# MAGIC USING (
# MAGIC   SELECT
# MAGIC     0 AS nation_id,
# MAGIC     'Unknown' AS nation_name,
# MAGIC     'Unknown' AS nation_name_display,
# MAGIC     CAST(NULL AS STRING) AS ioc_code,
# MAGIC     CAST(NULL AS STRING) AS fifa_code
# MAGIC ) s
# MAGIC ON t.nation_id = s.nation_id
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (nation_id, nation_name, nation_name_display, ioc_code, fifa_code)
# MAGIC   VALUES (s.nation_id, s.nation_name, s.nation_name_display, s.ioc_code, s.fifa_code);
# MAGIC
# MAGIC UPDATE naf_catalog.gold_dim.coach_dim
# MAGIC SET nation_id = 0
# MAGIC WHERE nation_id = CAST(PMOD(XXHASH64('UNKNOWN'), 2147483647) AS INT);
# MAGIC
# MAGIC UPDATE naf_catalog.gold_dim.tournament_dim
# MAGIC SET nation_id = 0
# MAGIC WHERE nation_id = CAST(PMOD(XXHASH64('UNKNOWN'), 2147483647) AS INT);
# MAGIC
# MAGIC DELETE FROM naf_catalog.silver.nations_entity
# MAGIC WHERE nation_id = CAST(PMOD(XXHASH64('UNKNOWN'), 2147483647) AS INT)
# MAGIC   AND LOWER(nation_name) = 'unknown'
# MAGIC   AND LOWER(nation_name_display) = 'unknown';
# MAGIC
# MAGIC DELETE FROM naf_catalog.gold_dim.nation_dim
# MAGIC WHERE nation_id = CAST(PMOD(XXHASH64('UNKNOWN'), 2147483647) AS INT)
# MAGIC   AND LOWER(nation_name) = 'unknown'
# MAGIC   AND LOWER(nation_name_display) = 'unknown';
# MAGIC
# MAGIC SELECT COUNT(*) AS silver_unknown_zero_rows
# MAGIC FROM naf_catalog.silver.nations_entity
# MAGIC WHERE nation_id = 0;
# MAGIC
# MAGIC SELECT COUNT(*) AS gold_dim_unknown_zero_rows
# MAGIC FROM naf_catalog.gold_dim.nation_dim
# MAGIC WHERE nation_id = 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH checks AS (
# MAGIC
# MAGIC   SELECT 'silver.nations_entity: UNKNOWN rows != 1' AS check_name,
# MAGIC          CAST(ABS(COUNT(*) - 1) AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.silver.nations_entity
# MAGIC   WHERE nation_key_canonical = 'UNKNOWN'
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'silver.nations_entity: UNKNOWN nation_id not 0 (spec)' AS check_name,
# MAGIC          CAST(CASE WHEN MIN(nation_id) = 0 AND MAX(nation_id) = 0 THEN 0 ELSE 1 END AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.silver.nations_entity
# MAGIC   WHERE nation_key_canonical = 'UNKNOWN'
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'gold_dim.nation_dim: missing nation_id=0 row (spec)' AS check_name,
# MAGIC          CAST(CASE WHEN SUM(CASE WHEN nation_id = 0 THEN 1 ELSE 0 END) >= 1 THEN 0 ELSE 1 END AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.gold_dim.nation_dim
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'gold_dim.coach_dim: PK duplicates (coach_id)' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM (
# MAGIC     SELECT coach_id
# MAGIC     FROM naf_catalog.gold_dim.coach_dim
# MAGIC     GROUP BY coach_id
# MAGIC     HAVING COUNT(*) > 1
# MAGIC   )
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'gold_dim.coach_dim: orphan nation_id' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.gold_dim.coach_dim c
# MAGIC   LEFT JOIN naf_catalog.gold_dim.nation_dim n
# MAGIC     ON c.nation_id = n.nation_id
# MAGIC   WHERE n.nation_id IS NULL
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'gold_dim.tournament_dim: is_major_tournament is NULL (must be NOT NULL)' AS check_name,
# MAGIC          CAST(SUM(CASE WHEN is_major_tournament IS NULL THEN 1 ELSE 0 END) AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.gold_dim.tournament_dim
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'gold_dim.tournament_dim: orphan nation_id' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.gold_dim.tournament_dim t
# MAGIC   LEFT JOIN naf_catalog.gold_dim.nation_dim n
# MAGIC     ON t.nation_id = n.nation_id
# MAGIC   WHERE n.nation_id IS NULL
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'gold_dim.tournament_dim: missing tournaments referenced by silver.games_clean' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM (
# MAGIC     SELECT DISTINCT g.tournament_id
# MAGIC     FROM naf_catalog.silver.games_clean g
# MAGIC     LEFT JOIN naf_catalog.gold_dim.tournament_dim t
# MAGIC       ON g.tournament_id = t.tournament_id
# MAGIC     WHERE t.tournament_id IS NULL
# MAGIC   )
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'gold_fact.games_fact: PK duplicates (game_id)' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM (
# MAGIC     SELECT game_id
# MAGIC     FROM naf_catalog.gold_fact.games_fact
# MAGIC     GROUP BY game_id
# MAGIC     HAVING COUNT(*) > 1
# MAGIC   )
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'gold_fact.games_fact: orphan date_id' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.gold_fact.games_fact g
# MAGIC   LEFT JOIN naf_catalog.gold_dim.date_dim d
# MAGIC     ON g.date_id = d.date_id
# MAGIC   WHERE d.date_id IS NULL
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'gold_fact.games_fact: orphan tournament_id' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.gold_fact.games_fact g
# MAGIC   LEFT JOIN naf_catalog.gold_dim.tournament_dim t
# MAGIC     ON g.tournament_id = t.tournament_id
# MAGIC   WHERE t.tournament_id IS NULL
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'gold_fact.games_fact: orphan home/away coach_id' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.gold_fact.games_fact g
# MAGIC   LEFT JOIN naf_catalog.gold_dim.coach_dim ch
# MAGIC     ON g.home_coach_id = ch.coach_id
# MAGIC   LEFT JOIN naf_catalog.gold_dim.coach_dim ca
# MAGIC     ON g.away_coach_id = ca.coach_id
# MAGIC   WHERE ch.coach_id IS NULL OR ca.coach_id IS NULL
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'gold_fact.coach_games_fact: PK duplicates (coach_id, game_id)' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM (
# MAGIC     SELECT coach_id, game_id
# MAGIC     FROM naf_catalog.gold_fact.coach_games_fact
# MAGIC     GROUP BY coach_id, game_id
# MAGIC     HAVING COUNT(*) > 1
# MAGIC   )
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'gold_fact.rating_history_fact: PK duplicates (scope, rating_system, game_id, coach_id, race_id)' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM (
# MAGIC     SELECT scope, rating_system, game_id, coach_id, race_id
# MAGIC     FROM naf_catalog.gold_fact.rating_history_fact
# MAGIC     GROUP BY scope, rating_system, game_id, coach_id, race_id
# MAGIC     HAVING COUNT(*) > 1
# MAGIC   )
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'gold_fact.rating_history_fact: GLOBAL scope with race_id != 0' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.gold_fact.rating_history_fact
# MAGIC   WHERE scope = 'GLOBAL' AND race_id <> 0
# MAGIC
# MAGIC   UNION ALL
# MAGIC   SELECT 'gold_fact.rating_history_fact: RACE scope with race_id = 0' AS check_name,
# MAGIC          CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.gold_fact.rating_history_fact
# MAGIC   WHERE scope = 'RACE' AND race_id = 0
# MAGIC
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM checks
# MAGIC WHERE fail_rows <> 0
# MAGIC ORDER BY check_name;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Coach dashboard – decisions + task list (pre-implementation)
# MAGIC
# MAGIC This note consolidates the agreed **semantic decisions** and the **work items** we’ll implement when we walk the code cell-by-cell.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1) Scope
# MAGIC
# MAGIC Applies to:
# MAGIC - `naf_catalog.gold_summary` coach summary outputs (metrics ownership)
# MAGIC - `naf_catalog.gold_presentation` coach contracts (dashboard shaping only)
# MAGIC - Databricks dashboard: `Coach Dashboard.lvdash.json`
# MAGIC
# MAGIC Source notebooks:
# MAGIC - `331_NAF_gold_summary_coach.py`
# MAGIC - `341_NAF_gold_presentation_coach.py`a
# MAGIC - Dashboard JSON: `Coach Dashboard.lvdash.json`
# MAGIC
# MAGIC ---
# MAGIC a
# MAGIC ## 2) Coach Profile – content decisions
# MAGIC
# MAGIC ### Overview
# MAGIC - **Add** `points_per_game` to the Overview section (coach-wide performance).
# MAGIC
# MAGIC ### Race Overview (new headline/section)
# MAGIC Create a section titled **“Race Overview”** containing:
# MAGIC - Favorite race
# MAGIC - Top race
# MAGIC - Race statistics
# MAGIC
# MAGIC Specific content rules:
# MAGIC - Under **Favorite race**: replace **win%** with **points per game** (show PPG instead of Win%).
# MAGIC - **Remove** “best race win%” (do not show this metric).
# MAGIC - Move **distinct races played** into **Race Overview** (not elsewhere).
# MAGIC
# MAGIC ### Top race additions
# MAGIC Under “Top race”, include:
# MAGIC - Peak ELO
# MAGIC - Peak ELO race
# MAGIC - Date of last game with this race
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 3) Ratings – content decisions
# MAGIC
# MAGIC ### What stays (coach dashboard “Ratings” tiles)
# MAGIC Keep only:
# MAGIC - **Current GLO**
# MAGIC - **Peak GLO (post-threshold)**
# MAGIC - **Mean GLO (post-threshold)**
# MAGIC - **Median GLO** (default: all-games median unless we later decide otherwise)
# MAGIC
# MAGIC Add:
# MAGIC - **Mean last 50 games** (like peak last-50 exists), as a first-class tile/field.
# MAGIC
# MAGIC ### Timelines / time series
# MAGIC - **ELO and GLO time series must include all rated games.**
# MAGIC - **No threshold filtering** should be applied to timeline datasets.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 4) Threshold policy (critical)
# MAGIC
# MAGIC Thresholds exist to protect *post-threshold* (“mature sample”) metrics only.
# MAGIC They do **not** apply to timelines.
# MAGIC
# MAGIC Current working thresholds (as implemented today):
# MAGIC - **ELO:** 25 games
# MAGIC - **GLO:** 50 games
# MAGIC
# MAGIC Semantics:
# MAGIC - **Post-threshold metrics** must be computed using **games after the first N games** (N=25 for ELO, N=50 for GLO),
# MAGIC   and only be considered valid when the coach has at least N games.
# MAGIC - **Last-50 metrics** must use the most recent 50 games *if available* (else NULL / invalid flags).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 5) Opponent strength bins – decisions
# MAGIC
# MAGIC ### Expected output shape (dynamic_5)
# MAGIC For `bin_scheme_id = dynamic_5`, the dashboard should always show:
# MAGIC - **5 bins** (even if some bins have zero games for a given coach)
# MAGIC - Plus an **Overall/Overview** row (placed last in ordering)
# MAGIC
# MAGIC ### Ordering
# MAGIC - Bins should be ordered logically by bin index (low → high)
# MAGIC - “Overview/Overall” should appear **last**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 6) Issues found in current code (from review)
# MAGIC
# MAGIC ### A) Global GLO “post-threshold” semantics likely wrong today
# MAGIC In `gold_summary.coach_rating_global_glo_summary`, the current `post_threshold` logic gates on
# MAGIC “coach has >= threshold games” but does **not** exclude the first N games.
# MAGIC This conflicts with the agreed semantics above.
# MAGIC
# MAGIC ### B) dynamic_5 not always producing 5 bins
# MAGIC The binning currently returns only bins that have at least one observed game for the coach.
# MAGIC This causes dashboards to show fewer than 5 rows/bins for `dynamic_5`.
# MAGIC
# MAGIC ### C) Bin ordering: “Overview last”
# MAGIC Ordering exists as `(group_order, row_order)` in some datasets, but:
# MAGIC - “Overall” currently sorts first (not last)
# MAGIC - Some widgets use static category ordering lists that may not match dynamic bin labels
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 7) Implementation task list (what we’ll do in the walkthrough)
# MAGIC
# MAGIC ### Gold_summary (`331_NAF_gold_summary_coach.py`)
# MAGIC 1) **Fix** `coach_rating_global_glo_summary` post-threshold logic:
# MAGIC    - filter games by game_number > threshold (not just “coach has >= threshold games”)
# MAGIC    - keep timelines untouched
# MAGIC 2) Ensure outputs contain and are used downstream:
# MAGIC    - `glo_mean_post_threshold`
# MAGIC    - `glo_mean_last_50`
# MAGIC    - validity flags clearly aligned to semantics
# MAGIC 3) Opponent bins:
# MAGIC    - produce a **bin spine** so each `(coach_id, bin_scheme_id)` has all 5 bins (0-filled) + overall
# MAGIC
# MAGIC ### Gold_presentation (`341_NAF_gold_presentation_coach.py`)
# MAGIC 4) **Coach Profile shape changes** (to support new “Race Overview” content):
# MAGIC    - include main/favorite race **points_per_game**
# MAGIC    - include top race “peak elo” + “last game date” fields needed by dashboard
# MAGIC 5) **Bin ordering changes**
# MAGIC    - set ordering fields so Overview/Overall sorts last
# MAGIC    - ensure datasets expose numeric sort keys where the dashboard can use them
# MAGIC
# MAGIC ### Dashboard JSON (`Coach Dashboard.lvdash.json`)
# MAGIC 6) Rework profile widgets:
# MAGIC    - add Overview PPG
# MAGIC    - replace favorite race Win% → PPG
# MAGIC    - remove best race win%
# MAGIC    - move distinct races played into Race Overview
# MAGIC    - add the “Race Overview” headline and regroup visuals
# MAGIC 7) Ratings widgets:
# MAGIC    - simplify to the kept metrics only (Current, Peak post-threshold, Mean post-threshold, Median, Mean last-50)
# MAGIC 8) Bins widgets:
# MAGIC    - remove static category ordering lists that don’t match dynamic bin labels
# MAGIC    - order by row_order / sort key, with Overview last
# MAGIC 9) Layout:
# MAGIC    - attempt to fit onto **one page** (reduce page count from 2 → 1) by moving widgets into a single page layout.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 8) Validation checklist (after changes)
# MAGIC
# MAGIC - Post-threshold metrics:
# MAGIC   - For coaches with < threshold games: post-threshold metrics are NULL/invalid, but timelines still show games.
# MAGIC   - For coaches with >= threshold games: post-threshold uses only games after first N.
# MAGIC - dynamic_5:
# MAGIC   - Always returns exactly 5 bins per coach + one Overall row.
# MAGIC   - Bins are ordered low→high and Overall is last.
# MAGIC - Dashboard:
# MAGIC   - “Race Overview” section contains the intended fields and the old ones are removed.
# MAGIC   - Ratings section shows only the chosen metrics.
# MAGIC

# COMMAND ----------

# DBTITLE 1,SILVER: CORE UNIQUENESS + NOT-NULL + DATE KEY INTEGRITY
# MAGIC %sql
# MAGIC -- =====================================================================
# MAGIC -- SILVER: CORE UNIQUENESS + NOT-NULL + DATE KEY INTEGRITY
# MAGIC -- Expectation: all “bad_*” counts = 0
# MAGIC -- =====================================================================
# MAGIC
# MAGIC -- -------------------------
# MAGIC -- Uniqueness (source IDs)
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
# MAGIC        SUM(CASE WHEN coach_id IS NULL THEN 1 ELSE 0 END) AS bad_null_coach_id,
# MAGIC        SUM(CASE WHEN nation_id IS NULL THEN 1 ELSE 0 END) AS bad_null_nation_id
# MAGIC FROM naf_catalog.silver.coaches_clean;
# MAGIC
# MAGIC SELECT 'silver.games_clean' AS table_name,
# MAGIC        SUM(CASE WHEN game_id IS NULL THEN 1 ELSE 0 END) AS bad_null_game_id,
# MAGIC        SUM(CASE WHEN home_coach_id IS NULL THEN 1 ELSE 0 END) AS bad_null_home_coach_id,
# MAGIC        SUM(CASE WHEN away_coach_id IS NULL THEN 1 ELSE 0 END) AS bad_null_away_coach_id,
# MAGIC        SUM(CASE WHEN home_race_id IS NULL THEN 1 ELSE 0 END) AS bad_null_home_race_id,
# MAGIC        SUM(CASE WHEN away_race_id IS NULL THEN 1 ELSE 0 END) AS bad_null_away_race_id
# MAGIC FROM naf_catalog.silver.games_clean;
# MAGIC
# MAGIC SELECT 'silver.tournaments_clean' AS table_name,
# MAGIC        SUM(CASE WHEN tournament_id IS NULL THEN 1 ELSE 0 END) AS bad_null_tournament_id,
# MAGIC        SUM(CASE WHEN nation_id IS NULL THEN 1 ELSE 0 END) AS bad_null_nation_id
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
# MAGIC SELECT 'silver.tournaments_clean start_date_id' AS check_name,
# MAGIC        COUNT(*) AS bad_rows
# MAGIC FROM naf_catalog.silver.tournaments_clean
# MAGIC WHERE date_start IS NOT NULL
# MAGIC   AND start_date_id <> CAST(date_format(date_start, 'yyyyMMdd') AS INT);
# MAGIC
# MAGIC SELECT 'silver.tournaments_clean end_date_id' AS check_name,
# MAGIC        COUNT(*) AS bad_rows
# MAGIC FROM naf_catalog.silver.tournaments_clean
# MAGIC WHERE date_end IS NOT NULL
# MAGIC   AND end_date_id <> CAST(date_format(date_end, 'yyyyMMdd') AS INT);
# MAGIC
# MAGIC -- =====================================================================
# MAGIC -- SILVER: NATION SURROGATE KEY SAFETY
# MAGIC -- Expectation: bad_* counts = 0
# MAGIC -- =====================================================================
# MAGIC
# MAGIC -- nation_id uniqueness
# MAGIC SELECT 'silver.nations_entity nation_id unique' AS check_name,
# MAGIC        COUNT(*) AS rows,
# MAGIC        COUNT(DISTINCT nation_id) AS distinct_ids,
# MAGIC        (COUNT(*) - COUNT(DISTINCT nation_id)) AS bad_duplicates
# MAGIC FROM naf_catalog.silver.nations_entity;
# MAGIC
# MAGIC -- exactly one UNKNOWN row
# MAGIC SELECT 'silver.nations_entity UNKNOWN count' AS check_name,
# MAGIC        COUNT(*) AS unknown_rows
# MAGIC FROM naf_catalog.silver.nations_entity
# MAGIC WHERE nation_key_canonical = 'UNKNOWN';
# MAGIC
# MAGIC -- recompute group_key the same way and confirm no collisions in the int-range hash
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
# MAGIC -- =====================================================================
# MAGIC -- GOLD_DIM: UNIQUENESS (minimal checks)
# MAGIC -- Expectation: bad_duplicates = 0
# MAGIC -- =====================================================================
# MAGIC
# MAGIC SELECT 'gold_dim.nation_dim' AS table_name,
# MAGIC        COUNT(*) AS rows,
# MAGIC        COUNT(DISTINCT nation_id) AS distinct_ids,
# MAGIC        (COUNT(*) - COUNT(DISTINCT nation_id)) AS bad_duplicates
# MAGIC FROM naf_catalog.gold_dim.nation_dim;
# MAGIC
# MAGIC SELECT 'gold_dim.coach_dim' AS table_name,
# MAGIC        COUNT(*) AS rows,
# MAGIC        COUNT(DISTINCT coach_id) AS distinct_ids,
# MAGIC        (COUNT(*) - COUNT(DISTINCT coach_id)) AS bad_duplicates
# MAGIC FROM naf_catalog.gold_dim.coach_dim;
# MAGIC
# MAGIC SELECT 'gold_dim.race_dim' AS table_name,
# MAGIC        COUNT(*) AS rows,
# MAGIC        COUNT(DISTINCT race_id) AS distinct_ids,
# MAGIC        (COUNT(*) - COUNT(DISTINCT race_id)) AS bad_duplicates
# MAGIC FROM naf_catalog.gold_dim.race_dim;
# MAGIC
# MAGIC SELECT 'gold_dim.tournament_dim' AS table_name,
# MAGIC        COUNT(*) AS rows,
# MAGIC        COUNT(DISTINCT tournament_id) AS distinct_ids,
# MAGIC        (COUNT(*) - COUNT(DISTINCT tournament_id)) AS bad_duplicates
# MAGIC FROM naf_catalog.gold_dim.tournament_dim;
# MAGIC
# MAGIC -- =====================================================================
# MAGIC -- GOLD_FACT: PK UNIQUENESS + FK COVERAGE (core coherence)
# MAGIC -- Expectation: all “missing_*” = 0 and duplicate queries return 0 rows
# MAGIC -- =====================================================================
# MAGIC
# MAGIC -- games_fact PK uniqueness (expect 0 rows)
# MAGIC SELECT game_id, COUNT(*) AS cnt
# MAGIC FROM naf_catalog.gold_fact.games_fact
# MAGIC GROUP BY 1
# MAGIC HAVING COUNT(*) > 1;
# MAGIC
# MAGIC -- tournament_statistics_fact PK uniqueness (expect 0 rows)
# MAGIC SELECT tournament_id, coach_id, race_id, stat_id, stat_date, COUNT(*) AS cnt
# MAGIC FROM naf_catalog.gold_fact.tournament_statistics_fact
# MAGIC GROUP BY 1,2,3,4,5
# MAGIC HAVING COUNT(*) > 1;
# MAGIC
# MAGIC -- rating_history_fact PK uniqueness (expect 0 rows)
# MAGIC SELECT scope, rating_system, game_id, coach_id, race_id, COUNT(*) AS cnt
# MAGIC FROM naf_catalog.gold_fact.rating_history_fact
# MAGIC GROUP BY 1,2,3,4,5
# MAGIC HAVING COUNT(*) > 1;
# MAGIC
# MAGIC -- games_fact FK coverage
# MAGIC SELECT
# MAGIC   SUM(CASE WHEN t.tournament_id IS NULL AND f.tournament_id IS NOT NULL THEN 1 ELSE 0 END) AS missing_tournament_dim,
# MAGIC   SUM(CASE WHEN hc.coach_id IS NULL THEN 1 ELSE 0 END) AS missing_home_coach_dim,
# MAGIC   SUM(CASE WHEN ac.coach_id IS NULL THEN 1 ELSE 0 END) AS missing_away_coach_dim,
# MAGIC   SUM(CASE WHEN hr.race_id  IS NULL THEN 1 ELSE 0 END) AS missing_home_race_dim,
# MAGIC   SUM(CASE WHEN ar.race_id  IS NULL THEN 1 ELSE 0 END) AS missing_away_race_dim,
# MAGIC   SUM(CASE WHEN d.date_id   IS NULL AND f.date_id IS NOT NULL THEN 1 ELSE 0 END) AS missing_date_dim
# MAGIC FROM naf_catalog.gold_fact.games_fact f
# MAGIC LEFT JOIN naf_catalog.gold_dim.tournament_dim t ON f.tournament_id = t.tournament_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.coach_dim hc     ON f.home_coach_id = hc.coach_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.coach_dim ac     ON f.away_coach_id = ac.coach_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.race_dim hr      ON f.home_race_id  = hr.race_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.race_dim ar      ON f.away_race_id  = ar.race_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.date_dim d       ON f.date_id       = d.date_id;
# MAGIC
# MAGIC -- rating_history_fact FK coverage (note: race_id/opponent_race_id can be 0 for GLOBAL)
# MAGIC SELECT
# MAGIC   SUM(CASE WHEN g.game_id IS NULL THEN 1 ELSE 0 END) AS missing_game_fact,
# MAGIC   SUM(CASE WHEN c.coach_id IS NULL THEN 1 ELSE 0 END) AS missing_coach_dim,
# MAGIC   SUM(CASE WHEN oc.coach_id IS NULL THEN 1 ELSE 0 END) AS missing_opponent_coach_dim,
# MAGIC   SUM(CASE WHEN t.tournament_id IS NULL AND f.tournament_id IS NOT NULL THEN 1 ELSE 0 END) AS missing_tournament_dim,
# MAGIC   SUM(CASE WHEN d.date_id IS NULL AND f.date_id IS NOT NULL THEN 1 ELSE 0 END) AS missing_date_dim,
# MAGIC   SUM(CASE WHEN f.scope = 'RACE' AND r.race_id IS NULL THEN 1 ELSE 0 END) AS missing_race_dim,
# MAGIC   SUM(CASE WHEN f.scope = 'RACE' AND orc.race_id IS NULL THEN 1 ELSE 0 END) AS missing_opponent_race_dim
# MAGIC FROM naf_catalog.gold_fact.rating_history_fact f
# MAGIC LEFT JOIN naf_catalog.gold_fact.games_fact g     ON f.game_id = g.game_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.coach_dim c       ON f.coach_id = c.coach_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.coach_dim oc      ON f.opponent_coach_id = oc.coach_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.tournament_dim t  ON f.tournament_id = t.tournament_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.date_dim d        ON f.date_id = d.date_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.race_dim r        ON f.race_id = r.race_id
# MAGIC LEFT JOIN naf_catalog.gold_dim.race_dim orc      ON f.opponent_race_id = orc.race_id;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Test report: collect all structured tests → CSV
# MAGIC %sql
# MAGIC -- =====================================================================
# MAGIC -- TEST REPORT: Collect ALL structured test results into a single table.
# MAGIC -- Writes to naf_catalog.gold_summary.test_report (Delta), then export
# MAGIC -- to CSV via the Python cell below.
# MAGIC --
# MAGIC -- Unlike the per-cell tests above (which only show failures),
# MAGIC -- this report captures EVERY check with its pass/fail status.
# MAGIC -- =====================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_summary.test_report
# MAGIC USING DELTA AS
# MAGIC
# MAGIC WITH params AS (
# MAGIC   SELECT array(1, 13, 15) AS allowed_variant_ids
# MAGIC ),
# MAGIC
# MAGIC -- -----------------------------------------------------------------
# MAGIC -- CELL 1: Core contract tests (severity + check_name + fail_rows)
# MAGIC -- -----------------------------------------------------------------
# MAGIC required_objects AS (
# MAGIC   SELECT * FROM VALUES
# MAGIC     ('silver',           'nations_entity',         'CORE', 'TABLE'),
# MAGIC     ('silver',           'coaches_clean',          'CORE', 'TABLE'),
# MAGIC     ('silver',           'games_clean',            'CORE', 'TABLE'),
# MAGIC     ('silver',           'tournaments_clean',      'CORE', 'TABLE'),
# MAGIC     ('gold_dim',         'nation_dim',             'CORE', 'TABLE'),
# MAGIC     ('gold_dim',         'coach_dim',              'CORE', 'TABLE'),
# MAGIC     ('gold_dim',         'tournament_dim',         'CORE', 'TABLE'),
# MAGIC     ('gold_dim',         'race_dim',               'CORE', 'TABLE'),
# MAGIC     ('gold_dim',         'date_dim',               'CORE', 'TABLE'),
# MAGIC     ('gold_fact',        'games_fact',             'CORE', 'TABLE'),
# MAGIC     ('gold_fact',        'coach_games_fact',       'CORE', 'TABLE'),
# MAGIC     ('gold_fact',        'rating_history_fact',    'CORE', 'TABLE'),
# MAGIC     ('gold_summary',     'coach_game_spine_v',                     'PLAN', 'VIEW'),
# MAGIC     ('gold_summary',     'coach_performance_summary',              'PLAN', 'TABLE'),
# MAGIC     ('gold_summary',     'coach_race_summary',                     'PLAN', 'TABLE'),
# MAGIC     ('gold_summary',     'coach_streak_detail',                  'PLAN', 'TABLE'),
# MAGIC     ('gold_summary',     'coach_streak_overview',                   'PLAN', 'TABLE'),
# MAGIC     ('gold_summary',     'coach_tournament_performance_summary',   'PLAN', 'TABLE'),
# MAGIC     ('gold_summary',     'coach_form_summary',                     'PLAN', 'TABLE'),
# MAGIC     ('gold_presentation','coach_profile',                          'PLAN', 'VIEW'),
# MAGIC     ('gold_presentation','coach_race_performance',                 'PLAN', 'VIEW'),
# MAGIC     ('gold_presentation','coach_tournament_performance',           'PLAN', 'VIEW')
# MAGIC   AS required_objects(table_schema, table_name, tier, expected_kind)
# MAGIC ),
# MAGIC object_existence AS (
# MAGIC   SELECT
# MAGIC     CASE WHEN r.tier = 'CORE' THEN 'ERROR' ELSE 'WARN' END AS severity,
# MAGIC     CONCAT('object_exists: ', r.table_schema, '.', r.table_name) AS check_name,
# MAGIC     CAST(CASE WHEN t.table_name IS NULL THEN 1 ELSE 0 END AS BIGINT) AS fail_rows
# MAGIC   FROM required_objects r
# MAGIC   LEFT JOIN naf_catalog.information_schema.tables t
# MAGIC     ON t.table_schema = r.table_schema
# MAGIC    AND t.table_name   = r.table_name
# MAGIC ),
# MAGIC
# MAGIC core_checks AS (
# MAGIC   SELECT 'ERROR' AS severity, 'silver.nations_entity: nation_id=0 row count != 1' AS check_name,
# MAGIC          CAST(ABS(COUNT(*) - 1) AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.silver.nations_entity WHERE nation_id = 0
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'silver.nations_entity: nation_id=0 row not labeled Unknown',
# MAGIC          CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.silver.nations_entity
# MAGIC   WHERE nation_id = 0 AND NOT (LOWER(nation_name) = 'unknown' AND LOWER(nation_name_display) = 'unknown')
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'silver.nations_entity: nation_id duplicates',
# MAGIC          CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM (SELECT nation_id FROM naf_catalog.silver.nations_entity GROUP BY nation_id HAVING COUNT(*) > 1)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'silver.coaches_clean: PK duplicates (coach_id)',
# MAGIC          CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM (SELECT coach_id FROM naf_catalog.silver.coaches_clean GROUP BY coach_id HAVING COUNT(*) > 1)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'silver.coaches_clean: orphan nation_id',
# MAGIC          CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.silver.coaches_clean c LEFT JOIN naf_catalog.silver.nations_entity n ON c.nation_id = n.nation_id WHERE n.nation_id IS NULL
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'silver.games_clean: PK duplicates (game_id)',
# MAGIC          CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM (SELECT game_id FROM naf_catalog.silver.games_clean GROUP BY game_id HAVING COUNT(*) > 1)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'silver.games_clean: required key(s) NULL',
# MAGIC          CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.silver.games_clean
# MAGIC   WHERE game_id IS NULL OR tournament_id IS NULL OR variant_id IS NULL OR date_id IS NULL
# MAGIC      OR home_coach_id IS NULL OR away_coach_id IS NULL OR home_race_id IS NULL OR away_race_id IS NULL
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'silver.games_clean: variant_id not in allowed set',
# MAGIC          CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.silver.games_clean g CROSS JOIN params p
# MAGIC   WHERE array_contains(p.allowed_variant_ids, g.variant_id) = FALSE
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'silver.games_clean: self-play',
# MAGIC          CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.silver.games_clean WHERE home_coach_id = away_coach_id
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'silver.games_clean: race_id = 0 used',
# MAGIC          CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.silver.games_clean WHERE home_race_id = 0 OR away_race_id = 0
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'silver.games_clean: coach FK violations',
# MAGIC          CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.silver.games_clean g
# MAGIC   LEFT JOIN naf_catalog.silver.coaches_clean ch ON g.home_coach_id = ch.coach_id
# MAGIC   LEFT JOIN naf_catalog.silver.coaches_clean ca ON g.away_coach_id = ca.coach_id
# MAGIC   WHERE ch.coach_id IS NULL OR ca.coach_id IS NULL
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'silver.tournaments_clean: PK duplicates',
# MAGIC          CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM (SELECT tournament_id FROM naf_catalog.silver.tournaments_clean GROUP BY tournament_id HAVING COUNT(*) > 1)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'silver.tournaments_clean: is_major_tournament NULL',
# MAGIC          CAST(SUM(CASE WHEN is_major_tournament IS NULL THEN 1 ELSE 0 END) AS BIGINT)
# MAGIC   FROM naf_catalog.silver.tournaments_clean
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'silver.tournaments_clean: tournament_ids missing for games_clean',
# MAGIC          CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM (SELECT DISTINCT tournament_id FROM naf_catalog.silver.games_clean WHERE tournament_id IS NOT NULL
# MAGIC         EXCEPT SELECT DISTINCT tournament_id FROM naf_catalog.silver.tournaments_clean WHERE tournament_id IS NOT NULL)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'silver.tournaments_clean: tournaments with no games',
# MAGIC          CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM (SELECT DISTINCT tournament_id FROM naf_catalog.silver.tournaments_clean WHERE tournament_id IS NOT NULL
# MAGIC         EXCEPT SELECT DISTINCT tournament_id FROM naf_catalog.silver.games_clean WHERE tournament_id IS NOT NULL)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'gold_dim.nation_dim: missing nation_id=0',
# MAGIC          CAST(CASE WHEN SUM(CASE WHEN nation_id = 0 THEN 1 ELSE 0 END) = 1 THEN 0 ELSE 1 END AS BIGINT)
# MAGIC   FROM naf_catalog.gold_dim.nation_dim
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'gold_dim.race_dim: missing race_id=0',
# MAGIC          CAST(CASE WHEN SUM(CASE WHEN race_id = 0 THEN 1 ELSE 0 END) = 1 THEN 0 ELSE 1 END AS BIGINT)
# MAGIC   FROM naf_catalog.gold_dim.race_dim
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'gold_dim.coach_dim: orphan nation_id',
# MAGIC          CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.gold_dim.coach_dim c LEFT JOIN naf_catalog.gold_dim.nation_dim n ON c.nation_id = n.nation_id WHERE n.nation_id IS NULL
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'gold_dim.tournament_dim: is_major_tournament NULL',
# MAGIC          CAST(SUM(CASE WHEN is_major_tournament IS NULL THEN 1 ELSE 0 END) AS BIGINT)
# MAGIC   FROM naf_catalog.gold_dim.tournament_dim
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'gold_dim.tournament_dim: missing tournaments from games_clean',
# MAGIC          CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM (SELECT DISTINCT g.tournament_id FROM naf_catalog.silver.games_clean g
# MAGIC         LEFT JOIN naf_catalog.gold_dim.tournament_dim t ON g.tournament_id = t.tournament_id
# MAGIC         WHERE g.tournament_id IS NOT NULL AND t.tournament_id IS NULL)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'gold_fact.games_fact: PK duplicates',
# MAGIC          CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM (SELECT game_id FROM naf_catalog.gold_fact.games_fact GROUP BY game_id HAVING COUNT(*) > 1)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'gold_fact.games_fact: orphan tournament_id',
# MAGIC          CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.gold_fact.games_fact g LEFT JOIN naf_catalog.gold_dim.tournament_dim t ON g.tournament_id = t.tournament_id WHERE t.tournament_id IS NULL
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'gold_fact.games_fact: orphan coach_id',
# MAGIC          CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.gold_fact.games_fact g
# MAGIC   LEFT JOIN naf_catalog.gold_dim.coach_dim ch ON g.home_coach_id = ch.coach_id
# MAGIC   LEFT JOIN naf_catalog.gold_dim.coach_dim ca ON g.away_coach_id = ca.coach_id
# MAGIC   WHERE ch.coach_id IS NULL OR ca.coach_id IS NULL
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'gold_fact.games_fact: orphan date_id',
# MAGIC          CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.gold_fact.games_fact g LEFT JOIN naf_catalog.gold_dim.date_dim d ON g.date_id = d.date_id WHERE d.date_id IS NULL
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'gold_fact.games_fact: orphan race_id',
# MAGIC          CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.gold_fact.games_fact g
# MAGIC   LEFT JOIN naf_catalog.gold_dim.race_dim rh ON g.home_race_id = rh.race_id
# MAGIC   LEFT JOIN naf_catalog.gold_dim.race_dim ra ON g.away_race_id = ra.race_id
# MAGIC   WHERE rh.race_id IS NULL OR ra.race_id IS NULL
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'gold_fact.coach_games_fact: PK duplicates',
# MAGIC          CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM (SELECT game_id, coach_id, home_away FROM naf_catalog.gold_fact.coach_games_fact GROUP BY game_id, coach_id, home_away HAVING COUNT(*) > 1)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'gold_fact.coach_games_fact: home_away not in (H,A)',
# MAGIC          CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.gold_fact.coach_games_fact WHERE home_away NOT IN ('H','A') OR home_away IS NULL
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'gold_fact.coach_games_fact: result_numeric not in (0,0.5,1)',
# MAGIC          CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.gold_fact.coach_games_fact WHERE result_numeric NOT IN (0.0, 0.5, 1.0) OR result_numeric IS NULL
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'gold_fact.coach_games_fact: rowcount != 2x games_fact',
# MAGIC          CAST(ABS((SELECT COUNT(*) FROM naf_catalog.gold_fact.coach_games_fact) - 2 * (SELECT COUNT(*) FROM naf_catalog.gold_fact.games_fact)) AS BIGINT)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'gold_fact.rating_history_fact: PK duplicates',
# MAGIC          CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM (SELECT scope, rating_system, game_id, coach_id, race_id FROM naf_catalog.gold_fact.rating_history_fact GROUP BY scope, rating_system, game_id, coach_id, race_id HAVING COUNT(*) > 1)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'gold_fact.rating_history_fact: GLOBAL scope with race_id != 0',
# MAGIC          CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.gold_fact.rating_history_fact WHERE scope = 'GLOBAL' AND race_id <> 0
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'gold_fact.rating_history_fact: RACE scope with race_id = 0',
# MAGIC          CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.gold_fact.rating_history_fact WHERE scope = 'RACE' AND race_id = 0
# MAGIC   UNION ALL
# MAGIC   SELECT 'WARN', 'gold_summary: forbidden display columns present',
# MAGIC          CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.information_schema.columns
# MAGIC   WHERE table_schema = 'gold_summary' AND column_name IN ('coach_name','race_name','nation_name_display','flag_code','tournament_name')
# MAGIC   UNION ALL
# MAGIC   SELECT 'WARN', 'gold_summary: game_date column present (should use date_id)',
# MAGIC          CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.information_schema.columns WHERE table_schema = 'gold_summary' AND column_name = 'game_date'
# MAGIC ),
# MAGIC
# MAGIC -- -----------------------------------------------------------------
# MAGIC -- CELL 2: Coach pipeline PK/FK
# MAGIC -- -----------------------------------------------------------------
# MAGIC coach_checks AS (
# MAGIC   SELECT 'ERROR' AS severity, 'coach_performance_summary: PK duplicates' AS check_name, CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM (SELECT coach_id FROM naf_catalog.gold_summary.coach_performance_summary GROUP BY coach_id HAVING COUNT(*) > 1)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'coach_rating_global_elo_summary: PK duplicates', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM (SELECT rating_system, coach_id FROM naf_catalog.gold_summary.coach_rating_global_elo_summary GROUP BY rating_system, coach_id HAVING COUNT(*) > 1)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'coach_rating_race_summary: PK duplicates', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM (SELECT rating_system, coach_id, race_id FROM naf_catalog.gold_summary.coach_rating_race_summary GROUP BY rating_system, coach_id, race_id HAVING COUNT(*) > 1)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'coach_race_summary: PK duplicates', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM (SELECT rating_system, coach_id, race_id FROM naf_catalog.gold_summary.coach_race_summary GROUP BY rating_system, coach_id, race_id HAVING COUNT(*) > 1)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'coach_opponent_summary: PK duplicates', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM (SELECT rating_system, coach_id, opponent_coach_id FROM naf_catalog.gold_summary.coach_opponent_summary GROUP BY rating_system, coach_id, opponent_coach_id HAVING COUNT(*) > 1)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'coach_streak_overview: PK duplicates', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM (SELECT scope, coach_id, scope_race_id FROM naf_catalog.gold_summary.coach_streak_overview GROUP BY scope, coach_id, scope_race_id HAVING COUNT(*) > 1)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'coach_tournament_performance_summary: PK duplicates', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM (SELECT rating_system, coach_id, tournament_id FROM naf_catalog.gold_summary.coach_tournament_performance_summary GROUP BY rating_system, coach_id, tournament_id HAVING COUNT(*) > 1)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'coach_profile: PK duplicates', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM (SELECT coach_id FROM naf_catalog.gold_presentation.coach_profile GROUP BY coach_id HAVING COUNT(*) > 1)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'coach_performance_summary: orphan coach_id', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.gold_summary.coach_performance_summary s LEFT JOIN naf_catalog.gold_dim.coach_dim d ON s.coach_id = d.coach_id WHERE d.coach_id IS NULL
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'coach_rating_global_elo_summary: orphan coach_id', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.gold_summary.coach_rating_global_elo_summary s LEFT JOIN naf_catalog.gold_dim.coach_dim d ON s.coach_id = d.coach_id WHERE d.coach_id IS NULL
# MAGIC ),
# MAGIC
# MAGIC -- -----------------------------------------------------------------
# MAGIC -- CELL 3: Phase 1 (form + opponent strength)
# MAGIC -- -----------------------------------------------------------------
# MAGIC phase1_checks AS (
# MAGIC   SELECT 'ERROR' AS severity, 'coach_form_summary: PK duplicates' AS check_name, CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM (SELECT coach_id FROM naf_catalog.gold_summary.coach_form_summary GROUP BY coach_id HAVING COUNT(*) > 1)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'coach_form_summary: table is empty', CAST(CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS BIGINT)
# MAGIC   FROM naf_catalog.gold_summary.coach_form_summary
# MAGIC   UNION ALL
# MAGIC   SELECT 'WARN', 'coach_form_summary: form_score outside [-20, +20]', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.gold_summary.coach_form_summary WHERE form_score IS NOT NULL AND (form_score < -20 OR form_score > 20)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'coach_form_summary: form_pctl outside [0, 100]', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.gold_summary.coach_form_summary WHERE form_pctl IS NOT NULL AND (form_pctl < 0 OR form_pctl > 100)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'coach_form_summary: orphan coach_id', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.gold_summary.coach_form_summary s LEFT JOIN naf_catalog.gold_dim.coach_dim d ON s.coach_id = d.coach_id WHERE d.coach_id IS NULL
# MAGIC ),
# MAGIC
# MAGIC -- -----------------------------------------------------------------
# MAGIC -- CELL 4: Nation pipeline PK/FK
# MAGIC -- -----------------------------------------------------------------
# MAGIC nation_checks AS (
# MAGIC   SELECT 'ERROR' AS severity, 'nation_coach_glo_metrics: PK duplicates' AS check_name, CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM (SELECT nation_id, coach_id FROM naf_catalog.gold_summary.nation_coach_glo_metrics GROUP BY nation_id, coach_id HAVING COUNT(*) > 1)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'nation_overview_summary: PK duplicates', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM (SELECT nation_id FROM naf_catalog.gold_summary.nation_overview_summary GROUP BY nation_id HAVING COUNT(*) > 1)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'nation_race_summary: PK duplicates', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM (SELECT nation_id, race_id FROM naf_catalog.gold_summary.nation_race_summary GROUP BY nation_id, race_id HAVING COUNT(*) > 1)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'nation_vs_nation_summary: PK duplicates', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM (SELECT nation_id, opponent_nation_id FROM naf_catalog.gold_summary.nation_vs_nation_summary GROUP BY nation_id, opponent_nation_id HAVING COUNT(*) > 1)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'nation_rivalry_summary: PK duplicates', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM (SELECT nation_id, opponent_nation_id FROM naf_catalog.gold_summary.nation_rivalry_summary GROUP BY nation_id, opponent_nation_id HAVING COUNT(*) > 1)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'nation_glo_metric_quantiles: PK duplicates', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM (SELECT nation_id, metric_type FROM naf_catalog.gold_summary.nation_glo_metric_quantiles GROUP BY nation_id, metric_type HAVING COUNT(*) > 1)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'nation_race_elo_peak_summary: PK duplicates', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM (SELECT nation_id, race_id FROM naf_catalog.gold_summary.nation_race_elo_peak_summary GROUP BY nation_id, race_id HAVING COUNT(*) > 1)
# MAGIC   UNION ALL
# MAGIC   -- REMOVED: nation_overview_comparison_summary (table dropped)
# MAGIC   SELECT 'ERROR', 'nation_coach_glo_metrics: orphan nation_id', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.gold_summary.nation_coach_glo_metrics s LEFT JOIN naf_catalog.gold_dim.nation_dim d ON s.nation_id = d.nation_id WHERE d.nation_id IS NULL
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'nation_coach_glo_metrics: orphan coach_id', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.gold_summary.nation_coach_glo_metrics s LEFT JOIN naf_catalog.gold_dim.coach_dim d ON s.coach_id = d.coach_id WHERE d.coach_id IS NULL
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'nation_glo_metric_quantiles: non-uppercase metric_type', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.gold_summary.nation_glo_metric_quantiles WHERE metric_type <> UPPER(metric_type)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'nation_rivalry_summary: should be TABLE not VIEW', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.information_schema.tables WHERE table_schema = 'gold_summary' AND table_name = 'nation_rivalry_summary' AND table_type = 'VIEW'
# MAGIC ),
# MAGIC
# MAGIC -- -----------------------------------------------------------------
# MAGIC -- CELL 5: Phase 2 (nation cumulative series)
# MAGIC -- -----------------------------------------------------------------
# MAGIC phase2_checks AS (
# MAGIC   SELECT 'ERROR' AS severity, 'nation_results_cumulative_series: PK duplicates' AS check_name, CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM (SELECT nation_id, game_sequence_number FROM naf_catalog.gold_summary.nation_results_cumulative_series GROUP BY nation_id, game_sequence_number HAVING COUNT(*) > 1)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'nation_results_cumulative_series: table is empty', CAST(CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS BIGINT)
# MAGIC   FROM naf_catalog.gold_summary.nation_results_cumulative_series
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'nation_results_cumulative_series: cum_wins+draws+losses != cum_games', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.gold_summary.nation_results_cumulative_series WHERE (cum_wins + cum_draws + cum_losses) <> cum_games
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'nation_results_cumulative_series: intra-nation game found', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.gold_summary.nation_results_cumulative_series WHERE opponent_nation_id = nation_id
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'nation_results_cumulative_series: Unknown nation_id=0 found', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.gold_summary.nation_results_cumulative_series WHERE nation_id = 0 OR opponent_nation_id = 0
# MAGIC ),
# MAGIC
# MAGIC -- -----------------------------------------------------------------
# MAGIC -- CELL 6: Phase 3 (race strength comparison)
# MAGIC -- -----------------------------------------------------------------
# MAGIC phase3_checks AS (
# MAGIC   SELECT 'ERROR' AS severity, 'world_race_elo_quantiles: view is empty' AS check_name,
# MAGIC          CAST(CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS BIGINT) AS fail_rows
# MAGIC   FROM naf_catalog.gold_summary.world_race_elo_quantiles
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'world_race_elo_quantiles: PK duplicates', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM (SELECT race_id FROM naf_catalog.gold_summary.world_race_elo_quantiles GROUP BY race_id HAVING COUNT(*) > 1)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'world_race_elo_quantiles: quantile ordering violated', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.gold_summary.world_race_elo_quantiles
# MAGIC   WHERE elo_peak_p10 > elo_peak_p25 OR elo_peak_p25 > elo_peak_p50 OR elo_peak_p50 > elo_peak_p75 OR elo_peak_p75 > elo_peak_p90
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'nation_race_strength_comparison: view is empty',
# MAGIC          CAST(CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS BIGINT)
# MAGIC   FROM naf_catalog.gold_presentation.nation_race_strength_comparison
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'nation_race_strength_comparison: missing NATION scope',
# MAGIC          CAST(CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS BIGINT)
# MAGIC   FROM naf_catalog.gold_presentation.nation_race_strength_comparison WHERE scope = 'NATION'
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'nation_race_strength_comparison: missing WORLD scope',
# MAGIC          CAST(CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS BIGINT)
# MAGIC   FROM naf_catalog.gold_presentation.nation_race_strength_comparison WHERE scope = 'WORLD'
# MAGIC ),
# MAGIC
# MAGIC -- -----------------------------------------------------------------
# MAGIC -- CELL 7: Phase 4 (home/abroad + specialist badges)
# MAGIC -- -----------------------------------------------------------------
# MAGIC phase4_checks AS (
# MAGIC   SELECT 'ERROR' AS severity, 'nation_domestic_summary: PK duplicates' AS check_name, CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM (SELECT nation_id FROM naf_catalog.gold_summary.nation_domestic_summary GROUP BY nation_id HAVING COUNT(*) > 1)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'nation_domestic_summary: table is empty', CAST(CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS BIGINT)
# MAGIC   FROM naf_catalog.gold_summary.nation_domestic_summary
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'nation_domestic_summary: orphan nation_id', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.gold_summary.nation_domestic_summary s LEFT JOIN naf_catalog.gold_dim.nation_dim d ON s.nation_id = d.nation_id WHERE d.nation_id IS NULL
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'coach_race_relative_strength: PK duplicates', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM (SELECT coach_id, race_id FROM naf_catalog.gold_summary.coach_race_relative_strength GROUP BY coach_id, race_id HAVING COUNT(*) > 1)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'coach_race_relative_strength: table is empty', CAST(CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS BIGINT)
# MAGIC   FROM naf_catalog.gold_summary.coach_race_relative_strength
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'coach_race_relative_strength: orphan coach_id', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.gold_summary.coach_race_relative_strength s LEFT JOIN naf_catalog.gold_dim.coach_dim d ON s.coach_id = d.coach_id WHERE d.coach_id IS NULL
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'coach_race_relative_strength: orphan race_id', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.gold_summary.coach_race_relative_strength s LEFT JOIN naf_catalog.gold_dim.race_dim d ON s.race_id = d.race_id WHERE d.race_id IS NULL
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'coach_race_nation_rank: PK duplicates', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM (SELECT coach_id, race_id FROM naf_catalog.gold_summary.coach_race_nation_rank GROUP BY coach_id, race_id HAVING COUNT(*) > 1)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'coach_race_nation_rank: table is empty', CAST(CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS BIGINT)
# MAGIC   FROM naf_catalog.gold_summary.coach_race_nation_rank
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'coach_race_nation_rank: orphan coach_id', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.gold_summary.coach_race_nation_rank s LEFT JOIN naf_catalog.gold_dim.coach_dim d ON s.coach_id = d.coach_id WHERE d.coach_id IS NULL
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'coach_race_nation_rank: orphan nation_id', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.gold_summary.coach_race_nation_rank s LEFT JOIN naf_catalog.gold_dim.nation_dim d ON s.nation_id = d.nation_id WHERE d.nation_id IS NULL
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'coach_race_nation_rank: Unknown nation_id=0 found', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.gold_summary.coach_race_nation_rank WHERE nation_id = 0
# MAGIC ),
# MAGIC
# MAGIC -- -----------------------------------------------------------------
# MAGIC -- CELL: Phase 5 (opponent-adjusted win rates)
# MAGIC -- -----------------------------------------------------------------
# MAGIC phase5_checks AS (
# MAGIC   SELECT 'ERROR' AS severity, 'nation_opponent_elo_bin_wdl: PK duplicates' AS check_name, CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM (SELECT nation_id, metric_type, bin_index FROM naf_catalog.gold_summary.nation_opponent_elo_bin_wdl GROUP BY nation_id, metric_type, bin_index HAVING COUNT(*) > 1)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'nation_opponent_elo_bin_wdl: table is empty', CAST(CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS BIGINT)
# MAGIC   FROM naf_catalog.gold_summary.nation_opponent_elo_bin_wdl
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'nation_opponent_elo_bin_wdl: wins+draws+losses != games', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.gold_summary.nation_opponent_elo_bin_wdl WHERE games > 0 AND (wins + draws + losses) <> games
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'nation_opponent_elo_bin_wdl: ppg outside [0,1]', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.gold_summary.nation_opponent_elo_bin_wdl WHERE ppg IS NOT NULL AND (ppg < 0 OR ppg > 1)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'nation_opponent_elo_bin_wdl: Unknown nation_id=0', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.gold_summary.nation_opponent_elo_bin_wdl WHERE nation_id = 0
# MAGIC ),
# MAGIC
# MAGIC -- -----------------------------------------------------------------
# MAGIC -- CELL: Phase 6 (team selector + nation power ranking)
# MAGIC -- -----------------------------------------------------------------
# MAGIC phase6_checks AS (
# MAGIC   -- nation_team_candidate_scores
# MAGIC   SELECT 'ERROR' AS severity, 'nation_team_candidate_scores: PK duplicates' AS check_name, CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM (SELECT nation_id, coach_id FROM naf_catalog.gold_summary.nation_team_candidate_scores GROUP BY nation_id, coach_id HAVING COUNT(*) > 1)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'nation_team_candidate_scores: table is empty', CAST(CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS BIGINT)
# MAGIC   FROM naf_catalog.gold_summary.nation_team_candidate_scores
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'nation_team_candidate_scores: selector_score < 1 (impossible)', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.gold_summary.nation_team_candidate_scores
# MAGIC   WHERE selector_score < 1
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'nation_team_candidate_scores: Unknown nation_id=0 found', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.gold_summary.nation_team_candidate_scores WHERE nation_id = 0
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'nation_team_candidate_scores: orphan coach_id', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.gold_summary.nation_team_candidate_scores s
# MAGIC   LEFT JOIN naf_catalog.gold_dim.coach_dim d ON s.coach_id = d.coach_id WHERE d.coach_id IS NULL
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'nation_team_candidate_scores: orphan nation_id', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.gold_summary.nation_team_candidate_scores s
# MAGIC   LEFT JOIN naf_catalog.gold_dim.nation_dim d ON s.nation_id = d.nation_id WHERE d.nation_id IS NULL
# MAGIC   UNION ALL
# MAGIC   SELECT 'WARN', 'nation_team_candidate_scores: no nation has selector_rank_national=1', CAST(CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS BIGINT)
# MAGIC   FROM naf_catalog.gold_summary.nation_team_candidate_scores WHERE selector_rank_national = 1
# MAGIC   UNION ALL
# MAGIC   -- nation_power_ranking
# MAGIC   SELECT 'ERROR', 'nation_power_ranking: PK duplicates', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM (SELECT nation_id FROM naf_catalog.gold_summary.nation_power_ranking GROUP BY nation_id HAVING COUNT(*) > 1)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'nation_power_ranking: table is empty', CAST(CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS BIGINT)
# MAGIC   FROM naf_catalog.gold_summary.nation_power_ranking
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'nation_power_ranking: power_rank=1 missing', CAST(CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS BIGINT)
# MAGIC   FROM naf_catalog.gold_summary.nation_power_ranking WHERE power_rank = 1
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'nation_power_ranking: orphan nation_id', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.gold_summary.nation_power_ranking s
# MAGIC   LEFT JOIN naf_catalog.gold_dim.nation_dim d ON s.nation_id = d.nation_id WHERE d.nation_id IS NULL
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'nation_power_ranking: top_8_avg_selector_score < 1 (impossible)', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.gold_summary.nation_power_ranking
# MAGIC   WHERE top_8_avg_selector_score < 1
# MAGIC   UNION ALL
# MAGIC   -- nation_elite_rivalry_summary
# MAGIC   SELECT 'ERROR', 'nation_elite_rivalry_summary: PK duplicates', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM (SELECT nation_id, opponent_nation_id FROM naf_catalog.gold_summary.nation_elite_rivalry_summary GROUP BY nation_id, opponent_nation_id HAVING COUNT(*) > 1)
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'nation_elite_rivalry_summary: table is empty', CAST(CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS BIGINT)
# MAGIC   FROM naf_catalog.gold_summary.nation_elite_rivalry_summary
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'nation_elite_rivalry_summary: rivalry_score < 1 (impossible)', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM naf_catalog.gold_summary.nation_elite_rivalry_summary
# MAGIC   WHERE rivalry_score < 1
# MAGIC ),
# MAGIC
# MAGIC -- -----------------------------------------------------------------
# MAGIC -- CELL 8: Rating system allowed values
# MAGIC -- -----------------------------------------------------------------
# MAGIC rating_checks AS (
# MAGIC   SELECT 'ERROR' AS severity, 'rating_history_fact: unexpected rating_system value' AS check_name, CAST(COUNT(*) AS BIGINT) AS fail_rows
# MAGIC   FROM (SELECT DISTINCT rating_system FROM naf_catalog.gold_fact.rating_history_fact WHERE rating_system <> 'NAF_ELO')
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'coach_rating_race_summary: unexpected rating_system value', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM (SELECT DISTINCT rating_system FROM naf_catalog.gold_summary.coach_rating_race_summary WHERE rating_system <> 'NAF_ELO')
# MAGIC   UNION ALL
# MAGIC   SELECT 'ERROR', 'coach_rating_global_elo_summary: unexpected rating_system value', CAST(COUNT(*) AS BIGINT)
# MAGIC   FROM (SELECT DISTINCT rating_system FROM naf_catalog.gold_summary.coach_rating_global_elo_summary WHERE rating_system <> 'NAF_ELO')
# MAGIC ),
# MAGIC
# MAGIC -- -----------------------------------------------------------------
# MAGIC -- COMBINE ALL
# MAGIC -- -----------------------------------------------------------------
# MAGIC all_results AS (
# MAGIC   SELECT * FROM object_existence
# MAGIC   UNION ALL SELECT * FROM core_checks
# MAGIC   UNION ALL SELECT * FROM coach_checks
# MAGIC   UNION ALL SELECT * FROM phase1_checks
# MAGIC   UNION ALL SELECT * FROM nation_checks
# MAGIC   UNION ALL SELECT * FROM phase2_checks
# MAGIC   UNION ALL SELECT * FROM phase3_checks
# MAGIC   UNION ALL SELECT * FROM phase4_checks
# MAGIC   UNION ALL SELECT * FROM phase5_checks
# MAGIC   UNION ALL SELECT * FROM phase6_checks
# MAGIC   UNION ALL SELECT * FROM rating_checks
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   severity,
# MAGIC   check_name,
# MAGIC   fail_rows,
# MAGIC   CASE WHEN fail_rows = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
# MAGIC   CURRENT_TIMESTAMP() AS run_timestamp
# MAGIC FROM all_results
# MAGIC ORDER BY
# MAGIC   CASE severity WHEN 'ERROR' THEN 1 WHEN 'WARN' THEN 2 ELSE 3 END,
# MAGIC   status DESC,  -- FAILs first
# MAGIC   check_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS naf_catalog.gold_summary.exports;

# COMMAND ----------

# DBTITLE 1,Export test report to CSV
import datetime
from pyspark.sql import functions as F

df = spark.table("naf_catalog.gold_summary.test_report")
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")

# Write to a folder in a UC Volume (do NOT end the folder name with .csv)
out_dir = f"/Volumes/naf_catalog/gold_summary/exports/naf_test_report_{timestamp}"
(df.coalesce(1)
  .write.mode("overwrite")
  .option("header", "true")
  .csv(out_dir))

# Copy the single part file to a nice name (still inside the same Volume)
part = [f.path for f in dbutils.fs.ls(out_dir) if f.name.startswith("part-") and f.name.endswith(".csv")][0]
final_csv = f"/Volumes/naf_catalog/gold_summary/exports/naf_test_report_{timestamp}.csv"
dbutils.fs.cp(part, final_csv, True)

# Summary (single pass)
summary = (df.agg(
    F.count("*").alias("total"),
    F.sum(F.when(F.col("status") == "FAIL", 1).otherwise(0)).alias("failures"),
    F.sum(F.when((F.col("status") == "FAIL") & (F.col("severity") == "WARN"), 1).otherwise(0)).alias("warnings"),
    F.sum(F.when((F.col("status") == "FAIL") & (F.col("severity") == "ERROR"), 1).otherwise(0)).alias("errors"),
).collect()[0])

print(f"Test report: {summary['total']} checks | {summary['failures']} failures ({summary['errors']} ERROR, {summary['warnings']} WARN)")
print(f"CSV written to: {final_csv}")

df.filter(F.col("status") == "FAIL").show(100, truncate=False)
