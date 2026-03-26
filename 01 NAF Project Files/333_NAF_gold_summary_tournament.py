# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_summary.tournament_winners AS
# MAGIC
# MAGIC -- 1) Identify the stat_id corresponding to "Winner"
# MAGIC WITH winner_stat AS (
# MAGIC   SELECT stat_id
# MAGIC   FROM naf_catalog.gold_dim.tournament_stat_dim
# MAGIC   WHERE LOWER(stat_name) = 'winner'
# MAGIC ),
# MAGIC
# MAGIC -- 2) Extract all winner rows from the tournament statistics fact table
# MAGIC winner_rows AS (
# MAGIC   SELECT
# MAGIC     s.tournament_id,
# MAGIC     s.coach_id    AS winner_coach_id,
# MAGIC     s.race_id     AS winner_race_id,
# MAGIC     s.date_event  AS winner_date
# MAGIC   FROM naf_catalog.gold_fact.tournament_statistics_fact s
# MAGIC   INNER JOIN winner_stat w
# MAGIC     ON s.stat_id = w.stat_id
# MAGIC ),
# MAGIC
# MAGIC -- 3) Optional enrichment with dimensions for labels
# MAGIC final AS (
# MAGIC   SELECT
# MAGIC     w.tournament_id,
# MAGIC     td.tournament_name,
# MAGIC     td.tournament_nation_display,
# MAGIC
# MAGIC     w.winner_coach_id,
# MAGIC     cd.coach_name    AS winner_coach_name,
# MAGIC
# MAGIC     w.winner_race_id,
# MAGIC     rd.race_name     AS winner_race_name,
# MAGIC
# MAGIC     w.winner_date
# MAGIC   FROM winner_rows w
# MAGIC   LEFT JOIN naf_catalog.gold_dim.tournament_dim td
# MAGIC     ON w.tournament_id = td.tournament_id
# MAGIC   LEFT JOIN naf_catalog.gold_dim.coach_dim cd
# MAGIC     ON w.winner_coach_id = cd.coach_id
# MAGIC   LEFT JOIN naf_catalog.gold_dim.race_dim rd
# MAGIC     ON w.winner_race_id = rd.race_id
# MAGIC )
# MAGIC
# MAGIC SELECT *
# MAGIC FROM final;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_summary.tournament_summary AS
# MAGIC
# MAGIC -- Base coach-games
# MAGIC WITH base_games AS (
# MAGIC   SELECT
# MAGIC     tournament_id,
# MAGIC     game_id,
# MAGIC     date_as_timestamp,
# MAGIC     coach_id,
# MAGIC     coach_race_id,
# MAGIC     result_numeric
# MAGIC   FROM naf_catalog.gold_fact.coach_games_fact
# MAGIC ),
# MAGIC
# MAGIC -- Distinct participants per tournament (one row per coach)
# MAGIC participants AS (
# MAGIC   SELECT DISTINCT
# MAGIC     tournament_id,
# MAGIC     coach_id,
# MAGIC     coach_race_id
# MAGIC   FROM base_games
# MAGIC ),
# MAGIC
# MAGIC -- Attach nations to participants
# MAGIC participants_with_nation AS (
# MAGIC   SELECT
# MAGIC     p.tournament_id,
# MAGIC     p.coach_id,
# MAGIC     p.coach_race_id,
# MAGIC     c.coach_nation_representation
# MAGIC   FROM participants p
# MAGIC   LEFT JOIN naf_catalog.gold_dim.coach_dim c
# MAGIC     ON p.coach_id = c.coach_id
# MAGIC ),
# MAGIC
# MAGIC -- Per-tournament race counts (coaches per race)
# MAGIC race_counts AS (
# MAGIC   SELECT
# MAGIC     tournament_id,
# MAGIC     coach_race_id,
# MAGIC     COUNT(*) AS participants_count_race
# MAGIC   FROM participants_with_nation
# MAGIC   GROUP BY tournament_id, coach_race_id
# MAGIC ),
# MAGIC
# MAGIC -- Race entropy (Shannon) per tournament, based on coach counts per race
# MAGIC race_entropy AS (
# MAGIC   SELECT
# MAGIC     tournament_id,
# MAGIC     -SUM(p * LN(p)) AS race_entropy
# MAGIC   FROM (
# MAGIC     SELECT
# MAGIC       tournament_id,
# MAGIC       coach_race_id,
# MAGIC       participants_count_race,
# MAGIC       CAST(participants_count_race AS DOUBLE) /
# MAGIC         SUM(participants_count_race) OVER (PARTITION BY tournament_id) AS p
# MAGIC     FROM race_counts
# MAGIC   ) x
# MAGIC   GROUP BY tournament_id
# MAGIC ),
# MAGIC
# MAGIC -- Race Gini coefficient per tournament (inequality of race usage)
# MAGIC race_gini AS (
# MAGIC   SELECT
# MAGIC     tournament_id,
# MAGIC     CASE
# MAGIC       WHEN MIN(sum_x) = 0 OR MIN(race_count) = 0 THEN NULL
# MAGIC       ELSE
# MAGIC         2.0 * SUM(rn * participants_count_race) / (MIN(race_count) * MIN(sum_x))
# MAGIC         - (MIN(race_count) + 1.0) / MIN(race_count)
# MAGIC     END AS race_gini
# MAGIC   FROM (
# MAGIC     SELECT
# MAGIC       tournament_id,
# MAGIC       coach_race_id,
# MAGIC       participants_count_race,
# MAGIC       ROW_NUMBER() OVER (
# MAGIC         PARTITION BY tournament_id
# MAGIC         ORDER BY participants_count_race
# MAGIC       ) AS rn,
# MAGIC       COUNT(*) OVER (PARTITION BY tournament_id)              AS race_count,
# MAGIC       SUM(participants_count_race) OVER (PARTITION BY tournament_id) AS sum_x
# MAGIC     FROM race_counts
# MAGIC   ) g
# MAGIC   GROUP BY tournament_id
# MAGIC ),
# MAGIC
# MAGIC -- Tournament-level participation & nations
# MAGIC tournament_participation AS (
# MAGIC   SELECT
# MAGIC     tournament_id,
# MAGIC     COUNT(DISTINCT coach_id)      AS participants_count,
# MAGIC     COUNT(DISTINCT coach_race_id) AS races_count,
# MAGIC
# MAGIC     -- count NULL as its own category
# MAGIC     COUNT(DISTINCT COALESCE(coach_nation_representation, 'UNKNOWN')) AS nations_count
# MAGIC   FROM participants_with_nation
# MAGIC   GROUP BY tournament_id
# MAGIC ),
# MAGIC
# MAGIC -- Tournament-level games & time span
# MAGIC tournament_games AS (
# MAGIC   SELECT
# MAGIC     tournament_id,
# MAGIC     COUNT(DISTINCT game_id) AS games_count,
# MAGIC     MIN(date_as_timestamp)  AS first_game_timestamp,
# MAGIC     MAX(date_as_timestamp)  AS last_game_timestamp
# MAGIC   FROM base_games
# MAGIC   GROUP BY tournament_id
# MAGIC ),
# MAGIC
# MAGIC -- Avg all-time peak GLO of participants
# MAGIC avg_glo_peak AS (
# MAGIC   SELECT
# MAGIC     p.tournament_id,
# MAGIC     AVG(crg.glo_peak) AS glo_peak_mean
# MAGIC   FROM participants p
# MAGIC   LEFT JOIN naf_catalog.gold_summary.coach_rating_global_summary crg
# MAGIC     ON p.coach_id = crg.coach_id
# MAGIC   GROUP BY p.tournament_id
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   g.tournament_id,
# MAGIC
# MAGIC   -- volumes
# MAGIC   g.games_count,
# MAGIC   p.participants_count,
# MAGIC   p.races_count,
# MAGIC   p.nations_count,
# MAGIC
# MAGIC   -- diversity
# MAGIC   re.race_entropy,
# MAGIC   rg.race_gini,
# MAGIC
# MAGIC   -- winner info (from tournament_winners)
# MAGIC   w.winner_coach_id,
# MAGIC   w.winner_race_id,
# MAGIC
# MAGIC   -- participant strength
# MAGIC   apg.glo_peak_mean,
# MAGIC
# MAGIC   -- timing
# MAGIC   g.first_game_timestamp,
# MAGIC   g.last_game_timestamp
# MAGIC
# MAGIC FROM tournament_games g
# MAGIC LEFT JOIN tournament_participation p
# MAGIC   ON g.tournament_id = p.tournament_id
# MAGIC LEFT JOIN race_entropy re
# MAGIC   ON g.tournament_id = re.tournament_id
# MAGIC LEFT JOIN race_gini rg
# MAGIC   ON g.tournament_id = rg.tournament_id
# MAGIC LEFT JOIN naf_catalog.gold_summary.tournament_winners w
# MAGIC   ON g.tournament_id = w.tournament_id
# MAGIC LEFT JOIN avg_glo_peak apg
# MAGIC   ON g.tournament_id = apg.tournament_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_summary.tournament_race_summary AS
# MAGIC
# MAGIC WITH base_games AS (
# MAGIC   SELECT
# MAGIC     tournament_id,
# MAGIC     game_id,
# MAGIC     coach_id,
# MAGIC     coach_race_id,
# MAGIC     result_numeric
# MAGIC   FROM naf_catalog.gold_fact.coach_games_fact
# MAGIC ),
# MAGIC
# MAGIC -- total participants and games per tournament (for percentage calculations)
# MAGIC tournament_counts AS (
# MAGIC   SELECT
# MAGIC     tournament_id,
# MAGIC     COUNT(DISTINCT coach_id) AS participants_count,
# MAGIC     COUNT(DISTINCT game_id)  AS games_count
# MAGIC   FROM base_games
# MAGIC   GROUP BY tournament_id
# MAGIC ),
# MAGIC
# MAGIC -- counts per tournament × race
# MAGIC race_counts AS (
# MAGIC   SELECT
# MAGIC     tournament_id,
# MAGIC     coach_race_id,
# MAGIC     COUNT(DISTINCT coach_id) AS participants_count_race,
# MAGIC     COUNT(*)                 AS games_count_race,
# MAGIC     AVG(result_numeric)      AS points_per_game_race
# MAGIC   FROM base_games
# MAGIC   GROUP BY tournament_id, coach_race_id
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   rc.tournament_id,
# MAGIC   rc.coach_race_id,
# MAGIC
# MAGIC   rc.participants_count_race,
# MAGIC   rc.games_count_race,
# MAGIC   rc.points_per_game_race,
# MAGIC
# MAGIC   -- percentages (0–100, per style guide)
# MAGIC   100.0 * rc.participants_count_race / tc.participants_count AS participants_pct_race,
# MAGIC   100.0 * rc.games_count_race        / tc.games_count        AS games_pct_race
# MAGIC
# MAGIC FROM race_counts rc
# MAGIC JOIN tournament_counts tc
# MAGIC   ON rc.tournament_id = tc.tournament_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Global race summary across ALL games
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_summary.global_race_summary AS
# MAGIC
# MAGIC WITH base_games AS (
# MAGIC   SELECT
# MAGIC     game_id,
# MAGIC     coach_id,
# MAGIC     coach_race_id,
# MAGIC     result_numeric
# MAGIC   FROM naf_catalog.gold_fact.coach_games_fact
# MAGIC ),
# MAGIC
# MAGIC -- total participants and games across ALL tournaments
# MAGIC global_counts AS (
# MAGIC   SELECT
# MAGIC     COUNT(DISTINCT coach_id) AS participants_count,
# MAGIC     COUNT(DISTINCT game_id)  AS games_count
# MAGIC   FROM base_games
# MAGIC ),
# MAGIC
# MAGIC -- counts per race globally
# MAGIC race_counts AS (
# MAGIC   SELECT
# MAGIC     coach_race_id,
# MAGIC     COUNT(DISTINCT coach_id) AS participants_count_race,
# MAGIC     COUNT(*)                 AS games_count_race,
# MAGIC     AVG(result_numeric)      AS points_per_game
# MAGIC   FROM base_games
# MAGIC   GROUP BY coach_race_id
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   rc.coach_race_id,
# MAGIC
# MAGIC   rc.participants_count_race,
# MAGIC   rc.games_count_race,
# MAGIC   rc.points_per_game,
# MAGIC
# MAGIC   -- global percentages (0–100, per style guide)
# MAGIC   100.0 * rc.participants_count_race / gc.participants_count AS participants_pct,
# MAGIC   100.0 * rc.games_count_race        / gc.games_count        AS games_pct
# MAGIC
# MAGIC FROM race_counts rc
# MAGIC CROSS JOIN global_counts gc;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE naf_catalog.gold_summary.tournament_nation_summary AS
# MAGIC
# MAGIC WITH base_games AS (
# MAGIC   SELECT
# MAGIC     tournament_id,
# MAGIC     game_id,
# MAGIC     coach_id,
# MAGIC     result_numeric
# MAGIC   FROM naf_catalog.gold_fact.coach_games_fact
# MAGIC ),
# MAGIC
# MAGIC -- attach nation to each coach-game row
# MAGIC games_with_nation AS (
# MAGIC   SELECT
# MAGIC     g.tournament_id,
# MAGIC     g.game_id,
# MAGIC     g.coach_id,
# MAGIC     g.result_numeric,
# MAGIC     COALESCE(c.coach_nation_representation, 'UNKNOWN') AS nation_name
# MAGIC   FROM base_games g
# MAGIC   LEFT JOIN naf_catalog.gold_dim.coach_dim c
# MAGIC     ON g.coach_id = c.coach_id
# MAGIC ),
# MAGIC
# MAGIC -- overall tournament totals (for percentages)
# MAGIC tournament_totals AS (
# MAGIC   SELECT
# MAGIC     tournament_id,
# MAGIC     COUNT(DISTINCT coach_id) AS participants_count,
# MAGIC     COUNT(DISTINCT game_id)  AS games_count
# MAGIC   FROM games_with_nation
# MAGIC   GROUP BY tournament_id
# MAGIC ),
# MAGIC
# MAGIC -- nation-level stats per tournament
# MAGIC nation_counts AS (
# MAGIC   SELECT
# MAGIC     tournament_id,
# MAGIC     nation_name,
# MAGIC     COUNT(DISTINCT coach_id) AS participants_count_nation,
# MAGIC     COUNT(*)                 AS games_count_nation,
# MAGIC     AVG(result_numeric)      AS points_per_game
# MAGIC   FROM games_with_nation
# MAGIC   GROUP BY tournament_id, nation_name
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   nc.tournament_id,
# MAGIC   nc.nation_name,
# MAGIC
# MAGIC   nc.participants_count_nation,
# MAGIC   nc.games_count_nation,
# MAGIC   nc.points_per_game,
# MAGIC
# MAGIC   -- percentages (0–100, per style guide)
# MAGIC   100.0 * nc.participants_count_nation / tt.participants_count AS participants_pct,
# MAGIC   100.0 * nc.games_count_nation        / tt.games_count        AS games_pct
# MAGIC
# MAGIC FROM nation_counts nc
# MAGIC JOIN tournament_totals tt
# MAGIC   ON nc.tournament_id = tt.tournament_id;
# MAGIC
