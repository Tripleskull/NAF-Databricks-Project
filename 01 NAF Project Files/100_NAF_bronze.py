# Databricks notebook source
# MAGIC %md
# MAGIC # 100 — Bronze Layer
# MAGIC
# MAGIC **Layer:** BRONZE &nbsp;|&nbsp; **Status:** Production
# MAGIC **Pipeline position:** First notebook in the pipeline
# MAGIC
# MAGIC Raw source truth (snapshot/overwrite). One Delta table per source feed. No business logic.
# MAGIC
# MAGIC **References:** `NAF_Design_Specification.md` (architecture + naming rules), `style_guides.md`
# MAGIC
# MAGIC ## Bronze rules
# MAGIC - One table per source feed (`*_raw`), stored as Delta
# MAGIC - Technical column sanitisation only — keep all fields, no semantic renames
# MAGIC - Allow `_rescued_data` for schema evolution
# MAGIC - No dedup, filtering, domain logic, or reshaping
# MAGIC - Required metadata: `ingest_timestamp`, `ingest_source`, `file_name`

# COMMAND ----------

# DBTITLE 1,Create schema bronze
# MAGIC %sql
# MAGIC
# MAGIC -- DROP SCHEMA IF EXISTS naf_catalog.bronze CASCADE;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS naf_catalog.bronze;

# COMMAND ----------

# DBTITLE 1,bronze.coach_export_raw
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE naf_catalog.bronze.coach_export_raw
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC   `NAF Nr`            AS `NAF_Nr`,
# MAGIC   `NAF name`          AS `NAF_name`,
# MAGIC   Country             AS Country,
# MAGIC   `Registration Date` AS `Registration_Date`,
# MAGIC   `Expiry Date`       AS `Expiry_Date`,
# MAGIC   _rescued_data,
# MAGIC
# MAGIC   current_timestamp() AS ingest_timestamp,
# MAGIC   input_file_name()   AS ingest_source,
# MAGIC   regexp_extract(input_file_name(), '[^/]+$', 0) AS file_name
# MAGIC FROM read_files(
# MAGIC   '/Volumes/naf_catalog/bronze/naf_raw/CoachExport.csv',
# MAGIC   format                   => 'csv',
# MAGIC   header                   => true,
# MAGIC   sep                      => ';',
# MAGIC   quote                    => '"',
# MAGIC   escape                   => '"',
# MAGIC   multiLine                => true,
# MAGIC   ignoreLeadingWhiteSpace  => true,
# MAGIC   ignoreTrailingWhiteSpace => true,
# MAGIC   rescuedDataColumn        => '_rescued_data',
# MAGIC   enforceSchema            => false
# MAGIC );
# MAGIC

# COMMAND ----------

# DBTITLE 1,bronze.coach_ranking_variant_raw
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE naf_catalog.bronze.coach_ranking_variant_raw
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC   coachID,
# MAGIC   raceID,
# MAGIC   variantID,
# MAGIC   dateUpdate,
# MAGIC   ranking,
# MAGIC   ranking_temp,
# MAGIC   _rescued_data,
# MAGIC
# MAGIC   current_timestamp() AS ingest_timestamp,
# MAGIC   input_file_name()   AS ingest_source,
# MAGIC   regexp_extract(input_file_name(), '[^/]+$', 0) AS file_name
# MAGIC FROM read_files(
# MAGIC   '/Volumes/naf_catalog/bronze/naf_raw/naf_coachranking_variant.csv',
# MAGIC   format                    => 'csv',
# MAGIC   header                    => true,
# MAGIC   sep                       => ';',
# MAGIC   quote                     => '"',
# MAGIC   escape                    => '"',
# MAGIC   multiLine                 => true,
# MAGIC   ignoreLeadingWhiteSpace   => true,
# MAGIC   ignoreTrailingWhiteSpace  => true,
# MAGIC   rescuedDataColumn         => '_rescued_data',
# MAGIC   enforceSchema             => false,
# MAGIC   schema => '
# MAGIC     coachID STRING,
# MAGIC     raceID STRING,
# MAGIC     variantID STRING,
# MAGIC     dateUpdate STRING,
# MAGIC     ranking STRING,
# MAGIC     ranking_temp STRING
# MAGIC   '
# MAGIC );
# MAGIC

# COMMAND ----------

# DBTITLE 1,bronze.game_raw
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE naf_catalog.bronze.game_raw
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC   gameid,
# MAGIC   seasonid,
# MAGIC   tournamentid,
# MAGIC   homecoachid,
# MAGIC   awaycoachid,
# MAGIC   racehome,
# MAGIC   raceaway,
# MAGIC   trhome,
# MAGIC   traway,
# MAGIC   rephome,
# MAGIC   repaway,
# MAGIC   rephome_calibrated,
# MAGIC   repaway_calibrated,
# MAGIC   dirty_calibrated,
# MAGIC   goalshome,
# MAGIC   goalsaway,
# MAGIC   badlyhurthome,
# MAGIC   badlyhurtaway,
# MAGIC   serioushome,
# MAGIC   seriousaway,
# MAGIC   killshome,
# MAGIC   killsaway,
# MAGIC   gate,
# MAGIC   winningshome,
# MAGIC   winningsaway,
# MAGIC   notes,
# MAGIC   date,
# MAGIC   dirty,
# MAGIC   hour,
# MAGIC   newdate,
# MAGIC   naf_variantsid,
# MAGIC   _rescued_data,
# MAGIC
# MAGIC   current_timestamp() AS ingest_timestamp,
# MAGIC   input_file_name()   AS ingest_source,
# MAGIC   regexp_extract(input_file_name(), '[^/]+$', 0) AS file_name
# MAGIC FROM read_files(
# MAGIC   '/Volumes/naf_catalog/bronze/naf_raw/naf_game.csv',
# MAGIC   format                    => 'csv',
# MAGIC   header                    => true,
# MAGIC   sep                       => ';',
# MAGIC   quote                     => '"',
# MAGIC   escape                    => '"',
# MAGIC   multiLine                 => true,
# MAGIC   ignoreLeadingWhiteSpace   => true,
# MAGIC   ignoreTrailingWhiteSpace  => true,
# MAGIC   rescuedDataColumn         => '_rescued_data',
# MAGIC   enforceSchema             => false,
# MAGIC   schema => '
# MAGIC     gameid STRING,
# MAGIC     seasonid STRING,
# MAGIC     tournamentid STRING,
# MAGIC     homecoachid STRING,
# MAGIC     awaycoachid STRING,
# MAGIC     racehome STRING,
# MAGIC     raceaway STRING,
# MAGIC     trhome STRING,
# MAGIC     traway STRING,
# MAGIC     rephome STRING,
# MAGIC     repaway STRING,
# MAGIC     rephome_calibrated STRING,
# MAGIC     repaway_calibrated STRING,
# MAGIC     dirty_calibrated STRING,
# MAGIC     goalshome STRING,
# MAGIC     goalsaway STRING,
# MAGIC     badlyhurthome STRING,
# MAGIC     badlyhurtaway STRING,
# MAGIC     serioushome STRING,
# MAGIC     seriousaway STRING,
# MAGIC     killshome STRING,
# MAGIC     killsaway STRING,
# MAGIC     gate STRING,
# MAGIC     winningshome STRING,
# MAGIC     winningsaway STRING,
# MAGIC     notes STRING,
# MAGIC     date STRING,
# MAGIC     dirty STRING,
# MAGIC     hour STRING,
# MAGIC     newdate STRING,
# MAGIC     naf_variantsid STRING
# MAGIC   '
# MAGIC );
# MAGIC

# COMMAND ----------

# DBTITLE 1,bronze.race_raw
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE naf_catalog.bronze.race_raw
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC   raceid,
# MAGIC   name,
# MAGIC   reroll_cost,
# MAGIC   apoth,
# MAGIC   `order` AS order_,        -- technical sanitisation only (reserved word)
# MAGIC   selectable,
# MAGIC   race_count,
# MAGIC   _rescued_data,
# MAGIC
# MAGIC   current_timestamp() AS ingest_timestamp,
# MAGIC   input_file_name()   AS ingest_source,
# MAGIC   regexp_extract(input_file_name(), '[^/]+$', 0) AS file_name
# MAGIC FROM read_files(
# MAGIC   '/Volumes/naf_catalog/bronze/naf_raw/naf_race.csv',
# MAGIC   format                    => 'csv',
# MAGIC   header                    => true,
# MAGIC   sep                       => ';',
# MAGIC   quote                     => '"',
# MAGIC   escape                    => '"',
# MAGIC   multiLine                 => true,
# MAGIC   ignoreLeadingWhiteSpace   => true,
# MAGIC   ignoreTrailingWhiteSpace  => true,
# MAGIC   rescuedDataColumn         => '_rescued_data',
# MAGIC   enforceSchema             => false,
# MAGIC   schema => '
# MAGIC     raceid STRING,
# MAGIC     name STRING,
# MAGIC     reroll_cost STRING,
# MAGIC     apoth STRING,
# MAGIC     `order` STRING,
# MAGIC     selectable STRING,
# MAGIC     race_count STRING
# MAGIC   '
# MAGIC );
# MAGIC

# COMMAND ----------

# DBTITLE 1,bronze.tournament_raw
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE naf_catalog.bronze.tournament_raw
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC   tournamentid,
# MAGIC   tournamentorganizerid,
# MAGIC   tournamentname,
# MAGIC   tournamentaddress1,
# MAGIC   tournamentaddress2,
# MAGIC   tournamentcity,
# MAGIC   tournamentstate,
# MAGIC   tournamentzip,
# MAGIC   tournamentnation,
# MAGIC   tournamenturl,
# MAGIC   tournamentnotesurl,
# MAGIC   tournamentstartdate,
# MAGIC   tournamentenddate,
# MAGIC   tournamenttype,
# MAGIC   tournamentstyle,
# MAGIC   tournamentscoring,
# MAGIC   tournamentcost,
# MAGIC   tournamentnaffee,
# MAGIC   tournamentnafdiscount,
# MAGIC   tournamentinformation,
# MAGIC   tournamentcontact,
# MAGIC   tournamentemail,
# MAGIC   tournamentorg,
# MAGIC   tournamentstatus,
# MAGIC   tournamentmajor,
# MAGIC   geolongitude,
# MAGIC   geolattitude,
# MAGIC   tournamentreport,
# MAGIC   subscription_closed,
# MAGIC   naf_rulesetid,
# MAGIC   naf_variantsid,
# MAGIC   variant_notes,
# MAGIC   variantstatus,
# MAGIC   tournament_ruleset_file,
# MAGIC   _rescued_data,
# MAGIC
# MAGIC   current_timestamp() AS ingest_timestamp,
# MAGIC   input_file_name()   AS ingest_source,
# MAGIC   regexp_extract(input_file_name(), '[^/]+$', 0) AS file_name
# MAGIC FROM read_files(
# MAGIC   '/Volumes/naf_catalog/bronze/naf_raw/naf_tournament.csv',
# MAGIC   format                    => 'csv',
# MAGIC   header                    => true,
# MAGIC   sep                       => ';',
# MAGIC   quote                     => '"',
# MAGIC   escape                    => '"',
# MAGIC   multiLine                 => true,
# MAGIC   ignoreLeadingWhiteSpace   => true,
# MAGIC   ignoreTrailingWhiteSpace  => true,
# MAGIC   rescuedDataColumn         => '_rescued_data',
# MAGIC   enforceSchema             => false,
# MAGIC   schema => '
# MAGIC     tournamentid STRING,
# MAGIC     tournamentorganizerid STRING,
# MAGIC     tournamentname STRING,
# MAGIC     tournamentaddress1 STRING,
# MAGIC     tournamentaddress2 STRING,
# MAGIC     tournamentcity STRING,
# MAGIC     tournamentstate STRING,
# MAGIC     tournamentzip STRING,
# MAGIC     tournamentnation STRING,
# MAGIC     tournamenturl STRING,
# MAGIC     tournamentnotesurl STRING,
# MAGIC     tournamentstartdate STRING,
# MAGIC     tournamentenddate STRING,
# MAGIC     tournamenttype STRING,
# MAGIC     tournamentstyle STRING,
# MAGIC     tournamentscoring STRING,
# MAGIC     tournamentcost STRING,
# MAGIC     tournamentnaffee STRING,
# MAGIC     tournamentnafdiscount STRING,
# MAGIC     tournamentinformation STRING,
# MAGIC     tournamentcontact STRING,
# MAGIC     tournamentemail STRING,
# MAGIC     tournamentorg STRING,
# MAGIC     tournamentstatus STRING,
# MAGIC     tournamentmajor STRING,
# MAGIC     geolongitude STRING,
# MAGIC     geolattitude STRING,
# MAGIC     tournamentreport STRING,
# MAGIC     subscription_closed STRING,
# MAGIC     naf_rulesetid STRING,
# MAGIC     naf_variantsid STRING,
# MAGIC     variant_notes STRING,
# MAGIC     variantstatus STRING,
# MAGIC     tournament_ruleset_file STRING
# MAGIC   '
# MAGIC );
# MAGIC

# COMMAND ----------

# DBTITLE 1,bronze.tournament_statistics_group_raw
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE naf_catalog.bronze.tournament_statistics_group_raw
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC   typeID,
# MAGIC   tournamentID,
# MAGIC   coachID,
# MAGIC   raceID,
# MAGIC   notes,
# MAGIC   date,
# MAGIC   _rescued_data,
# MAGIC
# MAGIC   current_timestamp() AS ingest_timestamp,
# MAGIC   input_file_name()   AS ingest_source,
# MAGIC   regexp_extract(input_file_name(), '[^/]+$', 0) AS file_name
# MAGIC FROM read_files(
# MAGIC   '/Volumes/naf_catalog/bronze/naf_raw/naf_tournament_statistics_group.csv',
# MAGIC   format                    => 'csv',
# MAGIC   header                    => true,
# MAGIC   sep                       => ';',
# MAGIC   quote                     => '"',
# MAGIC   escape                    => '"',
# MAGIC   multiLine                 => true,
# MAGIC   ignoreLeadingWhiteSpace   => true,
# MAGIC   ignoreTrailingWhiteSpace  => true,
# MAGIC   rescuedDataColumn         => '_rescued_data',
# MAGIC   enforceSchema             => false,
# MAGIC   schema => '
# MAGIC     typeID STRING,
# MAGIC     tournamentID STRING,
# MAGIC     coachID STRING,
# MAGIC     raceID STRING,
# MAGIC     notes STRING,
# MAGIC     date STRING
# MAGIC   '
# MAGIC );
# MAGIC

# COMMAND ----------

# DBTITLE 1,bronze.tournament_statistics_list_raw
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE naf_catalog.bronze.tournament_statistics_list_raw
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC   id,
# MAGIC   name,
# MAGIC   label,
# MAGIC   `order` AS order_,        -- technical sanitisation only (reserved word)
# MAGIC   _rescued_data,
# MAGIC
# MAGIC   current_timestamp() AS ingest_timestamp,
# MAGIC   input_file_name()   AS ingest_source,
# MAGIC   regexp_extract(input_file_name(), '[^/]+$', 0) AS file_name
# MAGIC FROM read_files(
# MAGIC   '/Volumes/naf_catalog/bronze/naf_raw/naf_tournament_statistics_list.csv',
# MAGIC   format                    => 'csv',
# MAGIC   header                    => true,
# MAGIC   sep                       => ';',
# MAGIC   quote                     => '"',
# MAGIC   escape                    => '"',
# MAGIC   multiLine                 => true,
# MAGIC   ignoreLeadingWhiteSpace   => true,
# MAGIC   ignoreTrailingWhiteSpace  => true,
# MAGIC   rescuedDataColumn         => '_rescued_data',
# MAGIC   enforceSchema             => false,
# MAGIC   schema => '
# MAGIC     id STRING,
# MAGIC     name STRING,
# MAGIC     label STRING,
# MAGIC     `order` STRING
# MAGIC   '
# MAGIC );
# MAGIC

# COMMAND ----------

# DBTITLE 1,bronze.tournament_coach_raw
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE naf_catalog.bronze.tournament_coach_raw
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC   naftournament,
# MAGIC   nafcoach,
# MAGIC   race,
# MAGIC   _rescued_data,
# MAGIC
# MAGIC   current_timestamp() AS ingest_timestamp,
# MAGIC   input_file_name()   AS ingest_source,
# MAGIC   regexp_extract(input_file_name(), '[^/]+$', 0) AS file_name
# MAGIC FROM read_files(
# MAGIC   '/Volumes/naf_catalog/bronze/naf_raw/naf_tournamentcoach.csv',
# MAGIC   format                    => 'csv',
# MAGIC   header                    => true,
# MAGIC   sep                       => ';',
# MAGIC   quote                     => '"',
# MAGIC   escape                    => '"',
# MAGIC   multiLine                 => true,
# MAGIC   ignoreLeadingWhiteSpace   => true,
# MAGIC   ignoreTrailingWhiteSpace  => true,
# MAGIC   rescuedDataColumn         => '_rescued_data',
# MAGIC   enforceSchema             => false,
# MAGIC   schema => '
# MAGIC     naftournament STRING,
# MAGIC     nafcoach STRING,
# MAGIC     race STRING
# MAGIC   '
# MAGIC );
# MAGIC

# COMMAND ----------

# DBTITLE 1,bronze.variants_raw
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE naf_catalog.bronze.variants_raw
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC   variantid,
# MAGIC   variantname,
# MAGIC   `order` AS order_,      -- technical sanitisation only (reserved word)
# MAGIC   _rescued_data,
# MAGIC
# MAGIC   current_timestamp() AS ingest_timestamp,
# MAGIC   input_file_name()   AS ingest_source,
# MAGIC   regexp_extract(input_file_name(), '[^/]+$', 0) AS file_name
# MAGIC FROM read_files(
# MAGIC   '/Volumes/naf_catalog/bronze/naf_raw/naf_variants.csv',
# MAGIC   format                    => 'csv',
# MAGIC   header                    => true,
# MAGIC   sep                       => ';',
# MAGIC   quote                     => '"',
# MAGIC   escape                    => '"',
# MAGIC   multiLine                 => true,
# MAGIC   ignoreLeadingWhiteSpace   => true,
# MAGIC   ignoreTrailingWhiteSpace  => true,
# MAGIC   rescuedDataColumn         => '_rescued_data',
# MAGIC   enforceSchema             => false,
# MAGIC   schema => '
# MAGIC     variantid STRING,
# MAGIC     variantname STRING,
# MAGIC     `order` STRING
# MAGIC   '
# MAGIC );
# MAGIC

# COMMAND ----------

# DBTITLE 1,FIFA country codes from HTML
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
import re, os

path = "/Volumes/naf_catalog/bronze/helper_files/FIFA Country Codes.html"

# Read file as binary (no UTF-8 assumptions)
binary = spark.read.format("binaryFile").load(path)
# https://dbc-24ea936c-b383.cloud.databricks.com/editor/notebooks/1284271501539849?o=3710693717888594$0

# Decode bytes explicitly
text = binary.select(
    F.decode(F.col("content"), "windows-1252").alias("text")
).collect()[0]["text"]

match = re.search(r"<pre>(.*)</pre>", text, re.S)
if not match:
    raise ValueError("Could not find <pre>...</pre> block in HTML file")

pre_block = match.group(1)

rows = []
for line in pre_block.splitlines():
    parts = [p for p in line.strip().split('\t') if p]
    if len(parts) == 3 and parts[0] != "Country/Independent State":
        country, fifa, ioc = [p.strip() for p in parts]
        rows.append((country, fifa, ioc))

schema = StructType([
    StructField("country_name_raw", StringType(), False),
    StructField("fifa_code",        StringType(), False),
    StructField("ioc_code",         StringType(), False),
])

df = spark.createDataFrame(rows, schema)

df_bronze = (
    df
    .withColumn("ingest_timestamp", F.current_timestamp())
    .withColumn("ingest_source",   F.lit(path))
    .withColumn("file_name",       F.lit(os.path.basename(path)))
    .withColumn("source_url", F.lit("https://www.rsssf.org/miscellaneous/fifa-codes.html"))
    .withColumn(
        "copyright_notice",
        F.lit(
            "Data source: 'FIFA Country Codes', compiled by Iain Jeffree for RSSSF. "
            "© Iain Jeffree & RSSSF 2000–2025 – used with acknowledgement."
        )
    )
)

(df_bronze.write
  .mode("overwrite")
  .saveAsTable("naf_catalog.bronze.fifa_country_codes_raw"))


# COMMAND ----------

# DBTITLE 1,ISO country codes from CSV
import re, os
from pyspark.sql import functions as F

iso_path = "/Volumes/naf_catalog/bronze/helper_files/wikipedia-iso-country-codes.csv"

df_raw = (
    spark.read
    .option("header", "true")
    .option("encoding", "UTF-8")
    .csv(iso_path)
)

def sanitize_delta_col(col_name: str) -> str:
    # Replace invalid chars with underscore; keep letters/digits/underscore
    s = re.sub(r"[ ,;{}()\n\t=]", "_", col_name)   # chars Delta complains about
    s = s.replace("-", "_")                        # hyphen also problematic
    s = re.sub(r"__+", "_", s).strip("_")
    return s

df_safe = df_raw
for c in df_raw.columns:
    df_safe = df_safe.withColumnRenamed(c, sanitize_delta_col(c))

df_bronze = (
    df_safe
    .withColumn("ingest_timestamp", F.current_timestamp())
    .withColumn("ingest_source",   F.lit(iso_path))
    .withColumn("file_name",       F.lit(os.path.basename(iso_path)))
    .withColumn("source_url", F.lit("https://en.wikipedia.org/wiki/List_of_ISO_3166_country_codes"))
)

# If you previously had the old curated-schema table, drop it first:
spark.sql("DROP TABLE IF EXISTS naf_catalog.bronze.iso_country_codes_raw")

(df_bronze.write
  .mode("overwrite")
  .saveAsTable("naf_catalog.bronze.iso_country_codes_raw")
)
                                                                                                                            