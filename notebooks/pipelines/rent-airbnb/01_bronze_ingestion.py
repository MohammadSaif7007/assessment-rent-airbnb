# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: Raw Ingestion
# MAGIC
# MAGIC Goal here is simple — land the raw source files into Delta tables with
# MAGIC zero transformation. We just add a few metadata columns for lineage.
# MAGIC
# MAGIC If anything in the cleaning logic changes later, we can always replay
# MAGIC from these tables without touching the source files again.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# Using widgets so this notebook can be parameterized when run as a job
dbutils.widgets.text("catalog", "revodata")
dbutils.widgets.text("schema", "amsterdam_investment")
dbutils.widgets.text("data_path", "/Volumes/revodata/amsterdam_investment/raw_data")

catalog   = dbutils.widgets.get("catalog")
schema    = dbutils.widgets.get("schema")
data_path = dbutils.widgets.get("data_path")

# Make sure the catalog and schema exist before we try to write
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Airbnb listings
# MAGIC
# MAGIC Reading everything as strings on purpose — we don't want Spark
# MAGIC guessing types here. Type casting happens in the silver layer
# MAGIC where we can be deliberate about it.

# COMMAND ----------

airbnb_bronze = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(f"{data_path}/airbnb.csv")
    .withColumn("_source", F.lit("airbnb"))
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit("airbnb.csv"))
)

print(f"Loaded {airbnb_bronze.count():,} Airbnb rows")
display(airbnb_bronze.limit(5))

# COMMAND ----------

(
    airbnb_bronze.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.bronze_airbnb")
)

print(f"✓ Saved to {catalog}.{schema}.bronze_airbnb")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Kamernet rentals
# MAGIC
# MAGIC The JSON was scraped from Kamernet and has a quirk — fields like
# MAGIC `_id`, `crawledAt`, and the `*SeenAt` timestamps come through as
# MAGIC single-element arrays instead of scalars. That's just how the scraper
# MAGIC stored them. We flatten those here since it's purely structural,
# MAGIC not a data transformation decision.

# COMMAND ----------

rentals_bronze = (
    spark.read
    .option("multiLine", "true")
    .json(f"{data_path}/rentals.json")
)

# Unwrap the single-element arrays
array_cols = ["_id", "crawledAt", "firstSeenAt", "lastSeenAt", "detailsCrawledAt"]
for col in array_cols:
    if col in rentals_bronze.columns:
        rentals_bronze = rentals_bronze.withColumn(col, F.element_at(F.col(col), 1))

rentals_bronze = (
    rentals_bronze
    .withColumn("_source", F.lit("kamernet"))
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit("rentals.json"))
)

print(f"Loaded {rentals_bronze.count():,} Kamernet rows")
display(rentals_bronze.limit(5))

# COMMAND ----------

(
    rentals_bronze.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.bronze_rentals")
)

print(f"Saved to {catalog}.{schema}.bronze_rentals")