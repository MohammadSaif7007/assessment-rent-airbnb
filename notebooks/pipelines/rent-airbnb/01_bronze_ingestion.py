# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: Raw Data Ingestion
# MAGIC
# MAGIC Ingests Airbnb CSV and Kamernet JSON into Delta tables.
# MAGIC Adds lineage metadata only — no transformation. This gives us
# MAGIC a reliable replay point if cleaning logic changes downstream.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("catalog", "revodata", "Unity Catalog name")
dbutils.widgets.text("schema", "amsterdam_investment", "Schema name")
dbutils.widgets.text("data_path", "/Volumes/revodata/amsterdam_investment/raw_data", "Path to source data")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
data_path = dbutils.widgets.get("data_path")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Airbnb CSV

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

print(f"Airbnb bronze: {airbnb_bronze.count()} rows")
display(airbnb_bronze.limit(5))

# COMMAND ----------

(
    airbnb_bronze.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.bronze_airbnb")
)
print(f"Written to {catalog}.{schema}.bronze_airbnb")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Kamernet Rentals JSON

# COMMAND ----------

rentals_bronze = (
    spark.read
    .option("multiLine", "true")
    .json(f"{data_path}/rentals.json")
)

# Flatten single-element array fields — artefact of how the JSON was scraped
for col_name in ["_id", "crawledAt", "firstSeenAt", "lastSeenAt", "detailsCrawledAt"]:
    if col_name in rentals_bronze.columns:
        rentals_bronze = rentals_bronze.withColumn(
            col_name, F.element_at(F.col(col_name), 1)
        )

rentals_bronze = (
    rentals_bronze
    .withColumn("_source", F.lit("kamernet"))
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit("rentals.json"))
)

print(f"Rentals bronze: {rentals_bronze.count()} rows")
display(rentals_bronze.limit(5))

# COMMAND ----------

(
    rentals_bronze.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.bronze_rentals")
)
print(f"Written to {catalog}.{schema}.bronze_rentals")