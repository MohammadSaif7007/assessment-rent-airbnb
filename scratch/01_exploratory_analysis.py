# Databricks notebook source
# MAGIC %md
# MAGIC # Exploratory Data Analysis
# MAGIC
# MAGIC Documents data quality issues discovered during initial exploration.
# MAGIC Validates cleaning assumptions before building the pipeline.

# COMMAND ----------

from pyspark.sql import functions as F

data_path = "/Volumes/revodata/amsterdam_investment/raw_data"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Airbnb Dataset

# COMMAND ----------

airbnb = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{data_path}/airbnb.csv")
)
print(f"Shape: {airbnb.count()} rows x {len(airbnb.columns)} cols")
airbnb.printSchema()

# COMMAND ----------

total = airbnb.count()
missing_zip = airbnb.filter(F.col("zipcode").isNull() | (F.col("zipcode") == "")).count()
print(f"Missing zipcode:       {missing_zip}/{total} ({missing_zip/total*100:.1f}%)")
print(f"Missing bedrooms:      {airbnb.filter(F.col('bedrooms').isNull()).count()}")
print(f"Missing review_scores: {airbnb.filter(F.col('review_scores_value').isNull()).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Finding: 22.7% missing postal codes
# MAGIC All have lat/lng — recovered via spatial join in silver layer. 100% recovery.

# COMMAND ----------

display(airbnb.select("price").summary())
print(f"Prices > €2000/night: {airbnb.filter(F.col('price') > 2000).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Finding: Outliers up to €9,000/night
# MAGIC Flagged in silver, excluded from gold aggregations.

# COMMAND ----------

zips = airbnb.filter(F.col("zipcode").isNotNull())
print(f"4-digit only (e.g. '1053'):      {zips.filter(F.col('zipcode').rlike('^[0-9]{4}$')).count()}")
print(f"6-char no space (e.g. '1013HE'): {zips.filter(F.col('zipcode').rlike('^[0-9]{4}[A-Za-z]{2}$')).count()}")
print(f"With space (e.g. '1016 AM'):     {zips.filter(F.col('zipcode').rlike('^[0-9]{4}\\s[A-Za-z]{2}$')).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Finding: Three different postal code formats
# MAGIC All normalized to PC4 (first 4 digits) in silver layer via UDF.

# COMMAND ----------

display(airbnb.groupBy("room_type").count().orderBy("count", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Kamernet Rentals Dataset

# COMMAND ----------

rentals = spark.read.option("multiLine", "true").json(f"{data_path}/rentals.json")
print(f"Shape: {rentals.count()} rows x {len(rentals.columns)} cols")

# COMMAND ----------

display(rentals.groupBy("city").count().orderBy("count", ascending=False).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Finding: Only 8,095 of 46,722 records are Amsterdam
# MAGIC Filtered to Amsterdam only in silver layer.

# COMMAND ----------

ams = rentals.filter(F.lower(F.col("city")) == "amsterdam")
display(ams.select("rent").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Finding: Rent strings like "€ 950,- Utilities incl." need parsing
# MAGIC Handled by `parse_rent_string()` in `src/assessment_rent_airbnb/cleaners.py`.

# COMMAND ----------

display(ams.groupBy("propertyType").count().orderBy("count", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Quality Summary
# MAGIC
# MAGIC | Issue | Dataset | Count | Resolution |
# MAGIC |---|---|---|---|
# MAGIC | Missing postal codes | Airbnb | 2,254 (22.7%) | Spatial backfill via geojson |
# MAGIC | Inconsistent postal code formats | Airbnb | All rows | Normalize to PC4 |
# MAGIC | Price outliers (up to €9,000/night) | Airbnb | ~20 | Flag in silver, exclude in gold |
# MAGIC | Non-Amsterdam records | Kamernet | 38,627 (83%) | Filter on city='Amsterdam' |
# MAGIC | Messy rent strings | Kamernet | All rows | Regex parser in cleaners.py |
# MAGIC | Utilities included ambiguity | Kamernet | ~40% | Boolean flag column |
# MAGIC | Single-element list fields | Kamernet | _id, timestamps | Flatten via element_at() in bronze |