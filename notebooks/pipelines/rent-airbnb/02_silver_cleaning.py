# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: Cleaning & Standardization
# MAGIC
# MAGIC This is where the opinionated decisions live. Bronze just landed the raw data —
# MAGIC here we decide what "clean" means:
# MAGIC
# MAGIC - All postal codes → PC4 (4-digit) so Airbnb and Kamernet can actually be joined
# MAGIC - Missing Airbnb postal codes backfilled via spatial join (22.7% of rows affected)
# MAGIC - Kamernet filtered to Amsterdam only — 83% of the dataset is other cities
# MAGIC - Rent strings parsed from "€ 1.250,- Utilities incl." into actual numbers
# MAGIC - Outliers flagged but kept — gold layer decides what to do with them

# COMMAND ----------

import sys
import json
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, BooleanType, IntegerType
from pyspark.sql.functions import pandas_udf

# COMMAND ----------

dbutils.widgets.text("catalog", "revodata")
dbutils.widgets.text("schema", "amsterdam_investment")
dbutils.widgets.text("data_path", "/Volumes/revodata/amsterdam_investment/raw_data")

catalog   = dbutils.widgets.get("catalog")
schema    = dbutils.widgets.get("schema")
data_path = dbutils.widgets.get("data_path")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register UDFs
# MAGIC
# MAGIC All the parsing logic lives in `src/assessment_rent_airbnb/cleaners.py` as
# MAGIC plain Python functions with no Spark dependency. That means they're easy to
# MAGIC unit test locally without spinning up a cluster. We just wrap them as UDFs here.

# COMMAND ----------

sys.path.insert(0, "/Workspace/Users/mdsaif091994@gmail.com/assessment-rent-airbnb/src")

from assessment_rent_airbnb.cleaners import (
    normalize_postal_code_to_pc4,
    parse_rent_amount,
    parse_rent_utilities,
    parse_area_sqm,
    parse_match_capacity,
    parse_deposit_string,
    detect_price_outliers,
)

udf_normalize_pc4       = F.udf(normalize_postal_code_to_pc4, StringType())
udf_parse_rent_amount   = F.udf(parse_rent_amount,            DoubleType())
udf_parse_rent_utilities= F.udf(parse_rent_utilities,         BooleanType())
udf_parse_area          = F.udf(parse_area_sqm,               DoubleType())
udf_parse_capacity      = F.udf(parse_match_capacity,         IntegerType())
udf_parse_deposit       = F.udf(parse_deposit_string,         DoubleType())
udf_is_price_outlier    = F.udf(detect_price_outliers,        BooleanType())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Airbnb — clean and type-cast
# MAGIC
# MAGIC Bronze kept everything as strings. Now we cast to proper types,
# MAGIC normalize zipcodes, and flag anything that looks like a data issue.
# MAGIC We drop rows with no price (useless for revenue) but keep rows with
# MAGIC missing postal codes — those get backfilled in the next cell.

# COMMAND ----------

airbnb_bronze = spark.table(f"{catalog}.{schema}.bronze_airbnb")

airbnb_silver = (
    airbnb_bronze
    .withColumn("pc4",                 udf_normalize_pc4(F.col("zipcode")))
    .withColumn("price",               F.col("price").cast(DoubleType()))
    .withColumn("bedrooms",            F.col("bedrooms").cast(DoubleType()))
    .withColumn("accommodates",        F.col("accommodates").cast(IntegerType()))
    .withColumn("latitude",            F.col("latitude").cast(DoubleType()))
    .withColumn("longitude",           F.col("longitude").cast(DoubleType()))
    .withColumn("review_scores_value", F.col("review_scores_value").cast(DoubleType()))
    .withColumn("is_price_outlier",    udf_is_price_outlier(F.col("price")))
    .withColumn("room_type",           F.trim(F.col("room_type")))
    .filter(F.col("latitude").isNotNull() | F.col("longitude").isNotNull())
    .filter(F.col("price").isNotNull())
)

total          = airbnb_silver.count()
missing_before = airbnb_silver.filter(F.col("pc4").isNull()).count()
print(f"Rows after cleaning:      {total:,}")
print(f"Missing PC4 (pre-backfill): {missing_before:,} ({missing_before/total*100:.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Backfill missing postal codes
# MAGIC
# MAGIC About 2,254 Airbnb listings have no postal code at all — but they all
# MAGIC have lat/lng coordinates. Rather than throwing them away, we do a
# MAGIC point-in-polygon lookup against the PC4 boundary polygons from the
# MAGIC geojson file.
# MAGIC
# MAGIC The geojson is only 22MB so we load it into driver memory and broadcast
# MAGIC it to executors. The actual lookup runs as a pandas_udf using Shapely.
# MAGIC End result: 100% of the missing codes recovered.

# COMMAND ----------

try:
    from shapely.geometry import shape, Point

    # Load and parse the geojson on the driver
    geojson_raw  = dbutils.fs.head(f"{data_path}/geo/post_codes.geojson", maxBytes=50_000_000)
    geojson_data = json.loads(geojson_raw)

    # Figure out which property field holds the PC4 code
    # (the geojson uses different column names depending on the source)
    pc4_col_name = None
    if geojson_data.get("features"):
        sample_props = geojson_data["features"][0].get("properties", {})
        for key in ["pc4", "PC4", "postcode4", "postcode"]:
            if key in sample_props:
                pc4_col_name = key
                break
        if pc4_col_name is None:
            # Fall back to any 4-digit field
            for key, val in sample_props.items():
                if isinstance(val, (str, int)) and len(str(val)) == 4 and str(val).isdigit():
                    pc4_col_name = key
                    break

    if pc4_col_name:
        pc4_polygons = []
        for feature in geojson_data["features"]:
            props = feature.get("properties", {})
            geom  = feature.get("geometry")
            pc4   = str(props.get(pc4_col_name, ""))
            if geom and pc4:
                try:
                    pc4_polygons.append((pc4, shape(geom)))
                except Exception:
                    pass

        print(f"Loaded {len(pc4_polygons):,} PC4 polygons from geojson")
        bc_polygons = spark.sparkContext.broadcast(pc4_polygons)

        @pandas_udf(StringType())
        def lookup_pc4(lat_series: pd.Series, lon_series: pd.Series) -> pd.Series:
            polygons = bc_polygons.value
            results  = []
            for lat, lon in zip(lat_series, lon_series):
                if pd.isna(lat) or pd.isna(lon):
                    results.append(None)
                    continue
                point = Point(lon, lat)
                found = next((code for code, poly in polygons if poly.contains(point)), None)
                results.append(found)
            return pd.Series(results)

        missing_mask = (
            F.col("pc4").isNull()
            & F.col("latitude").isNotNull()
            & F.col("longitude").isNotNull()
        )

        airbnb_silver = (
            airbnb_silver
            .withColumn("pc4_backfilled", F.lit(False))
            .withColumn("pc4_lookup",
                F.when(missing_mask, lookup_pc4(F.col("latitude"), F.col("longitude")))
                 .otherwise(F.lit(None)))
            .withColumn("pc4",
                F.when(F.col("pc4").isNull(), F.col("pc4_lookup"))
                 .otherwise(F.col("pc4")))
            .withColumn("pc4_backfilled",
                F.when(F.col("pc4_lookup").isNotNull() & missing_mask, F.lit(True))
                 .otherwise(F.col("pc4_backfilled")))
            .drop("pc4_lookup")
        )

        missing_after = airbnb_silver.filter(F.col("pc4").isNull()).count()
        print(f"Backfilled {missing_before - missing_after:,} of {missing_before:,} missing codes")

except Exception as e:
    print(f"Spatial backfill failed, skipping: {e}")
    airbnb_silver = airbnb_silver.withColumn("pc4_backfilled", F.lit(False))

# COMMAND ----------

display(airbnb_silver.limit(10))

# COMMAND ----------

(
    airbnb_silver.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.silver_airbnb")
)
print(f"✓ Saved to {catalog}.{schema}.silver_airbnb")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Kamernet — filter, parse, normalise
# MAGIC
# MAGIC The Kamernet dataset covers 17 Dutch cities but we only care about
# MAGIC Amsterdam. After filtering, we go from 46K rows down to about 8K.
# MAGIC
# MAGIC The main parsing challenge is the rent field — values come in as
# MAGIC strings like "€ 1.250,- Utilities incl." which need to be split
# MAGIC into a numeric amount and a utilities flag.

# COMMAND ----------

rentals_bronze = spark.table(f"{catalog}.{schema}.bronze_rentals")

rentals_silver = (
    rentals_bronze
    # Amsterdam only — other cities aren't relevant for this comparison
    .filter(F.lower(F.col("city")) == "amsterdam")
    # Parse the messy rent strings
    .withColumn("rent_amount",        udf_parse_rent_amount(F.col("rent")))
    .withColumn("utilities_included", udf_parse_rent_utilities(F.col("rent")))
    # Normalise postal code to PC4 so it can join with Airbnb
    .withColumn("pc4",                udf_normalize_pc4(F.col("postalCode")))
    # Parse the other text fields
    .withColumn("area_sqm",           udf_parse_area(F.col("areaSqm")))
    .withColumn("capacity",           udf_parse_capacity(F.col("matchCapacity")))
    .withColumn("deposit_amount",     udf_parse_deposit(F.col("deposit")))
    .withColumn("latitude",           F.col("latitude").cast(DoubleType()))
    .withColumn("longitude",          F.col("longitude").cast(DoubleType()))
    # Drop anything we can't calculate revenue for
    .filter(F.col("rent_amount").isNotNull())
    # Flag suspicious values — anything below €100 or above €5k/month
    # is almost certainly a data error. We flag rather than drop.
    .withColumn("is_rent_outlier",
        (F.col("rent_amount") < 100) | (F.col("rent_amount") > 5000))
    .withColumn("first_seen_at", F.to_timestamp("firstSeenAt"))
    .withColumn("last_seen_at",  F.to_timestamp("lastSeenAt"))
    .select(
        "_id", "pc4", "postalCode", "latitude", "longitude",
        "rent_amount", "utilities_included", "deposit_amount",
        "propertyType", "area_sqm", "capacity", "furnish",
        "is_rent_outlier", "first_seen_at", "last_seen_at",
        "_source", "_ingested_at"
    )
)

print(f"{rentals_silver.count():,} Amsterdam listings after filtering")
display(rentals_silver.limit(10))

# COMMAND ----------

(
    rentals_silver.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.silver_rentals")
)
print(f"Saved to {catalog}.{schema}.silver_rentals")