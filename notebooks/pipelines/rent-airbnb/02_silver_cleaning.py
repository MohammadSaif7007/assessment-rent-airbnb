# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Cleaning & Standardization
# MAGIC
# MAGIC - Normalizes postal codes to PC4 (4-digit) for join-ability
# MAGIC - Parses Kamernet rent strings into numeric values + utilities flag
# MAGIC - Filters Kamernet to Amsterdam only (83% of records are other cities)
# MAGIC - Flags outliers — removal is a gold-layer decision
# MAGIC - Backfills missing Airbnb postal codes via spatial join

# COMMAND ----------

import sys
import json
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, BooleanType, IntegerType
from pyspark.sql.functions import pandas_udf

# COMMAND ----------

dbutils.widgets.text("catalog", "revodata", "Unity Catalog name")
dbutils.widgets.text("schema", "amsterdam_investment", "Schema name")
dbutils.widgets.text("data_path", "/Volumes/revodata/amsterdam_investment/raw_data", "Path to source data")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
data_path = dbutils.widgets.get("data_path")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register UDFs
# MAGIC
# MAGIC Cleaning functions in src/assessment_rent_airbnb/cleaners.py are pure Python
# MAGIC — no Spark dependency — so they are unit-testable locally without a cluster.

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

udf_normalize_pc4 = F.udf(normalize_postal_code_to_pc4, StringType())
udf_parse_rent_amount = F.udf(parse_rent_amount, DoubleType())
udf_parse_rent_utilities = F.udf(parse_rent_utilities, BooleanType())
udf_parse_area = F.udf(parse_area_sqm, DoubleType())
udf_parse_capacity = F.udf(parse_match_capacity, IntegerType())
udf_parse_deposit = F.udf(parse_deposit_string, DoubleType())
udf_is_price_outlier = F.udf(detect_price_outliers, BooleanType())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean Airbnb Data

# COMMAND ----------

airbnb_bronze = spark.table(f"{catalog}.{schema}.bronze_airbnb")

airbnb_silver = (
    airbnb_bronze
    .withColumn("pc4", udf_normalize_pc4(F.col("zipcode")))
    .withColumn("price", F.col("price").cast(DoubleType()))
    .withColumn("bedrooms", F.col("bedrooms").cast(DoubleType()))
    .withColumn("accommodates", F.col("accommodates").cast(IntegerType()))
    .withColumn("latitude", F.col("latitude").cast(DoubleType()))
    .withColumn("longitude", F.col("longitude").cast(DoubleType()))
    .withColumn("review_scores_value", F.col("review_scores_value").cast(DoubleType()))
    .withColumn("is_price_outlier", udf_is_price_outlier(F.col("price")))
    .withColumn("room_type", F.trim(F.col("room_type")))
    .filter(F.col("latitude").isNotNull() | F.col("longitude").isNotNull())
    .filter(F.col("price").isNotNull())
)

missing_before = airbnb_silver.filter(F.col("pc4").isNull()).count()
total = airbnb_silver.count()
print(f"Airbnb silver: {total} rows")
print(f"Missing PC4 before backfill: {missing_before} ({missing_before/total*100:.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Backfill Missing Postal Codes via Spatial Join
# MAGIC
# MAGIC 23% of Airbnb listings have no postal code but have lat/lng.
# MAGIC Point-in-polygon lookup against PC4 boundary polygons recovers 100%
# MAGIC of missing codes. Uses broadcast + pandas_udf — geojson fits in driver memory.

# COMMAND ----------

try:
    from shapely.geometry import shape, Point

    geojson_raw = dbutils.fs.head(
        f"{data_path}/geo/post_codes.geojson", maxBytes=50_000_000
    )
    geojson_data = json.loads(geojson_raw)

    # Detect PC4 column name in geojson properties
    pc4_col_name = None
    if geojson_data.get("features"):
        sample_props = geojson_data["features"][0].get("properties", {})
        for key in ["pc4", "PC4", "postcode4", "postcode"]:
            if key in sample_props:
                pc4_col_name = key
                break
        if pc4_col_name is None:
            for key, val in sample_props.items():
                if isinstance(val, (str, int)) and len(str(val)) == 4 and str(val).isdigit():
                    pc4_col_name = key
                    break

    if pc4_col_name:
        pc4_polygons = []
        for feature in geojson_data["features"]:
            props = feature.get("properties", {})
            geom = feature.get("geometry")
            pc4_val = str(props.get(pc4_col_name, ""))
            if geom and pc4_val:
                try:
                    pc4_polygons.append((pc4_val, shape(geom)))
                except Exception:
                    pass

        print(f"Loaded {len(pc4_polygons)} PC4 polygons")
        bc_polygons = spark.sparkContext.broadcast(pc4_polygons)

        @pandas_udf(StringType())
        def lookup_pc4(lat_series: pd.Series, lon_series: pd.Series) -> pd.Series:
            polygons = bc_polygons.value
            results = []
            for lat, lon in zip(lat_series, lon_series):
                if pd.isna(lat) or pd.isna(lon):
                    results.append(None)
                    continue
                point = Point(lon, lat)
                found = next(
                    (code for code, polygon in polygons if polygon.contains(point)),
                    None
                )
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
            .withColumn(
                "pc4_lookup",
                F.when(missing_mask, lookup_pc4(F.col("latitude"), F.col("longitude")))
                 .otherwise(F.lit(None))
            )
            .withColumn(
                "pc4",
                F.when(F.col("pc4").isNull(), F.col("pc4_lookup"))
                 .otherwise(F.col("pc4"))
            )
            .withColumn(
                "pc4_backfilled",
                F.when(F.col("pc4_lookup").isNotNull() & missing_mask, F.lit(True))
                 .otherwise(F.col("pc4_backfilled"))
            )
            .drop("pc4_lookup")
        )

        missing_after = airbnb_silver.filter(F.col("pc4").isNull()).count()
        print(f"Backfilled {missing_before - missing_after}/{missing_before} missing postal codes")

except Exception as e:
    print(f"WARNING: Spatial backfill failed: {e}")
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
print(f"Written to {catalog}.{schema}.silver_airbnb")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean Kamernet Rentals

# COMMAND ----------

rentals_bronze = spark.table(f"{catalog}.{schema}.bronze_rentals")

rentals_silver = (
    rentals_bronze
    .filter(F.lower(F.col("city")) == "amsterdam")
    .withColumn("rent_amount", udf_parse_rent_amount(F.col("rent")))
    .withColumn("utilities_included", udf_parse_rent_utilities(F.col("rent")))
    .withColumn("pc4", udf_normalize_pc4(F.col("postalCode")))
    .withColumn("area_sqm", udf_parse_area(F.col("areaSqm")))
    .withColumn("capacity", udf_parse_capacity(F.col("matchCapacity")))
    .withColumn("deposit_amount", udf_parse_deposit(F.col("deposit")))
    .withColumn("latitude", F.col("latitude").cast(DoubleType()))
    .withColumn("longitude", F.col("longitude").cast(DoubleType()))
    .filter(F.col("rent_amount").isNotNull())
    .withColumn(
        "is_rent_outlier",
        (F.col("rent_amount") < 100) | (F.col("rent_amount") > 5000)
    )
    .withColumn("first_seen_at", F.to_timestamp("firstSeenAt"))
    .withColumn("last_seen_at", F.to_timestamp("lastSeenAt"))
    .select(
        "_id", "pc4", "postalCode", "latitude", "longitude",
        "rent_amount", "utilities_included", "deposit_amount",
        "propertyType", "area_sqm", "capacity", "furnish",
        "is_rent_outlier", "first_seen_at", "last_seen_at",
        "_source", "_ingested_at"
    )
)

print(f"Rentals silver: {rentals_silver.count()} rows (Amsterdam only)")
display(rentals_silver.limit(10))

# COMMAND ----------

(
    rentals_silver.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.silver_rentals")
)
print(f"Written to {catalog}.{schema}.silver_rentals")