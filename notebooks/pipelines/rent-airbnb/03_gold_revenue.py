# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer: Revenue Calculations & Aggregation
# MAGIC
# MAGIC Answers the core question: which Amsterdam postal codes are most
# MAGIC profitable — Airbnb (short-term) or Kamernet (long-term)?
# MAGIC
# MAGIC All revenue assumptions are parameterized via widgets.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("catalog", "revodata", "Unity Catalog name")
dbutils.widgets.text("schema", "amsterdam_investment", "Schema name")
dbutils.widgets.text("airbnb_occupancy_rate", "0.70", "Airbnb occupancy rate (0-1)")
dbutils.widgets.text("airbnb_service_fee_pct", "0.03", "Airbnb host service fee")
dbutils.widgets.text("airbnb_cleaning_cost", "30.0", "Cleaning cost per turnover (€)")
dbutils.widgets.text("airbnb_avg_stay_nights", "3.0", "Average booking length (nights)")
dbutils.widgets.text("kamernet_months_occupied", "11", "Kamernet months occupied per year")
dbutils.widgets.text("kamernet_maintenance_pct", "0.10", "Maintenance reserve")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

OCCUPANCY_RATE = float(dbutils.widgets.get("airbnb_occupancy_rate"))
SERVICE_FEE_PCT = float(dbutils.widgets.get("airbnb_service_fee_pct"))
CLEANING_COST = float(dbutils.widgets.get("airbnb_cleaning_cost"))
AVG_STAY = float(dbutils.widgets.get("airbnb_avg_stay_nights"))
MONTHS_OCCUPIED = int(dbutils.widgets.get("kamernet_months_occupied"))
MAINTENANCE_PCT = float(dbutils.widgets.get("kamernet_maintenance_pct"))

print("Revenue assumptions:")
print(f"  Airbnb occupancy rate:  {OCCUPANCY_RATE:.0%}")
print(f"  Airbnb service fee:     {SERVICE_FEE_PCT:.0%}")
print(f"  Cleaning per turnover:  €{CLEANING_COST}")
print(f"  Avg stay length:        {AVG_STAY} nights")
print(f"  Kamernet months/year:   {MONTHS_OCCUPIED}")
print(f"  Maintenance reserve:    {MAINTENANCE_PCT:.0%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Airbnb Revenue
# MAGIC ```
# MAGIC occupied_nights = 365 × occupancy_rate
# MAGIC gross_revenue   = occupied_nights × nightly_price
# MAGIC service_fees    = gross_revenue × service_fee_pct
# MAGIC cleaning_costs  = (occupied_nights / avg_stay) × cleaning_cost
# MAGIC net_revenue     = gross_revenue − service_fees − cleaning_costs
# MAGIC ```

# COMMAND ----------

occupied_nights = 365 * OCCUPANCY_RATE
num_turnovers = occupied_nights / AVG_STAY

airbnb_revenue = (
    spark.table(f"{catalog}.{schema}.silver_airbnb")
    .filter(F.col("is_price_outlier") == False)
    .withColumn("annual_gross_revenue", F.lit(occupied_nights) * F.col("price"))
    .withColumn("annual_service_fees", F.col("annual_gross_revenue") * F.lit(SERVICE_FEE_PCT))
    .withColumn("annual_cleaning_costs", F.lit(num_turnovers * CLEANING_COST))
    .withColumn(
        "annual_net_revenue",
        F.col("annual_gross_revenue")
        - F.col("annual_service_fees")
        - F.col("annual_cleaning_costs")
    )
)

print(f"Airbnb listings (excl. outliers): {airbnb_revenue.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Kamernet Revenue
# MAGIC ```
# MAGIC gross_revenue = monthly_rent × months_occupied
# MAGIC maintenance   = gross_revenue × maintenance_pct
# MAGIC net_revenue   = gross_revenue − maintenance
# MAGIC ```

# COMMAND ----------

kamernet_revenue = (
    spark.table(f"{catalog}.{schema}.silver_rentals")
    .filter(F.col("is_rent_outlier") == False)
    .withColumn("annual_gross_revenue", F.col("rent_amount") * F.lit(MONTHS_OCCUPIED))
    .withColumn("annual_maintenance_costs", F.col("annual_gross_revenue") * F.lit(MAINTENANCE_PCT))
    .withColumn(
        "annual_net_revenue",
        F.col("annual_gross_revenue") - F.col("annual_maintenance_costs")
    )
)

print(f"Kamernet listings (excl. outliers): {kamernet_revenue.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate by Postal Code

# COMMAND ----------

airbnb_agg = (
    airbnb_revenue.groupBy("pc4").agg(
        F.count("price").alias("airbnb_listing_count"),
        F.round(F.avg("price"), 2).alias("airbnb_avg_nightly_price"),
        F.round(F.percentile_approx("price", 0.5), 2).alias("airbnb_median_nightly_price"),
        F.round(F.avg("annual_net_revenue"), 2).alias("airbnb_avg_annual_net_revenue"),
        F.round(F.percentile_approx("annual_net_revenue", 0.5), 2).alias("airbnb_median_annual_net_revenue"),
        F.round(F.avg("review_scores_value"), 2).alias("airbnb_avg_review_score"),
    )
)

kamernet_agg = (
    kamernet_revenue.groupBy("pc4").agg(
        F.count("rent_amount").alias("kamernet_listing_count"),
        F.round(F.avg("rent_amount"), 2).alias("kamernet_avg_monthly_rent"),
        F.round(F.percentile_approx("rent_amount", 0.5), 2).alias("kamernet_median_monthly_rent"),
        F.round(F.avg("annual_net_revenue"), 2).alias("kamernet_avg_annual_net_revenue"),
        F.round(F.percentile_approx("annual_net_revenue", 0.5), 2).alias("kamernet_median_annual_net_revenue"),
        F.round(F.avg("area_sqm"), 2).alias("kamernet_avg_area_sqm"),
    )
)

gold_postal = (
    airbnb_agg.join(kamernet_agg, on="pc4", how="full_outer")
    .withColumn(
        "revenue_advantage",
        F.round(
            F.col("airbnb_median_annual_net_revenue")
            - F.col("kamernet_median_annual_net_revenue"),
            2
        )
    )
    .withColumn(
        "recommended_strategy",
        F.when(F.col("airbnb_median_annual_net_revenue").isNull(), "Kamernet (no Airbnb data)")
         .when(F.col("kamernet_median_annual_net_revenue").isNull(), "Airbnb (no Kamernet data)")
         .when(F.col("revenue_advantage") > 0, "Airbnb")
         .otherwise("Kamernet")
    )
    .orderBy(F.col("revenue_advantage").desc_nulls_last())
)

print(f"Unique postal codes: {gold_postal.count()}")
display(gold_postal)

# COMMAND ----------

(
    gold_postal.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.gold_postal_code_revenue")
)
print(f"Written to {catalog}.{schema}.gold_postal_code_revenue")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results

# COMMAND ----------

print("=== Strategy Distribution ===")
display(gold_postal.groupBy("recommended_strategy").count().orderBy("count", ascending=False))

# COMMAND ----------

print("=== Top 10 Postal Codes by Airbnb Revenue Advantage ===")
display(
    gold_postal
    .filter(F.col("revenue_advantage").isNotNull())
    .select(
        "pc4",
        "airbnb_listing_count",
        "kamernet_listing_count",
        "airbnb_median_annual_net_revenue",
        "kamernet_median_annual_net_revenue",
        "revenue_advantage",
        "recommended_strategy"
    )
    .limit(10)
)