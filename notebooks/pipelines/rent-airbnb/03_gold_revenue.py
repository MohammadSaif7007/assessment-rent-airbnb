# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: Revenue Calculations & Comparison
# MAGIC
# MAGIC This is the layer that actually answers the question — for each Amsterdam
# MAGIC postal code, is Airbnb or Kamernet the better investment?
# MAGIC
# MAGIC The revenue models for both sources are intentionally simple and transparent.
# MAGIC All the assumptions (occupancy rate, cleaning costs, maintenance reserve, etc.)
# MAGIC are exposed as widgets so anyone can re-run this with different numbers
# MAGIC without touching the code.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup & assumptions
# MAGIC
# MAGIC Tweak any of these widgets to re-run the revenue model with different inputs.
# MAGIC The defaults are based on Amsterdam market averages.

# COMMAND ----------

dbutils.widgets.text("catalog", "revodata")
dbutils.widgets.text("schema", "amsterdam_investment")

# Airbnb assumptions
dbutils.widgets.text("airbnb_occupancy_rate",  "0.70")   # 70% is realistic for Amsterdam
dbutils.widgets.text("airbnb_service_fee_pct", "0.03")   # Standard Airbnb host fee
dbutils.widgets.text("airbnb_cleaning_cost",   "30.0")   # Per turnover, conservative estimate
dbutils.widgets.text("airbnb_avg_stay_nights", "3.0")    # Average booking length

# Kamernet assumptions
dbutils.widgets.text("kamernet_months_occupied", "11")   # Allow 1 month vacancy per year
dbutils.widgets.text("kamernet_maintenance_pct", "0.10") # 10% for maintenance and management

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

OCCUPANCY_RATE  = float(dbutils.widgets.get("airbnb_occupancy_rate"))
SERVICE_FEE_PCT = float(dbutils.widgets.get("airbnb_service_fee_pct"))
CLEANING_COST   = float(dbutils.widgets.get("airbnb_cleaning_cost"))
AVG_STAY        = float(dbutils.widgets.get("airbnb_avg_stay_nights"))
MONTHS_OCCUPIED = int(dbutils.widgets.get("kamernet_months_occupied"))
MAINTENANCE_PCT = float(dbutils.widgets.get("kamernet_maintenance_pct"))

print("Running with these assumptions:")
print(f"  Airbnb occupancy:      {OCCUPANCY_RATE:.0%}")
print(f"  Airbnb service fee:    {SERVICE_FEE_PCT:.0%}")
print(f"  Cleaning per turnover: €{CLEANING_COST:.0f}")
print(f"  Avg stay length:       {AVG_STAY:.0f} nights")
print(f"  Kamernet months/year:  {MONTHS_OCCUPIED}")
print(f"  Maintenance reserve:   {MAINTENANCE_PCT:.0%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Airbnb revenue
# MAGIC
# MAGIC We exclude flagged outliers (anything priced above €2,000/night or below €10)
# MAGIC before calculating — a handful of listings at €9,000/night would completely
# MAGIC skew the postal code averages.
# MAGIC
# MAGIC ```
# MAGIC occupied_nights  = 365 × occupancy_rate
# MAGIC gross_revenue    = occupied_nights × nightly_price
# MAGIC service_fees     = gross_revenue × service_fee_pct
# MAGIC cleaning_costs   = (occupied_nights / avg_stay) × cleaning_cost_per_turnover
# MAGIC net_revenue      = gross_revenue − service_fees − cleaning_costs
# MAGIC ```

# COMMAND ----------

occupied_nights = 365 * OCCUPANCY_RATE
num_turnovers   = occupied_nights / AVG_STAY

airbnb_revenue = (
    spark.table(f"{catalog}.{schema}.silver_airbnb")
    .filter(F.col("is_price_outlier") == False)
    .withColumn("annual_gross_revenue",   F.lit(occupied_nights) * F.col("price"))
    .withColumn("annual_service_fees",    F.col("annual_gross_revenue") * F.lit(SERVICE_FEE_PCT))
    .withColumn("annual_cleaning_costs",  F.lit(num_turnovers * CLEANING_COST))
    .withColumn("annual_net_revenue",
        F.col("annual_gross_revenue")
        - F.col("annual_service_fees")
        - F.col("annual_cleaning_costs"))
)

print(f"Airbnb listings after removing outliers: {airbnb_revenue.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Kamernet revenue
# MAGIC
# MAGIC Simpler model — monthly rent times months occupied, minus a maintenance
# MAGIC reserve. We also exclude outlier rents here (below €100 or above €5k/month).
# MAGIC
# MAGIC ```
# MAGIC gross_revenue = monthly_rent × months_occupied
# MAGIC maintenance   = gross_revenue × maintenance_pct
# MAGIC net_revenue   = gross_revenue − maintenance
# MAGIC ```

# COMMAND ----------

kamernet_revenue = (
    spark.table(f"{catalog}.{schema}.silver_rentals")
    .filter(F.col("is_rent_outlier") == False)
    .withColumn("annual_gross_revenue",      F.col("rent_amount") * F.lit(MONTHS_OCCUPIED))
    .withColumn("annual_maintenance_costs",  F.col("annual_gross_revenue") * F.lit(MAINTENANCE_PCT))
    .withColumn("annual_net_revenue",
        F.col("annual_gross_revenue") - F.col("annual_maintenance_costs"))
)

print(f"Kamernet listings after removing outliers: {kamernet_revenue.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Aggregate by postal code
# MAGIC
# MAGIC Full outer join so we keep postal codes that only appear in one source.
# MAGIC We use median rather than mean for the revenue comparison — a few
# MAGIC high-end listings can pull the mean up significantly in some areas.

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
    .withColumn("revenue_advantage",
        F.round(
            F.col("airbnb_median_annual_net_revenue")
            - F.col("kamernet_median_annual_net_revenue"),
            2
        )
    )
    # Positive = Airbnb wins, negative = Kamernet wins
    # Handle postal codes that only appear in one source
    .withColumn("recommended_strategy",
        F.when(F.col("airbnb_median_annual_net_revenue").isNull(),  "Kamernet (no Airbnb data)")
         .when(F.col("kamernet_median_annual_net_revenue").isNull(), "Airbnb (no Kamernet data)")
         .when(F.col("revenue_advantage") > 0, "Airbnb")
         .otherwise("Kamernet")
    )
    .orderBy(F.col("revenue_advantage").desc_nulls_last())
)

print(f"Unique postal codes in gold table: {gold_postal.count():,}")
display(gold_postal)

# COMMAND ----------

(
    gold_postal.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.gold_postal_code_revenue")
)
print(f"✓ Saved to {catalog}.{schema}.gold_postal_code_revenue")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results

# COMMAND ----------

# How many postal codes favour each strategy?
display(
    gold_postal
    .groupBy("recommended_strategy")
    .count()
    .orderBy("count", ascending=False)
)

# COMMAND ----------

# Top 10 postal codes where Airbnb has the biggest revenue advantage
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