# assessment-rent-airbnb

A Databricks-native medallion pipeline comparing long-term rental revenue (Kamernet) against
short-term Airbnb revenue per Amsterdam postal code — helping identify the most profitable
investment strategy per neighborhood.

Built using the [RevoData Asset Bundle Template](https://github.com/revodatanl/revo-asset-bundle-templates).

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          SOURCE DATA (Volumes)                              │
│                                                                             │
│   airbnb.csv (9,913 rows)          rentals.json (46,722 rows)               │
│   • zipcode (mixed formats)        • city (17 cities)                       │
│   • lat/lng                        • rent (messy strings)                   │
│   • nightly price                  • postalCode (6-char)                    │
│   • room_type, bedrooms            • propertyType, areaSqm                  │
└──────────────────────┬──────────────────────────┬───────────────────────────┘
                       │                          │
                       ▼                          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        BRONZE LAYER (Delta Tables)                          │
│                                                                             │
│   bronze_airbnb                        bronze_rentals                       │
│   • All columns as strings             • Array fields flattened             │
│   • No type coercion                     (_id, crawledAt, firstSeenAt)      │
│   • Lineage metadata added             • Lineage metadata added             │
│     (_source, _ingested_at,              (_source, _ingested_at,            │
│      _source_file)                        _source_file)                     │
│                                                                             │
│   Replay point: re-process from here if cleaning logic changes downstream   │
└──────────────────────┬──────────────────────────┬───────────────────────────┘
                       │                          │
                       ▼                          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        SILVER LAYER (Delta Tables)                          │
│                                                                             │
│   silver_airbnb                        silver_rentals                       │
│                                                                             │
│   Cleaning steps:                      Cleaning steps:                      │
│   ✓ Cast to proper types               ✓ Filter to Amsterdam only           │
│   ✓ Normalize zipcode → PC4            ✓ Parse rent strings → float         │
│   ✓ Flag price outliers                ✓ Extract utilities_included flag    │
│   ✓ Spatial backfill for missing       ✓ Normalize postalCode → PC4         │
│     postal codes (22.7% recovered)     ✓ Parse areaSqm, matchCapacity       │
│     via point-in-polygon join          ✓ Flag rent outliers                 │
│     against post_codes.geojson         ✓ Parse timestamps                   │
│                                                                             │
│   UDFs sourced from:                                                        │
│   src/assessment_rent_airbnb/cleaners.py (pure Python, zero Spark dep)      │
└──────────────────────┬──────────────────────────┬───────────────────────────┘
                       │                          │
                       ▼                          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         GOLD LAYER (Delta Tables)                           │
│                                                                             │
│   gold_postal_code_revenue                                                  │
│                                                                             │
│   Airbnb revenue model:               Kamernet revenue model:               │
│   occupied_nights = 365 × 70%         gross = monthly_rent × 11 months      │
│   gross = occupied_nights × price     maintenance = gross × 10%             │
│   fees = gross × 3%                   net = gross − maintenance             │
│   cleaning = turnovers × €30                                                │
│   net = gross − fees − cleaning       All assumptions parameterized         │
│                                       via Databricks widgets                │
│                                                                             │
│   Output: per-PC4 comparison with revenue_advantage + recommended_strategy  │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Repository Structure

```
assessment-rent-airbnb/
│
├── databricks.yml                               # Asset Bundle config (dev/prod targets)
│
├── resources/
│   └── rent-airbnb-job.yml                      # Job DAG: task dependencies + schedule
│
├── notebooks/
│   └── pipelines/
│       └── rent-airbnb/
│           ├── 01_bronze_ingestion.py           # Ingest CSV + JSON → Delta tables
│           ├── 02_silver_cleaning.py            # Clean, normalize, spatial backfill
│           └── 03_gold_revenue.py               # Revenue calc + postal code comparison
│
├── scratch/
│   └── 01_exploratory_analysis.py              # EDA documenting data quality findings
│
├── src/
│   └── assessment_rent_airbnb/
│       └── cleaners.py                          # Pure Python UDFs (no Spark dependency)
│
├── tests/
│   └── default_test.py                          # Unit tests — run locally without Spark
│
├── pyproject.toml                               # Dependencies + tool config
└── .pre-commit-config.yaml                      # Code quality hooks
```

---

## Data Quality Issues & Resolutions

| Issue | Dataset | Count | Resolution |
|---|---|---|---|
| Missing postal codes | Airbnb | 2,254 (22.7%) | Spatial backfill via `post_codes.geojson` — 100% recovery |
| Inconsistent postal code formats | Airbnb | All rows | Normalize to PC4 (first 4 digits) via UDF |
| Price outliers (up to €9,000/night) | Airbnb | ~20 rows | Flag in silver, exclude from gold aggregations |
| Missing bedrooms / review scores | Airbnb | 14 / 1,711 | Retain as null — not required for revenue |
| Non-Amsterdam records | Kamernet | 38,627 (83%) | Filter on `city == 'Amsterdam'` in silver |
| Messy rent strings (`"€ 1.250,- Utilities incl."`) | Kamernet | All rows | Regex parser in `cleaners.py` |
| Utilities-included ambiguity | Kamernet | ~40% of rows | Boolean `utilities_included` flag column |
| Single-element array fields | Kamernet | `_id`, timestamps | Flattened via `element_at()` in bronze |

---

## Prerequisites

- Databricks workspace with Unity Catalog enabled ([free trial](https://www.databricks.com/try-databricks))
- Databricks CLI installed: `pip install databricks-cli`
- Source data files from the assessment repo:
  - `airbnb.csv`
  - `rentals.json`
  - `post_codes.geojson`

---

## Setup & Deployment

### Step 1 — Configure the Databricks CLI

```bash
pip install databricks-cli
databricks configure --token
# Enter your workspace URL and personal access token
```

Verify the connection:

```bash
databricks clusters list
```

### Step 2 — Create Unity Catalog resources

Run the following in the Databricks SQL Editor:

```sql
CREATE CATALOG IF NOT EXISTS revodata;
CREATE SCHEMA IF NOT EXISTS revodata.amsterdam_investment;
CREATE VOLUME IF NOT EXISTS revodata.amsterdam_investment.raw_data;
CREATE VOLUME IF NOT EXISTS revodata.amsterdam_investment.output;
```

Then create the geo subfolder:

```bash
databricks fs mkdir dbfs:/Volumes/revodata/amsterdam_investment/raw_data/geo
```

### Step 3 — Upload source data to Volumes

```bash
databricks fs cp airbnb.csv dbfs:/Volumes/revodata/amsterdam_investment/raw_data/airbnb.csv

databricks fs cp rentals.json dbfs:/Volumes/revodata/amsterdam_investment/raw_data/rentals.json

databricks fs cp post_codes.geojson dbfs:/Volumes/revodata/amsterdam_investment/raw_data/geo/post_codes.geojson
```

### Step 4 — Upload the src package to Databricks workspace

```bash
databricks workspace mkdirs /Workspace/assessment-rent-airbnb

databricks workspace import-dir src /Workspace/assessment-rent-airbnb/src --overwrite
```

### Step 5 — Upload notebooks to Databricks workspace

```bash
databricks workspace import-dir notebooks/pipelines/rent-airbnb /Workspace/assessment-rent-airbnb/notebooks/pipelines/rent-airbnb --overwrite
```

### Step 6 — Run the pipeline

Open each notebook in the Databricks UI, connect to **Serverless**, and run in order:

1. `01_bronze_ingestion` — ingests raw CSV and JSON into Delta tables
2. `02_silver_cleaning` — cleans, normalizes, and backfills postal codes
3. `03_gold_revenue` — calculates revenue and produces the comparison table

---

## Run Tests Locally

Tests run without Spark — the cleaning functions are pure Python.

```bash
pip install -e ".[dev]"
pytest tests/default_test.py -v
```

Expected output: **25 passed**

---

## Design Decisions

### Medallion Architecture
Bronze preserves raw data as a replay point — if cleaning logic changes, we reprocess from
bronze without re-acquiring source data. Silver is where opinions live: postal code
normalization, outlier thresholds, Amsterdam filtering. These are explicit and separate from
the business logic in gold. Changing a cleaning rule in silver never touches gold code.

### Pure Python UDFs
Cleaning functions in `cleaners.py` have zero Spark dependency. This means:
- Unit tests run locally in milliseconds without a cluster
- The same code works as Spark UDFs, in a REPL, or in a Pandas pipeline
- Every edge case (European thousands separators, unicode euro signs, mixed postal formats) is testable in isolation

### Spatial Backfill
22.7% of Airbnb listings have no postal code — but all have lat/lng coordinates.
Dropping them would bias revenue estimates toward neighborhoods where Airbnb's
data happens to be cleaner. The solution: load `post_codes.geojson` PC4 polygon
boundaries into driver memory (22MB), broadcast to executors, and run a
point-in-polygon lookup as a `pandas_udf`. Result: **100% of 2,260 missing
postal codes recovered**.

### Parameterized Revenue Assumptions
Occupancy rates, service fees, cleaning costs, and maintenance reserves are
Databricks widgets — not hardcoded values. A stakeholder can re-run the gold
layer with different assumptions without touching any code.

---

## Key Results

Airbnb short-term rental outperforms Kamernet long-term rental in every Amsterdam
postal code where both data sources are available.

| PC4 | Neighborhood | Airbnb net/year | Kamernet net/year | Advantage |
|-----|-------------|-----------------|-------------------|-----------|
| 1071 | De Pijp | €32,142 | €8,019 | **€24,123** |
| 1016 | Grachtengordel | €34,372 | €10,841 | **€23,531** |
| 1181 | Amstelveen | €41,807 | €20,790 | **€21,017** |
| 1012 | Centrum | €31,894 | €11,138 | **€20,757** |
| 1015 | Jordaan | €32,142 | €12,375 | **€19,767** |

> **Caveat:** These are gross revenue estimates. Airbnb requires active property
> management, compliance with Amsterdam's short-stay permit regulations, and carries
> higher vacancy risk between bookings. Operational costs should be modeled before
> making investment decisions.

---

## Revenue Model Assumptions

| Parameter | Value | Rationale |
|---|---|---|
| Airbnb occupancy rate | 70% | Amsterdam average per AirDNA data |
| Airbnb host service fee | 3% | Standard Airbnb host fee |
| Cleaning cost per turnover | €30 | Conservative Amsterdam estimate |
| Average stay length | 3 nights | Amsterdam short-stay average |
| Kamernet months occupied | 11/year | 1 month vacancy for tenant turnover |
| Maintenance reserve | 10% | Property management + repairs |

All parameters are adjustable via Databricks widgets in `03_gold_revenue`.