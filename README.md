# assessment-rent-airbnb

A Databricks-native medallion pipeline comparing long-term rental revenue
(Kamernet) against short-term Airbnb revenue per Amsterdam postal code —
helping identify the most profitable investment strategy per neighborhood.

Built using the [RevoData Asset Bundle Template](https://github.com/revodatanl/revo-asset-bundle-templates).

## Architecture
```
Bronze (raw Delta)  →  Silver (cleaned Delta)  →  Gold (revenue comparison)
```
airbnb.csv              PC4 normalized             by postal code
rentals.json            Amsterdam filtered          recommended strategy
Outliers flagged
Missing postcodes backfilled (100% recovery)

All layers are managed Delta tables in Unity Catalog under
`revodata.amsterdam_investment`.

## Repository Structure
```
├── databricks.yml                                   # Asset Bundle config
├── resources/
│   └── rent-airbnb-job.yml                          # Job DAG definition
├── notebooks/
│   └── pipelines/
│       └── rent-airbnb/
│           ├── 01_bronze_ingestion.py               # Raw CSV + JSON → Delta
│           ├── 02_silver_cleaning.py                # Cleaning + spatial backfill
│           └── 03_gold_revenue.py                   # Revenue calc + comparison
├── scratch/
│   └── 01_exploratory_analysis.py                   # EDA + data quality findings
├── src/assessment_rent_airbnb/
│   └── cleaners.py                                  # Pure Python UDFs
└── tests/
└── default_test.py                              # Unit tests (no Spark needed)
```

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- Databricks CLI: `pip install databricks-cli`
- Source data from the assessment repo:
  `airbnb.csv`, `rentals.json`, `post_codes.geojson`

## Setup

### 1. Configure the CLI

```bash
databricks configure --token
```

### 2. Create Unity Catalog resources

Run in Databricks SQL Editor:

```sql
CREATE CATALOG IF NOT EXISTS revodata;
CREATE SCHEMA IF NOT EXISTS revodata.amsterdam_investment;
CREATE VOLUME IF NOT EXISTS revodata.amsterdam_investment.raw_data;
CREATE VOLUME IF NOT EXISTS revodata.amsterdam_investment.output;
```

### 3. Upload source data

```bash
databricks fs cp airbnb.csv dbfs:/Volumes/revodata/amsterdam_investment/raw_data/airbnb.csv
databricks fs cp rentals.json dbfs:/Volumes/revodata/amsterdam_investment/raw_data/rentals.json
databricks fs mkdir dbfs:/Volumes/revodata/amsterdam_investment/raw_data/geo
databricks fs cp post_codes.geojson dbfs:/Volumes/revodata/amsterdam_investment/raw_data/geo/post_codes.geojson
```

### 4. Upload the src package

```bash
databricks workspace import-dir src /Workspace/Users/<your-email>/assessment-rent-airbnb/src
```

### 5. Run notebooks

Open each notebook in Databricks, connect to Serverless, and run in order:

1. `notebooks/pipelines/rent-airbnb/01_bronze_ingestion.py`
2. `notebooks/pipelines/rent-airbnb/02_silver_cleaning.py`
3. `notebooks/pipelines/rent-airbnb/03_gold_revenue.py`

## Run Tests Locally

```bash
pip install -e ".[dev]"
pytest tests/ -v
```

## Design Decisions

**Medallion layers:** Bronze preserves raw data as a replay point. Silver
is where cleaning opinions live — explicit and separate from business logic.
Gold answers the business question. Changing a cleaning rule never touches
gold code.

**Pure Python UDFs:** Cleaning functions in `cleaners.py` have zero Spark
dependency. Unit tests run locally in milliseconds without a cluster. The
same functions work as Spark UDFs, in notebooks, or in a REPL.

**Spatial backfill:** 22.7% of Airbnb listings have no postal code but have
lat/lng. Dropping them biases revenue estimates. A point-in-polygon join
against `post_codes.geojson` recovers 100% of the 2,260 missing codes.

**Parameterized assumptions:** Occupancy rates, service fees, and maintenance
reserves are Databricks widgets — adjustable per run without touching code.

## Key Results

Airbnb outperforms Kamernet in every Amsterdam postal code where both
sources have data.

```
| PC4 | Airbnb net/year | Kamernet net/year | Advantage |
|-----|-----------------|-------------------|-----------|
| 1016 | €34,372 | €10,841 | €23,531 |
| 1071 | €32,142 | €8,019 | €24,123 |
| 1181 | €41,807 | €20,790 | €21,017 |
```