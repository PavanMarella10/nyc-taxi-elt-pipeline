# NYC Taxi ELT Pipeline
A batch data pipeline that ingests NYC yellow taxi trip records, lands them in a
partitioned data lake, loads them into Postgres, and models them into a star
schema with dbt.

Built locally with tooling that maps directly onto its cloud equivalents
(S3 → Snowflake → dbt Cloud), so the architecture transfers without rewriting
the design.

---
## Results

| Metric | Value |
|---|---|
| Source rows ingested | 5,972,150 |
| Rows in the fact table after cleaning | 5,754,540 |
| Rejected by data quality rules | ~218,000 (3.6%) |
| Months of data | Jan–Feb 2024 |
| Load throughput | ~200k rows/sec via `COPY` |

**Data quality issues caught by the staging layer:**

| Issue | Rows |
|---|---|
| Negative fare amounts | 71,584 |
| Zero-distance trips | 128,596 |
| Missing passenger counts | 325,772 |
| Dropoff before pickup | 1,673 |
| Trips dated outside the loaded window | 19 |

Those 19 stray rows are dated 2002, 2008, 2009, 2023 and March 2024 — inside
files labelled January and February 2024. This is why partitions are derived
from the pickup timestamp rather than the file name.

---

## Architecture

```
NYC TLC (Parquet)
      │  extract.py — streaming download, validated, idempotent
      ▼
data/raw/                          immutable landing zone
      │  organize.py — partition on the pickup timestamp
      ▼
data/lake/year=YYYY/month=M/       Hive-partitioned lake
      │  load.py — bulk COPY, incremental, audited
      ▼
raw.yellow_trips                   staging layer, unchanged from source
      │  dbt
      ▼
analytics_staging.stg_yellow_trips renamed, typed, deduped, cleaned
      │
      ▼
analytics_marts.fct_trips          ← star schema
analytics_marts.dim_date
analytics_marts.dim_location
analytics_marts.dim_payment_type
```

### Star schema

`fct_trips` holds one row per completed trip: foreign keys to the dimensions
plus additive measures (fare, tip, distance, duration). Descriptive attributes
live in the dimensions.

The fact table stores `pickup_location_id = 161` once per trip. `dim_location`
stores `"Midtown Center, Manhattan"` once, for all 283,021 of those trips. That
is the economics of dimensional modeling.

---

## Stack

| Layer | Tool | Cloud equivalent |
|---|---|---|
| Extraction | Python, `requests` | Airbyte, Fivetran |
| Lake storage | Parquet, PyArrow | S3 + Glue Catalog |
| Warehouse | PostgreSQL (Docker) | Snowflake, BigQuery, Redshift |
| Transformation | dbt | dbt Cloud |
| Testing | dbt tests | dbt tests, Great Expectations |

---

## Design decisions

**ELT, not ETL.** Raw data lands in the warehouse unchanged and is transformed
in place. When cleaning logic turns out to be wrong — and it does — the original
is still there and everything downstream can be rebuilt. Transform-before-load
destroys that option permanently.

**Raw is immutable.** `data/raw/` matches the source byte for byte. Nothing
modifies it. This means the pipeline can always prove what actually arrived.

**Partitions derived from data, not filenames.** File names are metadata and
metadata lies — 19 rows here prove it. `organize.py` reads the pickup timestamp
and partitions on that.

**Partition on the filter column.** Queries filter by date, so partitions are
`year=/month=`. Partitioning on something high-cardinality like trip ID would
produce thousands of tiny files — the small files problem — and be slower than
no partitioning at all.

**Bulk `COPY`, not `INSERT`.** Row-by-row inserts mean one round trip per row.
`COPY` streams a batch in a single operation, roughly 100x faster.

**Idempotent loads.** Every partition is recorded in an audit table and skipped
on re-run; reloads delete before inserting. Pipelines fail halfway and get
re-run constantly, so a load that duplicates data on retry is a broken load.

**Commit per partition.** If partition 3 fails, partitions 1 and 2 stay
committed and the re-run resumes rather than starting over.

**Unknown members in every dimension.** Each dimension carries a `-1` row and
facts coalesce unmatched keys to it. Without this, an inner join silently drops
rows and reports under-report with no error anywhere.

**Cleaning happens in transform, not on ingest.** Filtering rules live in
version-controlled SQL that can be reviewed, tested, and changed. Cleaning
inside a load script is invisible and unauditable.

---

## Running it

Requires Python 3.10+ and Docker.

```bash
python -m venv .venv
.venv\Scripts\activate            # macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env

docker compose up -d              # Postgres

python extract.py                 # download source Parquet
python organize.py                # partition into the lake
python load.py                    # bulk load into Postgres
python explore_sql.py             # profile the raw layer

python get_zones.py               # taxi zone reference data
cd dbt
dbt seed  --profiles-dir .
dbt build --profiles-dir .        # run + test in dependency order
```

Add more months by editing `MONTHS` in `config.py` and re-running. Everything
already loaded is skipped.

---

## Sample query

```sql
SELECT l.zone_name, l.borough, COUNT(*) AS trips
FROM analytics_marts.fct_trips f
JOIN analytics_marts.dim_location l
  ON f.pickup_location_id = l.location_id
GROUP BY 1, 2
ORDER BY trips DESC
LIMIT 5;
```

```
 zone_name             | borough   | trips
-----------------------+-----------+--------
 Midtown Center        | Manhattan | 283021
 Upper East Side South | Manhattan | 277637
 Upper East Side North | Manhattan | 264431
 JFK Airport           | Queens    | 256775
 Midtown East          | Manhattan | 207309
```

---

## Testing

Data quality is enforced with dbt tests:

- **Primary keys** — `unique` + `not_null` on every model's key
- **Referential integrity** — `relationships` tests on every foreign key, so a
  fact can never point at a dimension row that does not exist
- **Business rules** — singular tests for physically impossible trips (speeds
  over 100 mph, negative amounts, dropoffs before pickups)
- **Reconciliation** — row counts compared between staging and fact to catch
  silent losses from a broken join or a stalled incremental model
- **Freshness** — `dbt source freshness` catches the failure mode where every
  test passes but the pipeline has been dead for days

  
## What I would change at scale

Postgres is the constraint here, not the design. At 100x this volume:

- **Warehouse** — move to Snowflake or BigQuery for separated storage and
  compute. The dbt models port with minimal change; that is the point of doing
  transformation in dbt rather than in Python.
- **Lake** — S3 with a Glue catalog instead of local disk, and Iceberg or Delta
  for schema evolution and time travel.
- **Compute** — Spark for transformations too large for a single warehouse
  node.
- **Partitioning** — daily rather than monthly, once monthly partitions grow
  past comfortable scan sizes.
- **Orchestration** — Airflow with per-partition tasks so backfills parallelise
  and failures are isolated to one partition.
