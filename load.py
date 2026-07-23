"""
Step 3 — Load.

Move data from the lake into Postgres, one partition at a time.

Concepts this teaches:
  - Staging / raw layer: land data in the warehouse UNCHANGED first.
  - ELT vs ETL: we Load first, Transform later (step 4). That is ELT.
  - Bulk loading: COPY, not INSERT. Roughly 100x faster.
  - Batching: stream in chunks so memory stays flat regardless of data size.
  - Incremental load: only load partitions we have not loaded before.
  - Idempotency: delete-then-insert per partition, so a re-run is safe.
  - Audit table: record what was loaded and when — this is data lineage.

Run it with:
    python load.py                 # incremental: skip partitions already loaded
    python load.py --full-refresh  # reload everything from scratch
"""

import io
import sys
import time

import psycopg2
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.dataset as ds

import config

LAKE_DIR = config.PROJECT_ROOT / "data" / "lake"

SCHEMA = "raw"
TABLE = "yellow_trips"
FQN = f"{SCHEMA}.{TABLE}"

# Rows pulled from the lake and pushed to Postgres at a time. Memory stays
# roughly constant no matter how much total data you have.
BATCH_SIZE = 250_000

# The target table definition. We declare types explicitly instead of letting
# a tool guess — "schema on write". A guessed schema silently changes when
# next month's data looks slightly different.
COLUMNS = [
    ("vendorid", "INTEGER"),
    ("tpep_pickup_datetime", "TIMESTAMP"),
    ("tpep_dropoff_datetime", "TIMESTAMP"),
    ("passenger_count", "DOUBLE PRECISION"),
    ("trip_distance", "DOUBLE PRECISION"),
    ("ratecodeid", "DOUBLE PRECISION"),
    ("store_and_fwd_flag", "TEXT"),
    ("pulocationid", "INTEGER"),
    ("dolocationid", "INTEGER"),
    ("payment_type", "BIGINT"),
    ("fare_amount", "DOUBLE PRECISION"),
    ("extra", "DOUBLE PRECISION"),
    ("mta_tax", "DOUBLE PRECISION"),
    ("tip_amount", "DOUBLE PRECISION"),
    ("tolls_amount", "DOUBLE PRECISION"),
    ("improvement_surcharge", "DOUBLE PRECISION"),
    ("total_amount", "DOUBLE PRECISION"),
    ("congestion_surcharge", "DOUBLE PRECISION"),
    ("airport_fee", "DOUBLE PRECISION"),
    ("year", "INTEGER"),
    ("month", "INTEGER"),
]

COLUMN_NAMES = [name for name, _ in COLUMNS]


def connect():
    """Open a Postgres connection using the settings in your .env file."""
    return psycopg2.connect(
        host=config.DB["host"],
        port=config.DB["port"],
        user=config.DB["user"],
        password=config.DB["password"],
        dbname=config.DB["dbname"],
    )


def create_objects(conn) -> None:
    """Create the schema, the staging table, and the audit table if missing."""
    cols_sql = ",\n            ".join(f"{name} {dtype}" for name, dtype in COLUMNS)
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {FQN} (
            {cols_sql},
            _loaded_at TIMESTAMP DEFAULT now()
            )
            """
        )
        # The audit table answers "what is in the warehouse and when did it
        # arrive?" Every production pipeline has some version of this.
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.load_audit (
                table_name  TEXT    NOT NULL,
                year        INTEGER NOT NULL,
                month       INTEGER NOT NULL,
                row_count   BIGINT  NOT NULL,
                loaded_at   TIMESTAMP DEFAULT now(),
                PRIMARY KEY (table_name, year, month)
            )
            """
        )
    conn.commit()


def already_loaded(conn) -> set:
    """Return the set of (year, month) partitions recorded in the audit table."""
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT year, month FROM {SCHEMA}.load_audit WHERE table_name = %s",
            (TABLE,),
        )
        return {(row[0], row[1]) for row in cur.fetchall()}


def find_partitions() -> list:
    """Discover year=/month= folders in the lake."""
    partitions = []
    for year_dir in sorted(LAKE_DIR.glob("year=*")):
        for month_dir in sorted(year_dir.glob("month=*")):
            year = int(year_dir.name.split("=")[1])
            month = int(month_dir.name.split("=")[1])
            partitions.append((year, month))
    return sorted(partitions)


def align_columns(batch: pa.RecordBatch, rename: dict) -> pa.Table:
    """
    Rename source columns to lowercase and put them in the target order.

    Source files are inconsistent: 'VendorID' one month, 'Airport_fee' another.
    Normalizing here means the rest of the pipeline never has to care.
    """
    table = pa.Table.from_batches([batch])
    table = table.rename_columns([rename[name] for name in table.schema.names])

    arrays, fields = [], []
    for name in COLUMN_NAMES:
        if name in table.schema.names:
            col = table.column(name)
        else:
            # Column missing from this month's file — fill with nulls rather
            # than crashing. Schemas drift over time; handle it explicitly.
            col = pa.nulls(table.num_rows, type=pa.float64())
        arrays.append(col)
        fields.append(pa.field(name, col.type))
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def load_partition(conn, dataset, rename: dict, year: int, month: int) -> int:
    """Delete and reload one partition. Returns rows loaded."""
    import pyarrow.compute as pc

    # Delete first, then insert. This makes the whole operation idempotent:
    # run it once or ten times, the result is identical. Without the delete,
    # a re-run after a partial failure would duplicate rows.
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {FQN} WHERE year = %s AND month = %s", (year, month))

    scanner = dataset.scanner(
        filter=(pc.field("year") == year) & (pc.field("month") == month),
        batch_size=BATCH_SIZE,
    )

    col_list = ", ".join(COLUMN_NAMES)
    copy_sql = f"COPY {FQN} ({col_list}) FROM STDIN WITH (FORMAT csv)"
    write_opts = pv.WriteOptions(include_header=False)

    rows = 0
    with conn.cursor() as cur:
        for batch in scanner.to_batches():
            table = align_columns(batch, rename)
            buf = io.BytesIO()
            pv.write_csv(table, buf, write_options=write_opts)
            buf.seek(0)
            # copy_expert streams the buffer straight into Postgres. This is
            # the bulk-load path — far faster than row-by-row INSERT.
            cur.copy_expert(copy_sql, buf)
            rows += table.num_rows
            print(f"         {rows:,} rows...", end="\r")

    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {SCHEMA}.load_audit (table_name, year, month, row_count)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (table_name, year, month)
            DO UPDATE SET row_count = EXCLUDED.row_count, loaded_at = now()
            """,
            (TABLE, year, month, rows),
        )

    # One commit per partition. If partition 3 fails, partitions 1 and 2 stay
    # safely committed and the re-run picks up where it stopped.
    conn.commit()
    return rows


def main() -> None:
    full_refresh = "--full-refresh" in sys.argv

    if not LAKE_DIR.exists():
        print(f"No lake at {LAKE_DIR}\nRun `python organize.py` first.")
        return

    partitions = find_partitions()
    if not partitions:
        print("No partitions found in the lake. Run `python organize.py` first.")
        return

    try:
        conn = connect()
    except Exception as exc:  # noqa: BLE001
        print(f"Could not connect to Postgres:\n  {exc}\n")
        print("Is the container running?  docker compose ps")
        return

    dataset = ds.dataset(str(LAKE_DIR), format="parquet", partitioning="hive")
    # Map actual source column names to lowercase versions.
    rename = {name: name.lower() for name in dataset.schema.names}

    create_objects(conn)
    done = set() if full_refresh else already_loaded(conn)

    mode = "FULL REFRESH" if full_refresh else "INCREMENTAL"
    print(f"\nLoad mode: {mode}")
    print(f"Target:    {FQN}")
    print(f"Found {len(partitions)} partition(s) in the lake\n")

    total, started = 0, time.perf_counter()
    for year, month in partitions:
        label = f"{year}-{month:02d}"
        if (year, month) in done:
            print(f"  skip   {label} (already loaded — use --full-refresh to redo)")
            continue
        print(f"  load   {label}")
        rows = load_partition(conn, dataset, rename, year, month)
        total += rows
        print(f"         {rows:,} rows loaded          ")

    elapsed = time.perf_counter() - started
    print(f"\nLoad complete. {total:,} rows in {elapsed:.1f}s.")
    if total:
        print(f"That is roughly {total / elapsed:,.0f} rows/second via COPY.")
    print("\nRe-run this script — it will skip everything. That is idempotency.")
    print("Next: python explore_sql.py\n")
    conn.close()


if __name__ == "__main__":
    main()
