"""
Step 2b — Organize the lake.

Read the raw files, derive partition columns from the data, and write a
partitioned dataset into data/lake/.

Concepts this teaches:
  - Zones: raw (immutable landing) vs. curated (organized, queryable).
  - Partitioning: folder structure that lets engines skip data entirely.
  - Hive-style layout: year=2024/month=1/ — the convention Spark, Athena,
    Glue, Snowflake and DuckDB all understand automatically.
  - Partition pruning: the query engine reads only the folders it needs.
  - A first look at dirty data: rows whose pickup date is not in the month
    the file claims to cover.

Note we derive the partition from the DATA (the pickup timestamp), not from
the file name. File names lie. Data is the source of truth.

Run it with:  python organize.py
"""

import shutil

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

import config

# The curated zone. Raw stays untouched; this is the organized copy.
LAKE_DIR = config.PROJECT_ROOT / "data" / "lake"

# The column holding the trip start time in the yellow taxi dataset.
TIME_COL = "tpep_pickup_datetime"


def add_partition_columns(table: pa.Table) -> pa.Table:
    """Derive year and month from the pickup timestamp and append them."""
    ts = table.column(TIME_COL)
    year = pc.year(ts).cast(pa.int32())
    month = pc.month(ts).cast(pa.int32())
    return table.append_column("year", year).append_column("month", month)


def report_stray_dates(table: pa.Table, expected_year: int, expected_month: int) -> None:
    """Count rows whose pickup date falls outside the file's stated month."""
    matches_year = pc.equal(table.column("year"), expected_year)
    matches_month = pc.equal(table.column("month"), expected_month)
    in_month = pc.and_(matches_year, matches_month)
    good = pc.sum(pc.cast(in_month, pa.int64())).as_py() or 0
    stray = table.num_rows - good

    print(f"         {good:,} rows in {expected_year}-{expected_month:02d}")
    if stray:
        pct = stray / table.num_rows * 100
        print(f"         {stray:,} rows ({pct:.3f}%) have dates OUTSIDE that month")


def process(year: int, month: int) -> int:
    """Partition one raw file into the lake. Returns rows written."""
    src = config.RAW_DIR / config.file_name(year, month)
    print(f"  read   {src.name}")

    table = pq.read_table(str(src))
    table = add_partition_columns(table)
    report_stray_dates(table, year, month)

    # write_to_dataset creates year=YYYY/month=M/ folders automatically and
    # APPENDS to any that already exist.
    pq.write_to_dataset(
        table,
        root_path=str(LAKE_DIR),
        partition_cols=["year", "month"],
        compression="snappy",
    )
    print(f"         written to lake\n")
    return table.num_rows


def show_layout() -> None:
    """Print the folder tree that partitioning produced."""
    print("=" * 70)
    print("LAKE LAYOUT — partitioning is just folders with a naming convention")
    print("=" * 70)
    for part in sorted(p for p in LAKE_DIR.rglob("*") if p.is_dir()):
        depth = len(part.relative_to(LAKE_DIR).parts)
        files = [f for f in part.iterdir() if f.is_file()]
        size = sum(f.stat().st_size for f in files) / 1_000_000
        indent = "  " * depth
        suffix = f"  ({len(files)} file(s), {size:.1f} MB)" if files else ""
        print(f"  {indent}{part.name}/{suffix}")

    print("\n  Those year=/month= folder names are Hive-style partitioning.")
    print("  Spark, Athena, Glue, Snowflake and DuckDB all read them as")
    print("  columns for free — no configuration needed.\n")
    print("  The payoff is partition pruning: a query filtered to January")
    print("  opens only the month=1 folder and never touches the rest. On a")
    print("  cloud warehouse that is a direct cut to your bill, because you")
    print("  are billed on bytes scanned.\n")
    print("  Partition on what you FILTER by (usually date). Do not partition")
    print("  on something high-cardinality like trip ID — thousands of tiny")
    print("  files is its own performance problem, the 'small files problem'.\n")


def main() -> None:
    if LAKE_DIR.exists():
        # Rebuilding from raw keeps this script idempotent: write_to_dataset
        # appends, so re-running without a clean start would duplicate rows.
        print(f"Clearing existing lake at {LAKE_DIR}\n")
        shutil.rmtree(LAKE_DIR)
    LAKE_DIR.mkdir(parents=True, exist_ok=True)

    total = 0
    for year, month in config.MONTHS:
        src = config.RAW_DIR / config.file_name(year, month)
        if not src.exists():
            print(f"  MISSING {src.name} — run `python extract.py` first\n")
            continue
        total += process(year, month)

    if total:
        show_layout()
        print(f"Organize complete. {total:,} rows partitioned into the lake.")
        print("Next: python query_lake.py\n")


if __name__ == "__main__":
    main()
