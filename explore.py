"""
Step 2a — Understand your raw data.

Before you move or transform data, you look at it. This script inspects one
Parquet file and proves *why* the industry uses Parquet instead of CSV.

Concepts this teaches:
  - Schema: Parquet stores column names AND types inside the file.
  - Columnar storage: data is grouped by column, not by row.
  - Column pruning: read only the columns you need (huge speed win).
  - Row groups: Parquet's internal chunking, the unit of parallel reads.
  - Compression: why a Parquet file is far smaller than the same data as CSV.

Run it with:  python explore.py
"""

import time

import pyarrow.parquet as pq

import config

# Use the first month in your config as the sample file.
YEAR, MONTH = config.MONTHS[0]
PATH = config.RAW_DIR / config.file_name(YEAR, MONTH)


def show_schema(pf: pq.ParquetFile) -> None:
    """Print every column and its data type."""
    print("=" * 70)
    print("SCHEMA — what columns exist and what type is each")
    print("=" * 70)
    schema = pf.schema_arrow
    for name, dtype in zip(schema.names, schema.types):
        print(f"  {name:<28} {dtype}")
    print(f"\n  {len(schema.names)} columns total")
    print("\n  Note: CSV has no types. Every value is text until you guess.")
    print("  Parquet stores the type with the data, so a timestamp stays a")
    print("  timestamp. That is one whole class of bugs you never have.\n")


def show_structure(pf: pq.ParquetFile) -> None:
    """Print row groups and file-level stats."""
    md = pf.metadata
    print("=" * 70)
    print("STRUCTURE — how the file is chunked internally")
    print("=" * 70)
    print(f"  Rows:        {md.num_rows:,}")
    print(f"  Row groups:  {md.num_row_groups}")
    print(f"  File size:   {PATH.stat().st_size / 1_000_000:.1f} MB")
    print("\n  A row group is a horizontal slice of the file. Engines like")
    print("  Spark assign row groups to different workers — that is how one")
    print("  file gets read in parallel. CSV cannot do this; it must be")
    print("  scanned start to finish.\n")


def show_column_pruning() -> None:
    """Time reading 2 columns vs all columns. The core columnar advantage."""
    print("=" * 70)
    print("COLUMN PRUNING — reading less means reading faster")
    print("=" * 70)

    start = time.perf_counter()
    few = pq.read_table(str(PATH), columns=["trip_distance", "total_amount"])
    few_time = time.perf_counter() - start

    start = time.perf_counter()
    allcols = pq.read_table(str(PATH))
    all_time = time.perf_counter() - start

    print(f"  2 columns:    {few_time:6.2f}s   {few.nbytes / 1_000_000:7.1f} MB in memory")
    print(f"  All columns:  {all_time:6.2f}s   {allcols.nbytes / 1_000_000:7.1f} MB in memory")
    if few_time > 0:
        print(f"\n  Reading 2 columns was {all_time / few_time:.1f}x faster.")
    print("\n  Because Parquet stores each column separately, it can skip the")
    print("  ones you did not ask for. A CSV row is one line of text, so you")
    print("  must read every field of every row to get at one column.")
    print("\n  This is why analytics warehouses are columnar and why")
    print("  SELECT * is a bad habit on large tables.\n")


def show_compression() -> None:
    """Estimate the same data as uncompressed CSV."""
    print("=" * 70)
    print("COMPRESSION — same data, much smaller on disk")
    print("=" * 70)
    table = pq.read_table(str(PATH))
    parquet_mb = PATH.stat().st_size / 1_000_000
    # In-memory Arrow size is a fair stand-in for uncompressed CSV size.
    raw_mb = table.nbytes / 1_000_000
    print(f"  Parquet on disk:   {parquet_mb:7.1f} MB")
    print(f"  Uncompressed:      {raw_mb:7.1f} MB")
    print(f"  Compression ratio: {raw_mb / parquet_mb:7.1f}x")
    print("\n  Columnar data compresses well because values in one column are")
    print("  similar to each other (all dates, all small integers). Mixed")
    print("  row-wise data does not compress nearly as well.")
    print("\n  In the cloud you pay for storage AND for bytes scanned. This")
    print("  ratio is money.\n")


def main() -> None:
    if not PATH.exists():
        print(f"File not found: {PATH}\nRun `python extract.py` first.")
        return

    print(f"\nInspecting: {PATH.name}\n")
    pf = pq.ParquetFile(str(PATH))
    show_schema(pf)
    show_structure(pf)
    show_column_pruning()
    show_compression()
    print("Done. Next: python organize.py\n")


if __name__ == "__main__":
    main()
