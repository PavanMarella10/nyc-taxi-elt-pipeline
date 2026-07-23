"""
Step 2c — Prove the partitioning actually pays off.

Query the partitioned lake and watch the engine skip data it does not need.

Concepts this teaches:
  - Dataset API: treat a folder of Parquet files as ONE logical table.
  - Partition pruning: filtering on a partition column skips whole folders.
  - Predicate pushdown: filters are applied while reading, not after.
  - Partition columns are real columns even though they are only folder names.

Run it with:  python query_lake.py
"""

import time

import pyarrow.compute as pc
import pyarrow.dataset as ds

import config

LAKE_DIR = config.PROJECT_ROOT / "data" / "lake"


def main() -> None:
    if not LAKE_DIR.exists():
        print(f"No lake at {LAKE_DIR}\nRun `python organize.py` first.")
        return

    # One line turns a folder tree into a single queryable table. The
    # partitioning="hive" flag tells Arrow to read year=/month= as columns.
    dataset = ds.dataset(str(LAKE_DIR), format="parquet", partitioning="hive")

    print("=" * 70)
    print("THE LAKE AS ONE TABLE")
    print("=" * 70)
    print(f"  Files:   {len(dataset.files)}")
    print(f"  Columns: {len(dataset.schema.names)}")
    print("\n  'year' and 'month' are queryable columns even though they only")
    print("  exist as folder names. That is the Hive convention working.\n")

    first_year, first_month = config.MONTHS[0]

    print("=" * 70)
    print("PARTITION PRUNING — filtering on a partition skips whole folders")
    print("=" * 70)

    start = time.perf_counter()
    one = dataset.to_table(
        filter=(pc.field("year") == first_year) & (pc.field("month") == first_month),
        columns=["trip_distance", "total_amount"],
    )
    one_time = time.perf_counter() - start

    start = time.perf_counter()
    every = dataset.to_table(columns=["trip_distance", "total_amount"])
    all_time = time.perf_counter() - start

    print(f"  One month:   {one.num_rows:>10,} rows   {one_time:5.2f}s")
    print(f"  Everything:  {every.num_rows:>10,} rows   {all_time:5.2f}s")
    print("\n  The filtered read never opened the other month's folder. Add")
    print("  five years of data and that gap becomes enormous — the cost of")
    print("  a partitioned query depends on what you asked for, not on how")
    print("  much data you own.\n")

    print("=" * 70)
    print("A REAL QUERY")
    print("=" * 70)
    trips = dataset.to_table(columns=["trip_distance", "total_amount", "month"])
    dist = trips.column("trip_distance")
    amt = trips.column("total_amount")

    print(f"  Total trips:      {trips.num_rows:,}")
    print(f"  Mean distance:    {pc.mean(dist).as_py():.2f} miles")
    print(f"  Mean fare:        ${pc.mean(amt).as_py():.2f}")
    print(f"  Max distance:     {pc.max(dist).as_py():,.1f} miles")
    print(f"  Min fare:         ${pc.min(amt).as_py():.2f}")

    print("\n  Look at that max distance and min fare. A negative fare and a")
    print("  thousand-mile taxi ride are not real trips — they are data")
    print("  quality problems. You do NOT fix them here. Raw and lake stay")
    print("  faithful to the source; cleaning happens in the transform layer")
    print("  where it is version-controlled and testable.\n")
    print("  Write that down for interviews: clean in transform, not on")
    print("  ingest. If you scrub data on the way in, you can never prove")
    print("  what the source actually said.\n")

    print("Step 2 complete.\n")


if __name__ == "__main__":
    main()
