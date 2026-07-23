"""
Step 1 — Extract.

Download the raw NYC taxi Parquet files into data/raw/.

Concepts this teaches:
  - Batch extraction: pull a whole file at a time (vs. streaming row-by-row).
  - The raw zone of a data lake: land data untouched before transforming it.
  - Idempotency: re-running should be safe. We skip files already downloaded.
  - Validation: confirm the file is real by opening it and counting rows.

Run it with:  python extract.py
"""

import sys

import requests
import pyarrow.parquet as pq

import config


def download(year: int, month: int) -> None:
    """Download one month's file, skipping it if we already have it."""
    url = config.file_url(year, month)
    dest = config.RAW_DIR / config.file_name(year, month)

    if dest.exists():
        print(f"  skip   {dest.name} (already downloaded)")
        return

    print(f"  fetch  {dest.name}")
    # stream=True downloads in chunks so we never hold the whole file in memory.
    with requests.get(url, stream=True, timeout=60) as response:
        response.raise_for_status()  # turn a 404/500 into a clear error
        with open(dest, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)

    size_mb = dest.stat().st_size / 1_000_000
    print(f"         done ({size_mb:.1f} MB)")


def validate(year: int, month: int) -> int:
    """Open the Parquet file and return its row count. Proves the file is good."""
    path = config.RAW_DIR / config.file_name(year, month)
    # Reading only the metadata is instant, even for millions of rows.
    return pq.ParquetFile(path).metadata.num_rows


def main() -> None:
    config.RAW_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Raw data folder: {config.RAW_DIR}\n")

    total_rows = 0
    for year, month in config.MONTHS:
        try:
            download(year, month)
            rows = validate(year, month)
            total_rows += rows
            print(f"         {rows:,} rows\n")
        except requests.HTTPError:
            print(
                f"         ERROR: {config.file_name(year, month)} not found. "
                f"That month may not be published yet — try an earlier one in config.py.\n"
            )
        except Exception as exc:  # noqa: BLE001 - keep the message friendly for now
            print(f"         ERROR: {exc}\n")
            sys.exit(1)

    print(f"Extract complete. {total_rows:,} total rows across {len(config.MONTHS)} file(s).")


if __name__ == "__main__":
    main()
