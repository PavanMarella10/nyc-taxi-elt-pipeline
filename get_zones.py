"""
Download the taxi zone lookup table into dbt/seeds/.

This is a small reference CSV mapping location IDs (132) to human names
(JFK Airport, Queens). It becomes dim_location in step 4.

Concepts this teaches:
  - Seeds: small, slow-changing reference data committed to the repo and
    loaded with `dbt seed`. Not everything comes from a source system.

Run it with:  python get_zones.py
"""

import requests

import config

URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
DEST = config.PROJECT_ROOT / "dbt" / "seeds" / "taxi_zone_lookup.csv"


def main() -> None:
    DEST.parent.mkdir(parents=True, exist_ok=True)

    if DEST.exists():
        print(f"Already have {DEST.name} — nothing to do.")
        return

    print(f"Downloading {URL}")
    response = requests.get(URL, timeout=60)
    response.raise_for_status()
    DEST.write_bytes(response.content)

    lines = response.text.strip().splitlines()
    print(f"Saved to {DEST}")
    print(f"{len(lines) - 1} zones\n")
    print("First few rows:")
    for line in lines[:4]:
        print(f"  {line}")
    print("\nNext: cd dbt && dbt seed --profiles-dir .\n")


if __name__ == "__main__":
    main()
