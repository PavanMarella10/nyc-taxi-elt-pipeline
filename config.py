"""
Central configuration for the NYC taxi ELT pipeline.

Keeping settings in one file (instead of scattered across scripts) is a small
but real industry habit: every other step imports from here, so there's one
place to change the dataset, the months, or the database connection.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

# Load variables from a local .env file into the environment.
load_dotenv()

# --- Dataset settings --------------------------------------------------------

# NYC TLC publishes one Parquet file per taxi type per month.
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# "yellow" is the classic Manhattan medallion taxi and the most common teaching
# dataset. Others: "green", "fhv", "fhvhv".
TAXI_TYPE = "yellow"

# The months to pull. Start small (2 files) so downloads and later steps are
# fast. Each file is ~50 MB. Add more months once the whole pipeline works.
# Format: (year, month). Data lags ~2 months, so recent months may not exist yet.
MONTHS = [
    (2024, 1),
    (2024, 2),
]

# --- Local paths -------------------------------------------------------------

# Project root = the folder this file lives in.
PROJECT_ROOT = Path(__file__).resolve().parent

# The "raw zone" of our data lake: untouched files exactly as downloaded.
# Rule of thumb: never modify raw. Every later step reads from here.
RAW_DIR = PROJECT_ROOT / "data" / "raw"


def file_name(year: int, month: int) -> str:
    """Build the standard TLC file name, e.g. yellow_tripdata_2024-01.parquet."""
    return f"{TAXI_TYPE}_tripdata_{year:04d}-{month:02d}.parquet"


def file_url(year: int, month: int) -> str:
    """Full download URL for a given month."""
    return f"{BASE_URL}/{file_name(year, month)}"


# --- Database settings (used in step 3, wired up now so it's ready) ----------

DB = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "user": os.getenv("POSTGRES_USER", "taxi"),
    "password": os.getenv("POSTGRES_PASSWORD", "taxi"),
    "dbname": os.getenv("POSTGRES_DB", "taxi"),
}


def db_url() -> str:
    """SQLAlchemy-style connection string."""
    return (
        f"postgresql+psycopg2://{DB['user']}:{DB['password']}"
        f"@{DB['host']}:{DB['port']}/{DB['dbname']}"
    )
