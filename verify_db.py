"""
Quick check that the Postgres container is up and reachable.

This isn't the load step (that's step 3) — it just confirms your database is
running and your .env credentials work, so the next step goes smoothly.

Run it with:  python verify_db.py
"""

from sqlalchemy import create_engine, text

import config


def main() -> None:
    engine = create_engine(config.db_url())
    try:
        with engine.connect() as conn:
            version = conn.execute(text("SELECT version()")).scalar()
        print("Connected to Postgres.")
        print(version)
    except Exception as exc:  # noqa: BLE001
        print("Could not connect to Postgres.")
        print(f"  {exc}")
        print("\nIs the container running?  docker compose ps")


if __name__ == "__main__":
    main()
