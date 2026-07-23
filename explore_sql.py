"""
Step 3b — Ask questions in SQL.

Now that the data is in Postgres you can query it. This runs a few SQL
statements against your staging table and prints the results.

Concepts this teaches:
  - Why we bother loading into a database: SQL is how everyone asks questions.
  - Profiling: look at your data before you model it.
  - Finding the data quality problems that step 4 will fix.

Run it with:  python explore_sql.py
"""

import psycopg2

import config

FQN = "raw.yellow_trips"

QUERIES = [
    (
        "How much data do we have?",
        f"""
        SELECT year, month, COUNT(*) AS trips
        FROM {FQN}
        GROUP BY year, month
        ORDER BY year, month
        """,
    ),
    (
        "What does the audit table say?",
        """
        SELECT year, month, row_count, loaded_at
        FROM raw.load_audit
        ORDER BY year, month
        """,
    ),
    (
        "Busiest hours of the day",
        f"""
        SELECT EXTRACT(HOUR FROM tpep_pickup_datetime)::INT AS hour,
               COUNT(*) AS trips,
               ROUND(AVG(total_amount)::NUMERIC, 2) AS avg_fare
        FROM {FQN}
        GROUP BY 1
        ORDER BY trips DESC
        LIMIT 5
        """,
    ),
    (
        "Top pickup locations",
        f"""
        SELECT pulocationid, COUNT(*) AS trips
        FROM {FQN}
        GROUP BY 1
        ORDER BY trips DESC
        LIMIT 5
        """,
    ),
    (
        "DATA QUALITY: impossible values we will fix in step 4",
        f"""
        SELECT
          COUNT(*) FILTER (WHERE total_amount < 0)        AS negative_fares,
          COUNT(*) FILTER (WHERE trip_distance <= 0)      AS zero_distance,
          COUNT(*) FILTER (WHERE trip_distance > 200)     AS absurd_distance,
          COUNT(*) FILTER (WHERE passenger_count = 0)     AS zero_passengers,
          COUNT(*) FILTER (WHERE passenger_count IS NULL) AS null_passengers,
          COUNT(*) FILTER (
            WHERE tpep_dropoff_datetime <= tpep_pickup_datetime
          ) AS dropoff_before_pickup
        FROM {FQN}
        """,
    ),
]


def run(cur, title: str, sql: str) -> None:
    print("=" * 70)
    print(title)
    print("=" * 70)
    cur.execute(sql)
    headers = [d[0] for d in cur.description]
    rows = cur.fetchall()

    widths = [len(h) for h in headers]
    for row in rows:
        for i, value in enumerate(row):
            widths[i] = max(widths[i], len(str(value)))

    print("  " + "  ".join(h.ljust(widths[i]) for i, h in enumerate(headers)))
    print("  " + "  ".join("-" * w for w in widths))
    for row in rows:
        print("  " + "  ".join(str(v).ljust(widths[i]) for i, v in enumerate(row)))
    print()


def main() -> None:
    try:
        conn = psycopg2.connect(
            host=config.DB["host"],
            port=config.DB["port"],
            user=config.DB["user"],
            password=config.DB["password"],
            dbname=config.DB["dbname"],
        )
    except Exception as exc:  # noqa: BLE001
        print(f"Could not connect to Postgres:\n  {exc}")
        return

    with conn.cursor() as cur:
        for title, sql in QUERIES:
            try:
                run(cur, title, sql)
            except Exception as exc:  # noqa: BLE001
                conn.rollback()
                print(f"  Query failed: {exc}\n")

    conn.close()

    print("Look at that last table. Trips with negative fares, zero distance,")
    print("dropoffs before pickups. This is what real data looks like.")
    print()
    print("Notice we did NOT fix any of it. The raw layer stays faithful to")
    print("the source so you can always prove what arrived. Cleaning happens")
    print("in step 4, in version-controlled SQL that can be tested and")
    print("reviewed — not hidden inside a load script.")
    print()
    print("Step 3 complete.\n")


if __name__ == "__main__":
    main()
