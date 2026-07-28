"""
NYC Taxi ELT pipeline — Airflow DAG.

This is the orchestration layer. Until now you ran extract.py, organize.py,
load.py and dbt by hand, in the right order, hoping nothing failed halfway.
That does not scale and does not survive you going on holiday.

Airflow turns those manual steps into a DAG — a Directed Acyclic Graph — that
runs on a schedule, in the correct order, retries on failure, and records what
happened. This one file is the thing interviewers ask about when they say
"how do you orchestrate your pipelines?", so it is worth understanding line
by line.

WHY "directed acyclic graph":
  - directed  — tasks have a direction: extract THEN load, never the reverse
  - acyclic   — no loops; a pipeline that could loop back on itself could run
                forever
  - graph     — tasks (nodes) connected by dependencies (edges)

The tasks here run inside the Airflow container, which has the project mounted
at /opt/airflow/project and reaches the taxi Postgres database on the host.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


# Where the project is mounted inside the container (see docker-compose.yaml).
PROJECT = "/opt/airflow/project"

# dbt lives in its own isolated virtualenv in the image so its dependencies
# cannot clash with Airflow's. See the Dockerfile.
DBT = "/opt/dbt-venv/bin/dbt"


# default_args apply to every task unless a task overrides them. This is where
# the reliability behaviour lives.
default_args = {
    "owner": "pavan",
    # If a task fails, try it again rather than failing the whole run. Most
    # real failures are transient — a network blip, a database briefly busy.
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    # Do not start a task until its upstream tasks have actually succeeded.
    "depends_on_past": False,
}


with DAG(
    dag_id="nyc_taxi_elt",
    description="Extract, land, load and model NYC yellow taxi trips.",
    default_args=default_args,

    # WHEN it runs. '@daily' means once a day at midnight. Real pipelines are
    # scheduled to match how often new data arrives.
    schedule="@daily",

    # The first logical date. Airflow reasons about time in terms of this.
    start_date=datetime(2024, 1, 1),

    # catchup=False is important. With start_date in the past and catchup=True,
    # Airflow would immediately try to run every missed day since January 2024
    # — hundreds of runs at once. False means "only run from now on". Turn it
    # on deliberately when you actually want to backfill history.
    catchup=False,

    # Stop one slow run from overlapping the next.
    max_active_runs=1,

    # Tags are just labels for finding this DAG in the UI.
    tags=["nyc-taxi", "elt", "portfolio"],
) as dag:

    # Each task is one stage of your pipeline. BashOperator runs a shell
    # command; here each command runs one of the scripts you already wrote.
    # Nothing about those scripts changed — Airflow just calls them in order.

    extract = BashOperator(
        task_id="extract",
        bash_command=f"cd {PROJECT} && python extract.py",
        doc_md="Download the source Parquet files. Idempotent: skips files already present.",
    )

    organize = BashOperator(
        task_id="organize",
        bash_command=f"cd {PROJECT} && python organize.py",
        doc_md="Partition the raw files into the Hive-style lake by year and month.",
    )

    load = BashOperator(
        task_id="load",
        bash_command=f"cd {PROJECT} && python load.py",
        doc_md="Bulk-load the lake into Postgres. Idempotent: skips partitions already loaded.",
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"cd {PROJECT}/dbt && {DBT} seed --profiles-dir .",
        doc_md="Load the taxi zone lookup reference data.",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {PROJECT}/dbt && {DBT} build --profiles-dir .",
        doc_md="Run every model AND its tests, in dependency order. Bad data stops here.",
    )

    # THE DEPENDENCIES. This single line defines the whole graph. The >>
    # operator means "then" — it points the arrows.
    #
    #   extract >> organize >> load >> dbt_seed >> dbt_build
    #
    # Airflow reads this and knows: never start organize until extract has
    # succeeded, never load until organize has succeeded, and so on. If load
    # fails, dbt_seed and dbt_build simply never start — no half-modelled
    # garbage in your warehouse.
    extract >> organize >> load >> dbt_seed >> dbt_build
