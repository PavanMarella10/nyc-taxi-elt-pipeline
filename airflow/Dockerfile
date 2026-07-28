# Custom Airflow image for the taxi pipeline.
#
# We build our own image instead of using the stock one because our tasks need
# libraries the base image does not have: pyarrow, requests, psycopg2 and dbt.
#
# Building a custom image (rather than installing packages at container start)
# is the production-standard approach — the image is built once, is
# reproducible, and starts fast every time.

FROM apache/airflow:2.10.5

# --- Pipeline dependencies -------------------------------------------------
# These are the libraries extract.py, organize.py and load.py import.
#
# Deliberately NOT installing sqlalchemy here: Airflow pins its own version and
# upgrading it would break Airflow. load.py uses psycopg2 directly, so we do
# not need it anyway.
RUN pip install --no-cache-dir \
        requests \
        pyarrow \
        psycopg2-binary \
        python-dotenv

# --- dbt in an isolated virtualenv -----------------------------------------
# dbt and Airflow both pin common libraries (jinja2, click) to different
# versions. Installing dbt into its own virtualenv keeps the two from fighting.
# The DAG calls /opt/dbt-venv/bin/dbt explicitly.
#
# We switch to root to create the venv under /opt (which the airflow user
# cannot write to), then hand ownership back to the airflow user and switch
# back. Containers should never RUN as root, so the USER airflow line at the
# end matters.
USER root
RUN python -m venv /opt/dbt-venv && \
    /opt/dbt-venv/bin/pip install --no-cache-dir dbt-core dbt-postgres && \
    chown -R airflow: /opt/dbt-venv
USER airflow
