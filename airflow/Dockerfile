# Airflow image with what the stock pipeline needs:
#  - loader.py packages (boto3, Snowflake connector) in Airflow's own environment
#  - dbt in a separate virtualenv, so its dependencies never clash with Airflow's
FROM apache/airflow:3.3.2-python3.12

RUN pip install --no-cache-dir \
        "apache-airflow==3.3.2" \
        "snowflake-connector-python==4.8.0" \
        "boto3" \
        "python-dotenv"

RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir \
        "dbt-core==1.12.5" \
        "dbt-snowflake==1.12.1"
