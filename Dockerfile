# Image for the always-on Python parts: producer.py, consumer.py and the Streamlit dashboard.
# The code itself is NOT copied in - docker-compose mounts the project folder at /app,
# so editing a .py file only needs a container restart, not a rebuild.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

# Same pinned versions as local development, minus dbt (only Airflow runs dbt)
COPY requirements.txt .
RUN grep -v '^dbt-' requirements.txt > /tmp/requirements.txt \
    && pip install --no-cache-dir -r /tmp/requirements.txt
