FROM apache/airflow:2.9.0-python3.12

USER root

RUN apt-get update && apt-get install -y \
    gcc \
    librdkafka-dev \
    curl \
    && apt-get clean

USER airflow

RUN pip install --no-cache-dir \
    confluent-kafka \
    duckdb \
    dbt-core \
    dbt-duckdb \
    fastapi \
    uvicorn \
    faker \
    boto3 \
    pandas \
    pyarrow \
    minio