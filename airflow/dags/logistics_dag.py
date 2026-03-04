from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

# Path constants (resolved inside the container)
SRC_DIR       = '/opt/airflow/src'
DBT_DIR       = '/opt/airflow/logistics_pipeline'
DBT_PROFILES  = '/home/airflow/.dbt'
DBT_TARGET    = 'docker'

with DAG(
    dag_id='logistics_data_pipeline',
    default_args=default_args,
    description='End-to-end orchestration: MinIO ingest → DQ → dbt run → dbt test',
    schedule_interval='@hourly',
    catchup=False,
    tags=['logistics', 'dbt', 'minio'],
) as dag:

    # 1. Stream / consume new events into MinIO (bronze bucket)
    stream_to_minio = BashOperator(
        task_id='stream_to_minio',
        bash_command=(
            'python {{ params.src_dir }}/stream/consumer_to_minio.py '
            '--duration 60'
        ),
        params={'src_dir': SRC_DIR},
    )

    # 2. Load bronze parquet files from MinIO into DuckDB
    load_to_duckdb = BashOperator(
        task_id='load_to_duckdb',
        bash_command='python {{ params.src_dir }}/warehouse/_init_duckdb.py',
        params={'src_dir': SRC_DIR},
    )

    # 3. Pre-transformation data quality gate
    run_dq_checks = BashOperator(
        task_id='data_quality_check',
        bash_command='python {{ params.src_dir }}/warehouse/data_quality_check.py',
        params={'src_dir': SRC_DIR},
    )

    # 4. dbt transformations (staging → intermediate → dim → fact)
    run_dbt = BashOperator(
        task_id='dbt_transform',
        bash_command=(
            'cd {{ params.dbt_dir }} && '
            'dbt run '
            '--profiles-dir {{ params.profiles }} '
            '--target {{ params.target }}'
        ),
        params={
            'dbt_dir': DBT_DIR,
            'profiles': DBT_PROFILES,
            'target': DBT_TARGET,
        },
    )

    # 5. dbt tests (post-transformation data quality)
    test_dbt = BashOperator(
        task_id='dbt_test',
        bash_command=(
            'cd {{ params.dbt_dir }} && '
            'dbt test '                          
            '--profiles-dir {{ params.profiles }} '
            '--target {{ params.target }}'
        ),
        params={
            'dbt_dir': DBT_DIR,
            'profiles': DBT_PROFILES,
            'target': DBT_TARGET,
        },
    )

    # 6. Log completion
    log_success = PythonOperator(
        task_id='log_pipeline_success',
        python_callable=lambda **ctx: logging.info(
            "Pipeline completed successfully for execution date: "
            f"{ctx['execution_date']}"
        ),
        provide_context=True,
    )

    # Dependencies
    stream_to_minio >> load_to_duckdb >> run_dq_checks >> run_dbt >> test_dbt >> log_success