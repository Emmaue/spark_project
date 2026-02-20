from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# 1. Define the default settings for the pipeline
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# 2. Instantiate the DAG
with DAG(
    'traffic_datalake_pipeline',
    default_args=default_args,
    description='End-to-End CV Traffic Data Pipeline (Bronze -> Silver -> Gold)',
    schedule_interval='@daily', # Runs once a day at midnight
    start_date=datetime(2026, 2, 18),
    catchup=False,
    tags=['portfolio', 'computer_vision', 'spark', 's3'],
) as dag:

    # Task 1: API Ingestion (Raw Data -> Bronze Layer)
    # Note: We use /opt/airflow/ because that is the standard root path inside the Airflow Docker container
    ingest_raw_data = BashOperator(
        task_id='ingest_api_data',
        bash_command='python /opt/airflow/scripts/ingest_traffic_data.py',
    )

    # Task 2: Computer Vision Processing (Bronze -> Silver Layer)
    extract_cv_features = BashOperator(
        task_id='extract_cv_features',
        bash_command='python /opt/airflow/scripts/cv_feature_extraction.py',
    )

    # Task 3: Spark Transformations (Silver -> Gold Layer)
    transform_gold_data = BashOperator(
        task_id='spark_transform_gold',
        bash_command='python /opt/airflow/scripts/spark_transformation.py',
    )

    # 3. Define the Pipeline Execution Order
    ingest_raw_data >> extract_cv_features >> transform_gold_data