from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
from main import extract, transform, load_to_csv, load_to_mongodb

default_args = {
    'owner': 'emirhan',
    'start_date': days_ago(0),
    'end_date': datetime(2024, 10,24),
    'email': ['mremirhan131@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=60),
}

dag = DAG(
    'etl_data_pipeline',
    default_args=default_args,
    description='Apache Airfow DAG for ETL process',
    schedule_interval=timedelta(days=1),
)

extract_task = PythonOperator(
    task_id='extract_phase',
    python_callable=extract,
    dag=DAG,
)

transform_task = PythonOperator(
    task_id='transform_phase',
    python_callable=transform,
    dag=DAG,
)

load_csv_task = PythonOperator(
    task_id='load_csv_phase',
    python_callable=load_to_csv(transform),
    dag=DAG,
)

load_mongodb_task = PythonOperator(
    task_id='load_mongodb_phase',
    python_callable=load_to_mongodb(transform),
    dag=DAG,
)
