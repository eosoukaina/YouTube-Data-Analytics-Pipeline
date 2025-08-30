from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import extraction
from transformation import transform_youtube_data
from Load import load_youtube_data

default_args = {
    'owner': 'Hanane',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 27),  # Corrected date format
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'youtube_etl_dag',
    default_args=default_args,
    description='YouTube Data Analysis Pipeline',
    schedule=timedelta(days=1),
    catchup=False,
)

extract_task = PythonOperator(
    task_id='extract_youtube_data',
    python_callable=extraction.extract_youtube_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_youtube_data',
    python_callable=transform_youtube_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_youtube_data',
    python_callable=load_youtube_data,
    dag=dag,
)

# Define task dependencies
extract_task >> transform_task >> load_task


# azure-synapse