import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'amanda',
    'start_date': dt.datetime(2024, 11, 1), 
    'retries': 1,
    'retry_delay': timedelta(minutes=10), 
}

with DAG(
    dag_id='milestone3_schedule',
    default_args=default_args,
    description='ETL pipeline for crypto data every Saturday from 09:10 AM to 09:30 AM',
    schedule_interval='10,20,30 9 * * 6',  
    catchup=False
) as dag:

    # Task 1: Extract
    python_extract = BashOperator(
        task_id='python_extract',
        bash_command='sudo -u airflow python /opt/airflow/scripts/extract.py'
    )

    # Task 2: Transform
    python_transform = BashOperator(
        task_id='python_transform',
        bash_command='sudo -u airflow python /opt/airflow/scripts/transform.py'
    )

    # Task 3: Load
    python_load = BashOperator(
        task_id='python_load',
        bash_command='sudo -u airflow python /opt/airflow/scripts/load.py'
    )

    python_extract >> python_transform >> python_load