import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes = 5),
}

if 'AIRFLOW_HOME' in os.environ:
    etl_script = '/opt/airflow/app/etl.py'
else:
    etl_script = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../etl.py'))

with DAG(
    dag_id='etl_process',
    default_args=default_args,
    description='Run extract, transform, and load steps separately',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:
    extract_task = BashOperator(
        task_id='extract',
        bash_command=f'python {etl_script} --process extract'
    )

    transform_task = BashOperator(
        task_id='transform',
        bash_command=f'python {etl_script} --process transform'
    )

    load_task = BashOperator(
        task_id='load',
        bash_command=f'python {etl_script} --process load'
    )

    extract_task >> transform_task >> load_task
