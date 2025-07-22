from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='spark_wordcount_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    run_spark_job = BashOperator(
        task_id='run_spark_wordcount',
        bash_command=(
            'spark-submit '
            '--master local[2] '
            '/opt/airflow/dags/repo/wordcount.py '
            '/opt/airflow/dags/repo/input.txt '
            '/opt/airflow/dags/repo/output_{{ ds }}'
        )
    )

    run_spark_job
