from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator  
from datetime import datetime

@dag(
    dag_id="spark_wordcount_dag",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["spark", "pyspark"],
)
def spark_wordcount_dag():
    run_spark = BashOperator(
        task_id="run_spark_wordcount",
        bash_command=(
            "spark-submit --master local[*] /opt/airflow/dags/repo/wordcount.py "
        ),
    )
    run_spark  # chỉ có 1 task
# Gọi DAG
spark_wordcount_dag()
