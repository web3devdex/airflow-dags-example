from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

@dag(
    dag_id="spark_wordcount_dag",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["spark", "pyspark"],
)
def spark_wordcount_dag():
    spark_wordcount = SparkSubmitOperator(
        task_id="spark_wordcount_task",
        application="/opt/airflow/dags/repo/wordcount.py",
        conn_id="spark_default",
        application_args=[
        ],
        executor_cores=2,
        executor_memory="1g",
        driver_memory="1g",
        verbose=True,
    )

    spark_wordcount  # chỉ có 1 task

# Gọi DAG
spark_wordcount_dag()
