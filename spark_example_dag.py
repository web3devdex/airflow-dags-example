VERSION_CODE = "08-00-00-22-07-2025"

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from datetime import datetime
import subprocess

@dag(
    dag_id="spark_wordcount_dag",
    schedule="@daily",       # tương đương schedule_interval='@daily'
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["spark", "pyspark"],
)
def spark_wordcount_dag():
    @task
    def run_spark_wordcount(execution_date: str = "{{ ds }}"):
        """
        Gọi spark-submit với PySpark job
        """
        bash_cmd = (f"spark-submit /opt/airflow/dags/repo/wordcount.py")
        print(f"Running: {bash_cmd}")
        subprocess.run(bash_cmd, shell=True, check=True)

    # Gọi task
    run_spark_wordcount()

spark_wordcount_dag()
