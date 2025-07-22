VERSION_CODE="08-00-00-22-07-2025"
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from time import sleep

@dag(schedule=None)
def parallel_dag():
    @task
    def task_1():
        print("task_1")
        sleep(30)

    @task
    def task_2():
        print("task_2")
        sleep(30)

    @task
    def task_3():
        print("task_3")
        sleep(30)
        
    @task
    def task_4():
        print("task_4")
        print('Done')

    chain([task_1(), task_2(), task_3()], task_4())

parallel_dag()
