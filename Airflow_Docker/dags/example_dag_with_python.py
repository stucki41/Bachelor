from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {"owner": "daniel", "retries": 5, "retry_delay": timedelta(minutes=5)}

def task1():
    print("Hello World! This is the first task")

def task2():
    print("Hello World! This is the second task")

def task3():
    print("Hello World! This is the third task")
    
with DAG(
    default_args=default_args,
    dag_id="dag_with_python_operator",
    description="dag using python operator",
    start_date=datetime(2024, 4, 22),
    schedule_interval="@daily",
) as dag:
    task1 = PythonOperator(task_id="task1", 
                           python_callable=task1)
    task2 = PythonOperator(task_id="task2",
                           python_callable=task2)
    task3 = PythonOperator(task_id="task3",
                           python_callable=task3)

    [task2, task3] >> task1