from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_welcome():
    print("Hello from Germany! I'm ready to build ETL pipelines.")

with DAG(
    dag_id="hello_germany",
    start_date=datetime(2025, 11, 3),
    schedule="@daily",
    catchup=False,
    tags=["germany", "etl"]
) as dag:

    welcome_task = PythonOperator(
        task_id="say_hello",
        python_callable=print_welcome
    )

    welcome_task