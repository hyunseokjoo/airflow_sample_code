from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'hsjoo',
}

# Factory 정의
items = ['A', 'B', 'C']

with DAG(
    dag_id="ex_factory_pattern",
    default_args=default_args,
    start_date=datetime(2022, 4, 30),
    schedule_interval='@once',
    tags=['test', 'factory'],
) as dag:

    for item in items:
        task = BashOperator(
            task_id=f"task1_{item}",
            bash_command=f"echo '{item}'"
        )

        task
