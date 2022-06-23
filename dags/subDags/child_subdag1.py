from time import sleep
from datetime import datetime, timedelta
from pendulum.tz.timezone import Timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

kst=Timezone('Asia/Seoul')

def dump() -> None:
    sleep(3)

default_args={
    'owner': 'eddie',
    'retries':1,
    'retry_delay':timedelta(minutes=1),
}

def child_subdag1(parent_dag_name, child_dag_name) -> DAG:
    with DAG(
        dag_id=f"{parent_dag_name}.{child_dag_name}",
        default_args=default_args,
        schedule_interval="@daily",
        start_date=datetime(2022,6,22, tzinfo=kst),
        tags=['test', 'subdag', 'sample1'],
    ) as dag:
        
        child_subdag1_start = PythonOperator(
            task_id='child_subdag1_start',
            python_callable=dump
        )

        child__subdag1_task = PythonOperator(
            task_id='child__subdag1_task',
            python_callable=dump
        )

        child__subdag1_end = DummyOperator(
            task_id='child__subdag1_end'
        )

        child_subdag1_start >> child__subdag1_task >> child__subdag1_end

        return dag