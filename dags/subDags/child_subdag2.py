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

def child_subdag2(parent_dag_name, child_dag_name) -> DAG:
    with DAG(
        dag_id=f"{parent_dag_name}.{child_dag_name}",
        default_args=default_args,
        schedule_interval="@daily",
        start_date=datetime(2022,6,22, tzinfo=kst),
        tags=['test', 'subdag', 'sample2'],
    ) as dag:
        
        child_subdag2_start = PythonOperator(
            task_id='child_subdag2_start',
            python_callable=dump
        )

        child_subdag2_task_1 = PythonOperator(
            task_id='child_subdag2_task_1',
            python_callable=dump
        )

        child_subdag2_task_2 = PythonOperator(
            task_id='child_subdag2_task_2',
            python_callable=dump
        )

        child_subdag2_task_3 = PythonOperator(
            task_id='child_subdag2_task_3',
            python_callable=dump
        )

        child_subdag2_end = DummyOperator(
            task_id='child_subdag2_end'
        )

        child_subdag2_start >> [child_subdag2_task_1, child_subdag2_task_2, child_subdag2_task_3] >> child_subdag2_end

        return dag
