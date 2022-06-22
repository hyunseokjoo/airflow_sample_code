import pendulum
from time import sleep

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.dummy_operator import DummyOperator


kst = pendulum.tz.timezone("Asia/Seoul")

default_args={
    'owner': 'eddie',
    'retries': 1,
    'retriy_delay': timedelta(minutes=1)
}

def dump() -> None:
    sleep(3)

def chkTrue():
    value = 1
    if value == 1:
        return 'true'
    return 'fasle'

with DAG(
    dag_id='ex_branch_task',
    description='',
    default_args=default_args,
    start_date=datetime(2022,6,21, tzinfo=kst),
    schedule_interval="@once",
    tags=['test', 'branch'],
    catchup=False,
) as dag:

    start = PythonOperator(
        task_id='start',
        python_callable=dump
    )
    
    branch = BranchPythonOperator(
        task_id='branch',
        python_callable=chkTrue
    )

    true = DummyOperator(
        task_id='true'
    )

    false = DummyOperator(
        task_id='false'
    )

    start >> branch >> [true, false]