"""
이전 task에 의존성을 걸어야 할 때 사용하는 조건
Depends_on_past : 이전 날짜의 task instance 들 중 하나라도 fail일 때는 성공할때 까지 기다린다
Wait_for_downstream : 이전 날짜의 task instance 들 중 fail인 것까지 작동하고 나머지는 no status로 대기한다.
"""
from time import sleep
from datetime import datetime, timedelta
from pendulum.tz.timezone import Timezone
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


kst=Timezone('Asia/Seoul')

default_args={
    'owner' : 'eddie',
    'retries' : 1,
    'retry_delay' : timedelta(minutes=1),
    'depends_on_past':True,
    'wait_for_downstream':False,
}

with DAG(
    dag_id="ex_dependencies_past",
    description='직전 dag, task를 바라보는 의존성(작업실행 할지 여부) 결정하기',
    default_args=default_args,
    start_date=days_ago(5),
    schedule_interval='@daily', 
    tags=['test', 'depends_on_past', 'wait_for_downstream'],
    catchup=False,
) as dag:

    t1 = DummyOperator(
        task_id='t1'
    )
    t2 = DummyOperator(
        task_id='t2',
    )
    t3 = DummyOperator(
        task_id='t3'
    )
    t4 = DummyOperator(
        task_id='t4'
    )

    t1 >> t2 >> t3 >> t4

