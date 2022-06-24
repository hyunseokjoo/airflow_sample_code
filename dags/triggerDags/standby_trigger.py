from time import sleep
from datetime import datetime, timedelta
from airflow import DAG
from pendulum.tz.timezone import Timezone
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

kst = Timezone('Asia/Seoul')

default_args={
    'owner' : 'eddie',
    'retries' : 1,
    'retry_delay' : timedelta(minutes=1)
}

def dump() -> None:
    sleep(3)

with DAG(
    dag_id='standby_trigger',
    description='trigger 기다리는 DAG입니다.',
    default_args=default_args,
    start_date=datetime(2022,6,22, tzinfo=kst),
    schedule_interval=None, # trigger DAG는 보통 None으로 처리 합니다.
    tags=['test', 'triggered_by_trigger_DAG_sample']
) as dag:

    trigger_start = PythonOperator(
        task_id='trigger_start',
        python_callable=dump
    )

    trigger_task_1 = PythonOperator(
        task_id='trigger_task_1',
        python_callable=dump
    )

    trigger_end = PythonOperator(
        task_id='trigger_end',
        python_callable=dump,
    )

    trigger_start >> trigger_task_1 >> trigger_end