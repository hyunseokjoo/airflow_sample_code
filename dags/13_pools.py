from time import sleep
from pendulum.tz.timezone import Timezone
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

kst = Timezone('Asia/Seoul')

default_args={
    'owner':'eddie',
    'retries':1,
    'retry_delay':timedelta(minutes=1),
}

with DAG(
    dag_id='ex_pools',
    default_args=default_args,
    start_date=datetime(2022,6,22, tzinfo=kst),
    schedule_interval="@once",
    tags=['test', 'pool sample']
) as dag:
    def dump(interval):
        sleep(interval)

    start = PythonOperator(
        task_id='start',
        python_callable=dump,
        op_args=[3],
    )

    task_heavy = PythonOperator(
        task_id='task_heavy',
        python_callable=dump,
        op_args=[500],
        pool='single_pool',
    )

    task_medium = PythonOperator(
        task_id='task_medium',
        python_callable=dump,
        op_args=[300],
        pool='single_pool'
    )

    task_light = PythonOperator(
        task_id='task_light',
        python_callable=dump,
        op_args=[100],
        pool='single_pool',
    )

    end = PythonOperator(
        task_id='end',
        python_callable=dump,
        op_args=[3],
    )

    start >> [task_heavy, task_medium, task_light] >> end
