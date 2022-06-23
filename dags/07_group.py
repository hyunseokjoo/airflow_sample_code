from random import sample
from time import sleep
from datetime import datetime, timedelta
from pendulum.tz.timezone import Timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

kst = Timezone('Asia/Seoul')

default_args={
    'owner' : 'eddie',
    'retries' : 1,
    'retry_delay' :timedelta(minutes=1) 
}

def dump() -> None:
    sleep(3)

with DAG(
    dag_id='ex_group',
    description='group 샘플입니다.',
    default_args=default_args,
    start_date=datetime(2022,6,22, tzinfo=kst),
    schedule_interval='@once',
    tags=['test', 'group_sample'],
) as dag:
    
    start = PythonOperator(
        task_id='start',
        python_callable=dump
    )

    with TaskGroup(group_id='group_1') as sampleGroup:
        task_1 = PythonOperator(task_id='tast_1', python_callable=dump)
        task_2 = PythonOperator(task_id='tast_2', python_callable=dump)
        task_3 = PythonOperator(task_id='tast_3', python_callable=dump)
        task_4 = PythonOperator(task_id='tast_4', python_callable=dump)

        with TaskGroup(group_id='inner_group_1') as innerGroup:
            task_5 = PythonOperator(task_id='tast_5', python_callable=dump)
            task_6 = PythonOperator(task_id='tast_6', python_callable=dump)
            task_7 = PythonOperator(task_id='tast_7', python_callable=dump)

            task_5 >> task_7
            task_5 >> task_6

        task_1 >> innerGroup >> [task_2, task_3] >> task_4

    end = PythonOperator(
        task_id='end',
        python_callable=dump
    )

    start >> sampleGroup >> end