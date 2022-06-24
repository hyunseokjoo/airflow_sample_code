from time import sleep
from datetime import datetime, timedelta
from pendulum.tz.timezone import Timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.edgemodifier import Label

kst = Timezone('Asia/Seoul')

default_args={
    'owner' : 'eddie',
    'retries' : 1,
    'retry_delay' :timedelta(minutes=1) 
}

def dump() -> None:
    sleep(3)

with DAG(
    dag_id='ex_label',
    description='group 샘플입니다.',
    default_args=default_args,
    start_date=datetime(2022,6,22, tzinfo=kst),
    schedule_interval='@once',
    tags=['test', 'label_sample'],
    doc_md="""
        여기에 작성하면 문서로 작성이 가능합니다.
        json, image 등 다양한 내용을 기입이 가능합니다.
    """
) as dag:

    start = PythonOperator(
        task_id='start',
        python_callable=dump
    )
    
    task_1 = PythonOperator(
        task_id='task_1',
        python_callable=dump,
    )

    end = PythonOperator(
        task_id='end',
        python_callable=dump
    )

    start >> Label('start') >> task_1 >> Label('end') >> end