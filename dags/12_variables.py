from time import sleep
from pendulum.tz.timezone import Timezone
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

kst = Timezone('Asia/Seoul')

default_args = {
    'owner' : 'eddie',
    'retries' : 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG (
    dag_id='ex_variables',
    default_args=default_args,
    schedule_interval='@once',
    start_date=datetime(2022,6,22, tzinfo=kst),
    tags=['test', 'variables sample'],
) as dag:

    def dump():
        sleep(3)

    # 일반 값 가져오기
    def get_variables1():
        value = Variable.get(key='test1')
        print(value)

    # json data 가져오기
    def get_variables2():
        json = Variable.get(key='test2', deserialize_json=True)
        value1 = json['value_1']
        value2 = json['value_2']

        print(value1)
        print(value2)

    start = PythonOperator(
        task_id='start',
        python_callable=dump
    )

    get_test1 = PythonOperator(
        task_id='get_test1',
        python_callable=get_variables1
    )

    get_test2 = PythonOperator(
        task_id='get_test2',
        python_callable=get_variables2
    )

    start >> get_test1 >> get_test2
