from datetime import datetime, timedelta
from time import sleep
from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from pendulum.tz.timezone import Timezone

kst = Timezone('Asia/Seoul')

default_args = {
    'owner':'eddie',
    'retries' : 1,
    'retriy_delay':timedelta(minutes=1),
}

with DAG(
    dag_id="ex_xcoms",    
    description="xcom으로 task간 데이터 주고 받는 실습",
    default_args=default_args,
    schedule_interval="@once",
    start_date=datetime(2022,6,22, tzinfo=kst),
    tags=['test','xcom sample']
) as dag:

    def dump():
        sleep(3)
    
    start = PythonOperator(
        task_id="start",
        python_callable=dump
    )

    # xcom 방법 1 pythonoperator
    def pythonOperator_return_xcom():
        return "python은 return 값이 xcom으로 바로 반환됨"

    
    def get_pythonOperator_xcom(**context):
        res = context['task_instance'].xcom_pull(task_ids=f'pythonOperator_return_xcom')
        print(res)

    pythonOperator_return_xcom = PythonOperator(
        task_id='pythonOperator_return_xcom',
        python_callable=pythonOperator_return_xcom,
    )

    # xcom 방법 2 push pull 이용
    def push_value(**context):
        task_instance = context['task_instance']
        task_instance.xcom_push(key='pushedValue', value="값이 넣어 졌지")

    def pull_value(**context):
        res = context['task_instance'].xcom_pull(key='pushedValue')
        print(res)

    pushed_value = PythonOperator(
        task_id='pushed_value',
        python_callable=push_value
    )

    pulled_value_python = PythonOperator(
        task_id='pulled_value',
        python_callable=pull_value
    )

    # xcom 방법 3 jinja template 이용
    def pull_value_from_jinja(**context):
        res = context['task_instance'].xcom_pull(key='jinja')
        print(res)

    jinja_template = BashOperator(
        task_id='jinja_template',
        bash_command='echo "{{task_instance.xcom_push(key="jinja", value="템플릿 멋져")}}"'
    )

    pulled_value_jinja = PythonOperator(
        task_id='pulled_value_jinja',
        python_callable=pull_value_from_jinja
    )

    start >> pythonOperator_return_xcom
    start >> pushed_value >> pulled_value_python
    start >> jinja_template >> pulled_value_jinja

    