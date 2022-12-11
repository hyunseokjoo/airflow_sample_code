import pendulum
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# timezone 한국시간으로 변경
kst = pendulum.timezone("Asia/Seoul")

# 기본 args 생성
default_args = {
    'owner' : 'Hello World',
    'email' : ['airflow@airflow.com'],
    'email_on_failure': False,
}

# DAG 생성 
# 2022/06/21 @once 한번만 실행하는 DAG생성
with DAG(
    dag_id='ex_hello_world',
    default_args=default_args,
    start_date=datetime(2022, 6,21, tzinfo=kst),
    description='print hello world',
    schedule_interval='@once',
    tags=['test']
) as dag:

    # 앞에서 만든 validation.py 호출
    t1 = BashOperator(
        task_id='execute_validation',
        bash_command='set -e; python validator.py order_count.sql order_full_count.sql equals',
    )

    t1