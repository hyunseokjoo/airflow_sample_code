import pendulum
from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# timezone 한국시간으로 변경
kst = pendulum.timezone("Asia/Seoul")

# 기본 args 생성
default_args = {
    'owner' : 'hsjoo',
    'email' : ['airflow@airflow.com'],
    'email_on_failure': False,
}

with DAG(
    dag_id='ex_execute_embulk',
    default_args=default_args,
    start_date=datetime(2022, 6,21, tzinfo=kst),
    description='print hello world',
    schedule_interval='@once',
    tags=['embulk_test']
) as dag:

    t1 = BashOperator(
        task_id='embulk_bash',
        bash_command='ssh "embulk" '
    )

    t1