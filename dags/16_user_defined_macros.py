from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'hsjoo',
}

# 매크로로 사용할 변수 설정
MACRO_VARS={
    "id" : "sampleID",
    "pw" : 1234,
    "dataset" : "userDS"
}

with DAG(
    dag_id="ex_user_defined_macros",
    default_args=default_args,
    start_date=datetime(2022, 4, 30),
    schedule_interval='@once',
    # MACRO_VARS 변수 설정
    # DAG객체 안에 user_defined_macros를 설정하면 Task에서 JinjaTemplate으로 불러 사용가능하다.
    user_defined_macros=MACRO_VARS,
    tags=['test', 'user_defined_macros sample'],
) as dag:
    
    task1 = BashOperator(
        task_id='task1', 
        # jinja template을 이용하여 변수 활용
        bash_command="echo '{{id}} and {{pw}} and {{dataset}}'"
    )

    task1