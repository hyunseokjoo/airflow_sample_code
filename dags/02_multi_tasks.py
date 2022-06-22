from datetime import datetime, timedelta
from time import sleep
from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum.tz.timezone import Timezone

default_args = {
    'owner' : 'eddie',
    'retries' : 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='ex_multi_tasks',
    description='다중 작업 순차 처리 예제입니다(병렬처리 x)',
    default_args=default_args,
    start_date=datetime(2022,6,21, tzinfo=Timezone("Asia/Seoul")),
    schedule_interval="@daily",
    tags=['test', 'multi tasks']
) as dag:

    # 잠깐 멈추는 함수 만들기 
    def dump() -> None:
        sleep(3)

    # 시작 함수
    start = PythonOperator(task_id='start', python_callable=dump)

    # 여러 태스크 동시 실행 할 수 있게 생성
    task_1 = PythonOperator(task_id='task_1', python_callable=dump)
    task_2 = PythonOperator(task_id='task_2', python_callable=dump)
    task_3 = PythonOperator(task_id='task_3', python_callable=dump)
    task_4 = PythonOperator(task_id='task_4', python_callable=dump)
    task_5 = PythonOperator(task_id='task_5', python_callable=dump)

    # 의존성 부여
    start >> task_1 >> task_2 >> task_5
    start >> task_3 >> task_4 >> task_5
