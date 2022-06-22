from time import sleep
from datetime import datetime, timedelta
from airflow import DAG
from pendulum.tz.timezone import Timezone
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args={
    'owner':'eddie',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='ex_parallel_tasks',
    description='병렬처리 예제 입니다.',
    default_args=default_args,
    start_date=datetime(2022,6,21, tzinfo=Timezone("Asia/Seoul")),
    schedule_interval="@daily",
    tags=['test', 'parallel tasks']
) as dag:
    def dump() -> None:
        print('hello world')

    # 방법 1
    start = BashOperator(
        task_id='start', 
        bash_command="date"
    )

    # task 동적 할당
    tasks = []
    for i in range(4):
        temp_task = PythonOperator(task_id=f"task_{i}", python_callable=dump)
        tasks.append(temp_task)

    end = PythonOperator(
        task_id="end", 
        python_callable=dump
    )

    # 의존성 리스트 할당 방법 1
    start >> tasks >> end


    '''
    # 방법 2
    start = PythonOperator(task_id='start', python_callable=dump)

    task_1 = PythonOperator(task_id='task_1', python_callable=dump)
    task_2 = PythonOperator(task_id='task_1', python_callable=dump)
    task_3 = PythonOperator(task_id='task_1', python_callable=dump)
    task_4 = PythonOperator(task_id='task_1', python_callable=dump)
    task_5 = PythonOperator(task_id='task_1', python_callable=dump)

    end = PythonOperator(task_id="end", python_callable=dump)

    # 의존성 리스트 할당 방법 2
    start >> [task_1, task_2, task_3, task_4, task_5] >> end
    '''

    # 방법 1 = 방법 2