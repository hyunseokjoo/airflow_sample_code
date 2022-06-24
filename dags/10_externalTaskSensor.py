from time import sleep
from datetime import datetime, timedelta
from pendulum.tz.timezone import Timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor

kst = Timezone('Asia/Seoul')

default_args={
    'owner': 'eddie',
    'retries':1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='ex_externalSensor',
    description='외부 DAG의 Task를 기다리는 Trigger Sensor 예제 입니다.',
    default_args=default_args,
    schedule_interval='@once',
    start_date=datetime(2022,6,22, tzinfo=kst),
    tags=['test', 'external sample']
) as dag:
    
    def dump() -> None:
        sleep(3)

    external_task = ExternalTaskSensor(
        task_id='external_task',
        external_dag_id='ex_trigger_sample', # 바라볼 DAG를 지정
        external_task_id='call_trigger', # 바라본 DAG안에 Sensor를 실행 시킬 Task id 지정
        allowed_states=['success'], # 바라본 Task의 어떤 state를 바라볼 건지 
        failed_states=None, # 바라본 Task의 어떤 stae 를 지정 할 것인지 
        execution_delta=None, # Sensor를 실행한 결과의 시간값의 차이를 적용 오늘 한번 실행 됬따면 내일 될때까지 실행 x
        execution_date_fn=None, # Seonsor를 실행할 날짜를 정하는 함수를 지정  execution_delta, execution_date_fn은 둘 중 하나만 써야함
        check_existence=False, # true 로 두면 external task가 있는지 확인한다, 없으면 dag를 중단한다. default값은 false이다.
    )

    end = PythonOperator(
        task_id='end',
        python_callable=dump,
    )

    external_task >> end