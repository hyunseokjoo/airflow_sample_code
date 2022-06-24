"""
TriggerDagRunOperator 공식 문서
https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/operators/trigger_dagrun/index.html#airflow.operators.trigger_dagrun.TriggerDagRunOperator
"""
from time import sleep
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from pendulum.tz.timezone import Timezone

kst=Timezone('Asia/Seoul')

with DAG(
    dag_id='ex_trigger_sample',
    description='trigger를 실행하게 해주는 DAG입니다.',
    schedule_interval='@once',
    start_date=datetime(2022,6,22, tzinfo=kst),
    tags=['test', 'call_trigger_sample']
) as dag:

    def dump() -> None:
        sleep(3)

    start = PythonOperator(
        task_id='start',
        python_callable=dump
    )

    call_trigger = TriggerDagRunOperator(
        task_id='call_trigger',
        trigger_dag_id='standby_trigger',
        trigger_run_id=None,
        execution_date=None,
        reset_dag_run=False,
        wait_for_completion=False,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=None,
    )

    # trigger rule을 사용하지 않으면 바로 end task가 실행된다
    # 바로 실행 되지 않고 trigger를 기다리고 싶다면 trigger rule을 사용
    # email보내는 것과 같이 그냥 보내고 다른 task를 진행하고 싶다면 trigger rule을 사용하지 않고 진행
    end = PythonOperator(
        task_id='end',
        python_callable=dump
    )

    start >> call_trigger >> end