'''
SubDag란?
Dag안에 또다른 Dag를 넣어 ui에서는 하나의 Task로 보여지게 하는 것임
SubDag를 사용하여 얻는 점 
1. 모듈처럼 관리가 용이해짐
2. 다른 곳에서도 불러와 사용할 수 있기 때문에 재사용성이 좋음
3. ui에서 하나의 task처럼 보이기 때문에 ui적으로 보기 좋음
'''
from time import sleep
from pendulum.tz.timezone import Timezone
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.subdag import SubDagOperator
import subDags.child_subdag1 as cs1
import subDags.child_subdag2 as cs2

kst=Timezone('Asia/Seoul')

default_args={
    'owner': 'eddie',
    'retries':1,
    'retry_delay':timedelta(minutes=1),
}

def dump() -> None:
    sleep(3)

dag_id = 'ex_subDAG'

with DAG(
    dag_id=dag_id,
    default_args=default_args,
    start_date=datetime(2022,6,22, tzinfo=kst),
    schedule_interval="@daily",
    tags=['test', 'subDAG']
) as dag:
    
    parent_task_start = PythonOperator(
        task_id='parent_task_start',
        python_callable=dump,
    )

    parent_subDAG_sample1 = SubDagOperator(
        task_id='parent_subDAG_sample1',
        subdag=cs1.child_subdag1(parent_dag_name=dag_id, child_dag_name="parent_subDAG_sample1")
    )

    parent_subDAG_sample2 = SubDagOperator(
        task_id='parent_subDAG_sample2',
        subdag=cs2.child_subdag2(parent_dag_name=dag_id, child_dag_name="parent_subDAG_sample2")
    )

    parent_task_end= PythonOperator(
        task_id='parent_task_end',
        python_callable=dump,
    )

    parent_task_start >> [parent_subDAG_sample1, parent_subDAG_sample2] >> parent_task_end