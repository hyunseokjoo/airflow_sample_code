from datetime import datetime
from time import sleep
import json

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator

default_arg = {
    'owner': 'eddie',
    'start_date' : datetime(2022,6,19),
}

with DAG(
    dag_id='ex_connections',
    schedule_interval='@daily',
    default_args=default_arg,
    tags=['test', 'connection sample'],
    catchup=False
) as dag:

    def dump():
        sleep(3)

    start = PythonOperator(
        task_id='start',
        python_callable=dump
    )

    # 기본적으로 Http호출하는 Operator
    extract_gorest = SimpleHttpOperator(
        task_id='extract_gorest',
        http_conn_id='GoRest',
        endpoint='v2/users',
        method='GET',
        response_filter=lambda res: json.loads(res.text),
        log_response=True
    )

    end = PythonOperator(
        task_id='end',
        python_callable=dump
    )

    start >> extract_gorest >> end



