from datetime import datetime
from re import M
from airflow import DAG
from airflow.providers.apache.spark.oprators.spark_submit import SparkSubmitOperator

default_args = {
    'start_date' : datetime(2021,1,1)
}

with DAG(dag_id = 'spark-example',
    schedule_interval = "@daily",
    default_args = default_args,
    tags = ['spark'],
    catchup=False
) as dag:

    # airflow에서는 해비한 작업을 하지 않아야 하기 때문에 
    # SparkSubmitOperator를 실행하여 Spark job을 실행 시켜 주기만 하면 된다.
    # SparkSubmitOperator를 log만 남겨주는 것이지 job을 직접 실행시켜주는 것은 아니다.
    submit_job = SparkSubmitOperator(
        application = "yourScripts.py",
        task_id="submit_job",
        conn_id="yourCon" # connection에 cluster정보를 만들어 주어야 한다.
    )