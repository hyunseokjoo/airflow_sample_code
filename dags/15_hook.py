import csv
import logging
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator, MySqlHook


default_args = {
    'owner': 'hsjoo',
    'retries': 5,
    'retry_delay': timedelta(minutes=10)
}


def mysql_hook():
    # hook 내용 작성
    logging.info("Started mysql_hook")
    hook = MySqlHook.get_hook(conn_id="local-mysql") # 미리 정의한 mysql connection 적용
    conn = hook.get_conn() # connection 하기
    cursor = conn.cursor() # cursor객체 만들기 
    cursor.execute("use testDB") # sql문 수행
    cursor.execute("select * from user")
    # csv파일 만들기
    with open("dags/get_data_mysql_to_csv.txt", "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor)
        f.flush()
        cursor.close()
        conn.close()
        logging.info("Saved data in text file: %s", "dags/get_data_mysql_hook.txt")


with DAG(
    dag_id="ex_mysql_to_csv",
    default_args=default_args,
    start_date=datetime(2022, 4, 30),
    schedule_interval='@once'
) as dag:
    # mysql_hook 적용
    task1 = PythonOperator(
        task_id="mysql_to_csv",
        python_callable=mysql_hook
    )
    task1