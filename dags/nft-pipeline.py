from datetime import datetime
import json

from airflow import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from pandas import json_normalize

default_arg = {
    'start_date' : datetime(2022,6,19),
}

with DAG(
    dag_id='nft-pipeline',
    schedule_interval='@daily',
    default_args=default_arg,
    tags=['nft'],
    catchup=False
) as dag:

    # nft api 호출 한 데이터 processing하기 
    def _processing_nft(ti):
        # xcom을 이용하여 다른 태스크에서 데이터를 가져와야 함
        # xcom은 cross communication이라는 뜻임
        # xcom push, pull로 전달 또는 받기 가능
        assets = ti.xcom_pull(task_id=['extract_nft'])
        if not len(assets):
            raise ValueError("assets is empty")
        nft = assets[0]['assets'][0]

        # pandas를 이용하여 json내용 가져오기
        processed_nft = json_normalize({
            'token_id': nft['token_id'],
            'name' : nft['name'],
            'image_url' : nft['image_url'],
        })
        # 가져온 내용 csv파일로 만들기
        processed_nft.to_csv('/tmp/processed_nft.csv', index=None, header=False)


    # SqliteOperator를 이용하여 task 작성
    creating_table = SqliteOperator( 
        # task id 정의
        task_id='creating_table', 
        # webserver UI Admin - Connections에서 Add record를하여 추가한 id를 작성
        sqlite_conn_id='db_sqlite',
        # Sqlite에 날릴 쿼리 정의
        sql='''
            CREATE TABLE IF NOT EXISTS nfts (
                token_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                image_url TEXT NOT NULL
            )
        '''
    )


    # Http가 호출 될때 까지 기다리는 Sensor
    is_api_avaiable = HttpSensor(
        # task id 정의
        task_id='is_api_available',
        # connection에 host http domain name id
        http_conn_id='opensea_api',
        # domain 뒤에 붙을 주소 정의
        endpoint='api/v1/assets?collection=doodles-official&limit=1'
    )

    # 기본적으로 Http호출하는 Operator
    extract_nft = SimpleHttpOperator(
        # task id 정의
        task_id='extract_nft',
        # connection에 host http domain name id
        http_conn_id='opensea_api',
        # domain 뒤에 붙을 주소 정의
        endpoint='api/v1/assets?collection=doodles-official&limit=1',
        # restful method 정의
        method='GET',
        # response 형태 어떻게 변형할지 정의 - 원래는 함수로 정의 함
        response_filter=lambda res: json.loads(res.text),
        # log_response debugging할 때 사용
        log_response=True
    )

    # python script를 사용하기 위한 operator
    process_nft = PythonOperator(
        # task id
        task_id='process_nft',
        # python_callable은 함수나 lambda로 정의 된 내용을 호출
        python_callable=_processing_nft,
    )

    # Bash script를 사용하기 위한 operator
    store_nft = BashOperator(
        # task id
        task_id='store_nft',
        # bash_command는 terminal에 명령어를 날리는 것과 동일하게 명령어를 정의 할 수 있다
        # 아래의 내용은 sqlite에 데이터를 저장하는 것이다.
        bash_command='echo -e ".separator ","\n.import /tmp/processed_nft.csv nfts" | sqlite3 /Users/planit/airflow/airflow.db'
    )

    creating_table >> is_api_avaiable >> extract_nft >> process_nft >> store_nft



