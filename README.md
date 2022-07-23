# airflow 사용법 정리 

### [airflow란?](https://magpienote.tistory.com/192)
airflow 설치하기(conda 환경하에서 진행)
```bash
pip install apache-airflow
which airflow
```

airflow 기본 적인 내용 확인
```bash
airflow 

airflow config list
```

airflow DB 초기화
```bash
airflow db init
```

airflow 유저 계정 생성
```bash
airflow users create \ 
--username {Login_ID} \
--firstname {First_NAME} \ 
--lastname {Last_NAME} \
--role Admin \              # 해당 부분은 고정
--password {Password} \
--email {Email}
```

Webserver시작하기
```bash
airflow webserver --port 9111

airflow webserver
```

scheduler 실행
```bash
airflow scheduler
```

airflow-spark 연동하기 
```bash 
pip install apache-airflow-providers-apache-spark
```





- [provider 공식문서](https://airflow.apache.org/docs/#providers-packages-docs-apache-airflow-providers-index-html)