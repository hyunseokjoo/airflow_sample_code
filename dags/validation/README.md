# Airflow 데이터 검증 예시

Airflow에서 데이터파이프라인 관리 시 적재 후에 간단하게 데이터 검증하는 소스 입니다.
- validator_dag 는 airflow에서 사용하는 데이터 적재 시 검증 dag
- validator.py는 warehouse에 접속, 일정 쿼리로 비교하는 스크립트
- order_count.sql와 order_full_count.sql은 쿼리문 작성하는 곳