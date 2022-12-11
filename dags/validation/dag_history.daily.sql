create table if not exists dag_history_daily(
    execution_date Date,
    dag_id varchar(250),
    dag_state varchar(250),
    runtime_seconds decimal(12,4),
    dag_run_count int
);

Truncate Table dag_history_daily;

INSERT INTO dag_history_daily
(execution_date, dag_id, dag_state, runtime_seconds, dag_run_count)
select 
    cast(execution_date as DATE),
    dag_id,
    state,
    sum(extract(epoch from (end_date - start_date))),
    count(*) as dag_run_count
from dag_run_history
GROUP BY 
  cast(execution_date as DATE),
  dag_id,
  state;

   
-- 성공률
select test_composite_name, 
    SUM(case when test_result = 'true' then 1 else 0 end
    / cast(sum(test_count) as decimal(6,2)))
    as success_rate
from validator_summary_daily
GROUP BY
  test_composite_name;
  