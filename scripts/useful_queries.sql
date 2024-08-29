select get_ddl('schema', 'coupon_platform');



select * from sql_eval.public.submissions; --5a35ce1c9a634a7d8d18f099f024b612

select * from sql_eval.public.results where 
1=1
--and submission_id in ('c998440f26ec444a9b6de59896005c78')
--and score != TRUE
and score is null
;

select * from sql_eval.public.submissions where submission_id not in ('a42a3c21caed4828aee1edd01a0749db');


-- delete from sql_eval.public.submissions where submission_id not in ('a42a3c21caed4828aee1edd01a0749db');
-- delete from sql_eval.public.results where submission_id not in ('a42a3c21caed4828aee1edd01a0749db');

-- delete from sql_eval.public.submissions;
-- delete from sql_eval.public.results;


select concat(LLM_NAME, ' - ', NAME) as submission ,score from submissions order by score asc;


SHOW GRANTS TO ROLE READ_ONLY_ROLE;



--SHOW PARAMETERS in USER READ_ONLY_BENCHMARK_USER;

--ALTER USER READ_ONLY_BENCHMARK_USER SET DEFAULT_WAREHOUSE='PROD_KLABS_DWH' 

-- desc user READ_ONLY_BENCHMARK_USER;

