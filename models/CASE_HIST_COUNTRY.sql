{{ config(materialized='table') }}
select to_date(DATE, 'mm/dd/yyyy') as DATE,
to_char(to_date(DATE, 'mm/dd/yyyy'), 'mon-yy') as Month,
to_date('01' || to_char(to_date(DATE, 'mm/dd/yyyy'), '-mm-yy'), 'dd-mm-yy') as Month_START
,TOTAL_CASES
,NEW_CASES
,NEW_ACTIVE_CASES
,TOTAL_RECOVERED
,NEW_RECOVERED
,CASE_RECOVERED_RATE
,TOTAL_DEATHS
,NEW_DEATHS
,GROWTH_FACTOR_OF_NEW_CASES
,GROWTH_FACTOR_OF_NEW_DEATHS
,CASE_FATALITY_RATE
,TOTAL_CASES_PER_MILLION
,TOTAL_ACTIVE_CASES
,NEW_CASES_PER_MILLION
,NEW_DEATHS_PER_MILLION
,TOTAL_DEATHS_PER_MILLION
,ifNULL(province, 'Indonesia') as Province
, CASE WHEN to_date(DATE, 'mm/dd/yyyy') = 
    (select max(to_date(DATE, 'mm/dd/yyyy')) from {{ ref('COVID19_INDONESIA_RAW') }} where location_level = 'Country') 
    then 'Y' ELSE 'N' END as CURRENT_FLAG
from  {{ ref('COVID19_INDONESIA_RAW') }}
where location_level = 'Country'
order by 1