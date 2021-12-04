{{ config(materialized='table') }}
select C.Province,
sum(TOTAL_DEATHS) as TOTAL_DEATHS,
sum(TOTAL_NEW_CASES) as TOTAL_NEW_CASES,
sum(TOTAL_RECOVERED_CASES) as TOTAL_RECOVERED_CASES,
sum(TOTAL_DEATHS)*100/max(p.population) as Mortality_Rate, 
CASE WHEN sum(TOTAL_NEW_CASES) = 0 then 0 else sum(TOTAL_DEATHS)*100/sum(TOTAL_NEW_CASES) end as Fatality_Rate,
sum(TOTAL_NEW_CASES)/max(p.population) as CASES_PER_CAPITA,
CASE WHEN sum(TOTAL_NEW_CASES) = 0 then 0 else sum(TOTAL_RECOVERED_CASES)/sum(TOTAL_NEW_CASES) end as Recovery_Rate  
from {{ ref('CASE_ANALYTICS_BY_MONTH_PROVINCE') }} C, {{ ref('POPULATION') }} P
where c.province = p.province
group by c.province