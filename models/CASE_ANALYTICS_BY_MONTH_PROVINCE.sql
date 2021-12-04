{{ config(materialized='table') }}
select C.Province, c.month,c.month_start,
sum(NEW_DEATHS) as TOTAL_DEATHS,
sum(NEW_CASES) as TOTAL_NEW_CASES,
sum(NEW_RECOVERED) as TOTAL_RECOVERED_CASES,
sum(NEW_DEATHS)*100/max(p.population) as Mortality_Rate, 
CASE WHEN sum(new_cases) = 0 then 0 else sum(NEW_DEATHS)*100/sum(NEW_CASES) end as Fatality_Rate,
sum(NEW_CASES)/max(p.population) as CASES_PER_CAPITA,
CASE WHEN sum(new_cases) = 0 then 0 else sum(NEW_RECOVERED)/sum(NEW_CASES) end as Recovery_Rate  
from {{ ref('CASE_HIST_PROVINCE') }} C, {{ ref('POPULATION') }} P
where c.province = p.province
group by c.province,c.month,c.month_start