{{ config(materialized='table') }}
select MONTH, 
sum(NEW_DEATHS) as DEATHS,
sum(NEW_CASES) as NEW_CASES,
sum(NEW_RECOVERED) as RECOVERED_CASES,
sum(NEW_DEATHS)*100/max(p.population) as Mortality_Rate, 
sum(NEW_DEATHS)*100/sum(NEW_CASES) as Fatality_Rate,
sum(NEW_CASES)/max(p.population) as CASES_PER_CAPITA,
sum(NEW_RECOVERED)/sum(NEW_CASES) as Recovery_Rate 
from {{ ref('CASE_HIST_COUNTRY') }} a, {{ ref('POPULATION') }} P
where a.province = p.province
group by month