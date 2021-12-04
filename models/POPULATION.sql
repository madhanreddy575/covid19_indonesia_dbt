{{ config(materialized='table') }}
select distinct ifNULL(province, 'Indonesia') as Province,
population, population_density from {{ ref('COVID19_INDONESIA_RAW') }}