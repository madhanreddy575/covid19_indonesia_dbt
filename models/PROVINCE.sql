{{ config(materialized='table') }}
select distinct ifNULL(province, 'Indonesia') as Province, continent, Country, Island, latitude, longitude, location_iso_code, time_zone,
total_regencies, total_cities, total_districts, total_rural_villages, special_status, total_urban_villages, Location, AREA_KM_2_
from  {{ ref('COVID19_INDONESIA_RAW') }}