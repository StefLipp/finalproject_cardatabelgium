
{{ config(
    materialized='table'
) }}

WITH stg_city AS (SELECT * FROM {{ref('stg_carprojectbelgium__spark_muncipalitydata')}}),
stg_car AS (SELECT * FROM {{ref('stg_carprojectbelgium__spark_cardata2017-2023')}}),
city_names AS (
SELECT DISTINCT
  stg_city.Gemeente,
  stg_car.TX_MUNTY_DESCR_DE,
  stg_car.TX_MUNTY_DESCR_FR,
  stg_car.TX_MUNTY_DESCR_NL,
  stg_car.TX_MUNTY_DESCR_EN
FROM 
  stg_city
LEFT JOIN 
  stg_car
ON 
  stg_city.Gemeente = stg_car.TX_MUNTY_DESCR_DE
  OR stg_city.Gemeente = stg_car.TX_MUNTY_DESCR_FR
  OR stg_city.Gemeente = stg_car.TX_MUNTY_DESCR_NL )

SELECT
ROW_NUMBER() OVER(ORDER BY c.province_or_region, c.Gemeente) AS city_id,
c.Gemeente as city_name_nl,
n.TX_MUNTY_DESCR_EN as city_name_en,
n.TX_MUNTY_DESCR_FR as city_name_fr,
n.TX_MUNTY_DESCR_DE as city_name_de,
c.province_or_region,
c.count_boroughs,
c.year_2000 as pop_in_2000,
c.year_2024 as pop_in_2024,
c.area_km2,
c.pop_per_km2,
c.welv_index as prosperity_index,
c.pop_perc_increase_2024v2000 as pop_increase_2024v2000_percent
FROM
stg_city c
LEFT JOIN city_names n
ON c.Gemeente = n.Gemeente