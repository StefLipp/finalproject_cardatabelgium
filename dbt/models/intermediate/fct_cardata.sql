{{ config(
    materialized='table'
) }}

WITH dim_city AS (SELECT * FROM {{ref('dim_city')}}),

stg_car AS (SELECT * FROM {{ref('stg_carprojectbelgium__spark_cardata2017-2023')}}),

pivot_car AS (
SELECT 
    CD_YEAR AS cardata_year, 
    TX_MUNTY_DESCR_FR, 
    hh_type_EN AS household_type_en, 
    SUM(CASE WHEN car_status = 'has_car' THEN total_huisH ELSE 0 END) AS total_households_with_car,
    SUM(total_huisH) AS total_households
FROM 
    stg_car
GROUP BY 
    CD_YEAR, 
    TX_MUNTY_DESCR_FR, 
    hh_type_EN
ORDER BY 
    CD_YEAR, 
    TX_MUNTY_DESCR_FR, 
    hh_type_EN
)

SELECT
ROW_NUMBER() OVER(ORDER BY h.cardata_year, c.city_id, h.household_type_en) AS household_id,
c.city_id,
h.household_type_en,
h.total_households,
h.total_households_with_car,
round(h.total_households_with_car/h.total_households*100,2) as household_hascar_perc_of_total,
h.cardata_year
FROM pivot_car h
LEFT JOIN dim_city c
ON c.city_name_FR = h.TX_MUNTY_DESCR_FR

