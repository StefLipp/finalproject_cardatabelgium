{{ config(
    materialized='table'
) }}

WITH car AS (SELECT * FROM {{ref('fct_cardata')}})

SELECT
(SELECT MAX(cardata_year) FROM car) as year_of_data,
household_type_en as household_type_in_english,
sum(total_households) as total_households,
sum(total_households_with_car) as total_households_with_car,
ROUND(sum(total_households_with_car)/sum(total_households)*100,2) as percentage_car_having_households_of_total_households
FROM car
WHERE cardata_year = (SELECT MAX(cardata_year) FROM car)
GROUP BY household_type_en