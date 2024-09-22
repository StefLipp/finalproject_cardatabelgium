{{ config(
    materialized='table'
) }}

WITH car AS (SELECT * FROM {{ref('fct_cardata')}}),

city AS (SELECT * FROM {{ref('dim_city')}}),

car_grouped AS (
SELECT 
city_id,
(SELECT MAX(cardata_year) FROM car) as year_of_cardata,
sum(total_households) as total_households,
sum(total_households_with_car) as total_households_with_car,
ROUND(sum(total_households_with_car)/sum(total_households)*100,2) as percentage_car_having_households_of_total_households
FROM car
WHERE cardata_year = (SELECT MAX(cardata_year) FROM car)
GROUP BY city_id )

SELECT
c.city_name_nl as city_name_in_dutch,
c.city_id,
h.year_of_cardata,
h.total_households,
h.total_households_with_car,
h.percentage_car_having_households_of_total_households,
c.province_or_region,
c.count_boroughs as count_of_submunicipalities,
c.pop_in_2000 as population_in_2000,
c.pop_in_2024 as population_in_2024,
c.area_km2 as area_in_km2,
c.pop_per_km2 as population_density_per_km2,
c.prosperity_index,
c.pop_increase_2024v2000_percent as population_increase_compared_to_2000
FROM
city c
LEFT JOIN car_grouped h
ON c.city_id = h.city_id
ORDER BY c.pop_per_km2 DESC

