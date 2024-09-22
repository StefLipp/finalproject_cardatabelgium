WITH carsource AS ( 
    SELECT * FROM {{ source('carprojectbelgium', 'spark_cardata2017-2023') }})

SELECT 
CD_YEAR, 
    TRIM(REGEXP_REPLACE(TX_MUNTY_DESCR_FR, r'\s*\(.*?\)', '')) AS TX_MUNTY_DESCR_FR,
    TRIM(REGEXP_REPLACE(TX_MUNTY_DESCR_NL, r'\s*\(.*?\)', '')) AS TX_MUNTY_DESCR_NL,
    TRIM(REGEXP_REPLACE(TX_MUNTY_DESCR_EN, r'\s*\(.*?\)', '')) AS TX_MUNTY_DESCR_EN,
    TRIM(REGEXP_REPLACE(TX_MUNTY_DESCR_DE, r'\s*\(.*?\)', '')) AS TX_MUNTY_DESCR_DE,
hh_type_EN,
total_huisH,
total_wagens,
car_status
FROM carsource