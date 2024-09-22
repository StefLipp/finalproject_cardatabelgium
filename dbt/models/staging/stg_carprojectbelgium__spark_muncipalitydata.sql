WITH citysource AS ( 
    SELECT * FROM {{ source('carprojectbelgium', 'spark_muncipalitydata') }})

SELECT
  CASE 
    WHEN Gemeente = "Court-Saint-Étienne" THEN "Court-Saint-Etienne"
    WHEN Gemeente = "Fontaine-l'Evêque" THEN "Fontaine-l’Evêque"
    WHEN Gemeente = "Ecaussines" THEN "Ecaussinnes"
    WHEN Gemeente = "Érezée" THEN "Erezée"
    WHEN Gemeente = "Mont-de-l'Enclus" THEN "Mont-de-l’Enclus"
    WHEN Gemeente = "Blieberg" THEN "Plombières"
    WHEN Gemeente = "Villers-le-Bouillet" THEN "Villers-Le-Bouillet"
    WHEN Gemeente = "Blégny" THEN "Blegny"
    WHEN Gemeente = "'s-Gravenbrakel" THEN "’s Gravenbrakel"
    WHEN Gemeente = "Steenput" THEN "Estaimpuis"
    WHEN Gemeente = "Belœil" THEN "Beloeil"
    WHEN Gemeente = "Gembloers" THEN "Gembloux"
    WHEN Gemeente = "Éghezée" THEN "Eghezée"
    ELSE Gemeente
  END AS Gemeente,
province_or_region,
count_boroughs,
year_2000,
year_2024,
ROUND(area_km2, 2) AS area_km2,
CAST(ROUND(pop_per_km2, 0) AS INT) AS pop_per_km2,
welv_index,
pop_perc_increase_2024v2000
FROM
citysource