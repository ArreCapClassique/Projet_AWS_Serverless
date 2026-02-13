-- ----------------------------------------------------------------------------
-- SCÉNARIO 1 : Qualité des données et aperçu des échantillons (Couche SILVER)
-- OBJECTIF : Vérifier l'intégrité de l'ingestion et de la transformation.
-- UTILISATION : On consulte SILVER car on a besoin des relevés les plus récents.
-- ----------------------------------------------------------------------------
SELECT 
    station_id,
    timestamp_utc,
    temperature_c,
    humidity,
    wind_speed
FROM "meteo_db"."silver" 
ORDER BY timestamp_utc DESC 
LIMIT 10;


-- ----------------------------------------------------------------------------
-- SCÉNARIO 2 : Surveillance des températures extrêmes (Couche SILVER)
-- OBJECTIF : Identifier les pics de chaleur précis (ex: > 30°C).
-- UTILISATION : SILVER est privilégié pour connaître l'heure exacte du pic.
-- ----------------------------------------------------------------------------
SELECT 
    timestamp_utc,
    temperature_c,
    wind_speed
FROM "meteo_db"."silver"
WHERE CAST(temperature_c AS DOUBLE) > 30.0 
ORDER BY CAST(temperature_c AS DOUBLE) DESC;


-- ----------------------------------------------------------------------------
-- SCÉNARIO 3 : Agrégation des indicateurs quotidiens (Couche GOLD)
-- OBJECTIF : Analyse des tendances à long terme (moyennes, max, min par jour).
-- UTILISATION : On consulte GOLD (Parquet). C'est beaucoup plus rapide et moins 
-- coûteux que de scanner tous les fichiers CSV de la couche Silver.
-- ----------------------------------------------------------------------------
SELECT 
    SUBSTR(timestamp_utc, 1, 10) AS date_observation, 
    ROUND(AVG(CAST(temperature_c AS DOUBLE)), 2) AS temp_moyenne,
    MAX(CAST(temperature_c AS DOUBLE)) AS temp_max,
    MIN(CAST(temperature_c AS DOUBLE)) AS temp_min,
    ROUND(AVG(CAST(humidity AS DOUBLE)), 1) AS humidite_moyenne
FROM "meteo_db"."gold"
GROUP BY SUBSTR(timestamp_utc, 1, 10)
ORDER BY date_observation DESC;


-- ----------------------------------------------------------------------------
-- SCÉNARIO 4 : Évaluation du potentiel éolien (Couche GOLD)
-- OBJECTIF : Catégoriser la force du vent sur de grands volumes de données.
-- UTILISATION : GOLD est idéal ici pour les statistiques globales de performance.
-- ----------------------------------------------------------------------------
SELECT 
    CASE 
        WHEN CAST(wind_speed AS DOUBLE) < 2 THEN 'Calme (<2m/s)'
        WHEN CAST(wind_speed AS DOUBLE) BETWEEN 2 AND 5 THEN 'Brise (2-5m/s)'
        ELSE 'Vent Fort (>5m/s)'
    END AS categorie_vent,
    COUNT(*) AS nombre_total_releves,
    ROUND(AVG(CAST(temperature_c AS DOUBLE)), 2) AS temp_moyenne_par_condition
FROM "meteo_db"."gold"
GROUP BY 
    CASE 
        WHEN CAST(wind_speed AS DOUBLE) < 2 THEN 'Calme (<2m/s)'
        WHEN CAST(wind_speed AS DOUBLE) BETWEEN 2 AND 5 THEN 'Brise (2-5m/s)'
        ELSE 'Vent Fort (>5m/s)'
    END
ORDER BY nombre_total_releves DESC;


-- ----------------------------------------------------------------------------
-- SCÉNARIO 5 : Détection de variation thermique brutale (Couche SILVER)
-- OBJECTIF : Calculer l'écart de température entre deux relevés consécutifs.
-- UTILISATION : SILVER est impératif car GOLD (agrégé) ferait perdre la 
-- granularité nécessaire pour comparer deux points temporels proches.
-- ----------------------------------------------------------------------------
SELECT 
    timestamp_utc,
    temperature_c,
    LAG(CAST(temperature_c AS DOUBLE), 1) OVER (ORDER BY timestamp_utc) AS temp_precedente,
    ROUND(
        CAST(temperature_c AS DOUBLE) - LAG(CAST(temperature_c AS DOUBLE), 1) OVER (ORDER BY timestamp_utc), 
        2
    ) AS delta_thermique
FROM "meteo_db"."silver"
ORDER BY timestamp_utc DESC;