-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03 · Gold: métricas para dashboard
-- MAGIC Tablas agregadas sobre `silver_states`. Cambiá `workspace.vuelos` si usás otro catálogo/esquema.

-- COMMAND ----------

USE workspace.vuelos;

-- COMMAND ----------

-- Aviones en el aire por snapshot (serie temporal de tráfico)
CREATE OR REPLACE TABLE gold_trafico_por_snapshot AS
SELECT
  snapshot_ts,
  count(DISTINCT icao24)                                   AS aviones_total,
  count(DISTINCT CASE WHEN NOT on_ground THEN icao24 END)  AS aviones_en_vuelo,
  round(avg(CASE WHEN NOT on_ground THEN baro_altitude_m END)) AS altitud_media_m,
  round(avg(CASE WHEN NOT on_ground THEN velocity_ms * 3.6 END)) AS velocidad_media_kmh
FROM silver_states
GROUP BY snapshot_ts;

-- COMMAND ----------

-- Actividad diaria por país de matrícula del avión
CREATE OR REPLACE TABLE gold_actividad_pais_diaria AS
SELECT
  fecha,
  origin_country,
  count(DISTINCT icao24)   AS aviones_distintos,
  count(DISTINCT callsign) AS callsigns_distintos
FROM silver_states
GROUP BY fecha, origin_country;

-- COMMAND ----------

-- Aerolíneas más activas: los 3 primeros caracteres del callsign suelen ser el código OACI (ARG, LAN, FBZ...)
CREATE OR REPLACE TABLE gold_top_operadores AS
SELECT
  fecha,
  substr(callsign, 1, 3)   AS codigo_oaci,
  count(DISTINCT callsign) AS vuelos_distintos,
  count(DISTINCT icao24)   AS aviones_distintos
FROM silver_states
WHERE callsign RLIKE '^[A-Z]{3}[0-9]'
GROUP BY fecha, substr(callsign, 1, 3);

-- COMMAND ----------

SELECT * FROM gold_top_operadores
WHERE fecha = (SELECT max(fecha) FROM gold_top_operadores)
ORDER BY vuelos_distintos DESC
LIMIT 15;
