# ✈️ OpenSky → Databricks (arquitectura medallion)

Pipeline que captura el tráfico aéreo sobre Argentina desde la API de [OpenSky Network](https://opensky-network.org)
y lo modela en Databricks con capas bronze / silver / gold sobre Delta Lake y Unity Catalog.

```
OpenSky /states/all ──► Volume (JSON crudo, bronze) ──Auto Loader──► silver_states (Delta) ──► gold_* ──► Dashboard
```

## Estructura

| Archivo | Qué hace |
|---|---|
| `notebooks/00_test_conexion.py` | Verifica que el workspace pueda salir a OpenSky |
| `notebooks/01_bronze_ingesta.py` | Llama a la API (OAuth2) y guarda el JSON crudo en un Volume, particionado por fecha |
| `notebooks/02_silver_states.py` | Auto Loader incremental → tabla Delta tipada `silver_states` |
| `notebooks/03_gold_metricas.sql` | Agregados: tráfico por snapshot, actividad por país y top operadores |
| `ingesta_externa/` + `.github/workflows/` | Plan B: ingesta desde GitHub Actions si Databricks no tiene salida a OpenSky |

## Setup

1. **OpenSky**: creá una cuenta y, en *Account → API Client*, generá un `client_id` y un `client_secret`.
2. **Databricks**: en *Workspace → Create → Git folder*, cloná este repo.
3. **Secretos** (con la Databricks CLI):
   ```bash
   databricks secrets create-scope opensky
   databricks secrets put-secret opensky client_id
   databricks secrets put-secret opensky client_secret
   ```
4. Corré `00_test_conexion`. Si da OK, seguí; si no, usá el Plan B.
5. **Job**: en *Jobs & Pipelines → Create job*, armá 3 tareas encadenadas (01 → 02 → 03)
   con un schedule cada 15 min.

## Presupuesto de créditos

Una cuenta registrada tiene 4.000 créditos por día. Una consulta acotada a un bounding box chico cuesta menos
que una consulta global, y la de Argentina es intermedia. Con 96 ejecuciones por día (cada 15 min) entrás
holgado. Mirá el header `X-Rate-Limit-Remaining` que imprime el notebook 01.

## Próximos pasos

- Dimensiones: aeropuertos (OurAirports) y aerolíneas (OpenFlights) para enriquecer el callsign.
- Endpoint `/flights/departure` de OpenSky para obtener vuelos por aeropuerto (por ejemplo, SAEZ o SABE).
- Data quality con *expectations* migrando 02 a Lakeflow Declarative Pipelines.
