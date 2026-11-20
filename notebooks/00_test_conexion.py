# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Test de conexión a OpenSky
# MAGIC Corré esto primero. En Databricks **Free Edition** la salida a internet está restringida
# MAGIC a una lista de dominios que no es pública, así que hay que confirmar que `opensky-network.org`
# MAGIC y `auth.opensky-network.org` responden desde tu workspace.

# COMMAND ----------

import requests

for url in [
    "https://auth.opensky-network.org/auth/realms/opensky-network/.well-known/openid-configuration",
    "https://opensky-network.org/api/states/all?lamin=-35&lomin=-59&lamax=-34&lomax=-58",  # anónimo, zona AMBA
]:
    try:
        r = requests.get(url, timeout=20)
        print("OK  ", r.status_code, url)
    except Exception as e:
        print("FAIL", type(e).__name__, url, "->", e)

# COMMAND ----------

# MAGIC %md
# MAGIC **Si ambos dan OK** → seguí con `01_bronze_ingesta`.
# MAGIC
# MAGIC **Si falla** → la ingesta se hace afuera (GitHub Actions, ver `ingesta_externa/`) y sube los JSON
# MAGIC al Volume de Databricks; los notebooks 02 y 03 funcionan igual.
