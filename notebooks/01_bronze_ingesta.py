# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze: ingesta cruda de OpenSky
# MAGIC Llama a `/states/all` (posición actual de cada avión) y guarda la respuesta **tal cual** como JSON
# MAGIC en un Volume de Unity Catalog, particionado por fecha. Pensado para correr como Job cada 10–15 min.
# MAGIC
# MAGIC Credenciales: creá un API client en https://opensky-network.org/my-opensky/account y guardalas en un secret scope:
# MAGIC ```
# MAGIC databricks secrets create-scope opensky
# MAGIC databricks secrets put-secret opensky client_id
# MAGIC databricks secrets put-secret opensky client_secret
# MAGIC ```

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "vuelos")
# Bounding box por defecto: Argentina. Dejar vacío para el mundo entero (cuesta más créditos).
dbutils.widgets.text("bbox", "-55.5,-73.6,-21.7,-53.6")  # lamin,lomin,lamax,lomax

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
BBOX = dbutils.widgets.get("bbox").strip()

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw")
RAW_DIR = f"/Volumes/{CATALOG}/{SCHEMA}/raw/states"

# COMMAND ----------

import json, os, time
from datetime import datetime, timezone
import requests

TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
API_URL = "https://opensky-network.org/api/states/all"


def get_token() -> str | None:
    """OAuth2 client credentials. Si no hay secretos, devuelve None (modo anónimo, menos créditos)."""
    try:
        cid = dbutils.secrets.get("opensky", "client_id")
        csec = dbutils.secrets.get("opensky", "client_secret")
    except Exception:
        print("Sin secretos configurados: usando acceso anónimo.")
        return None
    r = requests.post(
        TOKEN_URL,
        data={"grant_type": "client_credentials", "client_id": cid, "client_secret": csec},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def fetch_states(token: str | None) -> dict:
    params = {"extended": 1}
    if BBOX:
        lamin, lomin, lamax, lomax = [float(x) for x in BBOX.split(",")]
        params |= {"lamin": lamin, "lomin": lomin, "lamax": lamax, "lomax": lomax}
    headers = {"Authorization": f"Bearer {token}"} if token else {}

    for intento in range(3):
        r = requests.get(API_URL, params=params, headers=headers, timeout=60)
        if r.status_code == 429:  # sin créditos o rate limit
            espera = int(r.headers.get("X-Rate-Limit-Retry-After-Seconds", 60))
            print(f"429 rate limit, reintento en {espera}s")
            time.sleep(min(espera, 120))
            continue
        r.raise_for_status()
        print("Créditos restantes:", r.headers.get("X-Rate-Limit-Remaining"))
        return r.json()
    raise RuntimeError("OpenSky no respondió tras 3 intentos")

# COMMAND ----------

payload = fetch_states(get_token())
ingested_at = datetime.now(timezone.utc)

record = {
    "ingested_at": ingested_at.isoformat(),
    "bbox": BBOX or None,
    "api_time": payload.get("time"),
    "states": payload.get("states") or [],
}

out_dir = f"{RAW_DIR}/fecha={ingested_at:%Y-%m-%d}"
os.makedirs(out_dir, exist_ok=True)
out_path = f"{out_dir}/states_{ingested_at:%Y%m%dT%H%M%SZ}.json"
with open(out_path, "w") as f:
    json.dump(record, f)

print(f"{len(record['states'])} aviones -> {out_path}")
