"""Plan B: ingesta fuera de Databricks (por ej. GitHub Actions).

Se usa si Databricks Free Edition bloquea la salida a opensky-network.org.
Baja los state vectors y sube el JSON al Volume con la Files API de Databricks,
con la misma estructura que el notebook 01, así que 02 y 03 no cambian.

Variables de entorno:
  OPENSKY_CLIENT_ID, OPENSKY_CLIENT_SECRET   (opcionales: sin ellas va anónimo)
  DATABRICKS_HOST   ej. https://dbc-xxxx.cloud.databricks.com
  DATABRICKS_TOKEN  personal access token
  VOLUME_PATH       ej. /Volumes/workspace/vuelos/raw/states
  BBOX              ej. -55.5,-73.6,-21.7,-53.6  (vacío = mundo)
"""
import json
import os
from datetime import datetime, timezone

import requests

TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
API_URL = "https://opensky-network.org/api/states/all"


def opensky_token():
    cid, csec = os.getenv("OPENSKY_CLIENT_ID"), os.getenv("OPENSKY_CLIENT_SECRET")
    if not (cid and csec):
        return None
    r = requests.post(TOKEN_URL, data={"grant_type": "client_credentials",
                                       "client_id": cid, "client_secret": csec}, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]


def main():
    bbox = os.getenv("BBOX", "").strip()
    params = {"extended": 1}
    if bbox:
        lamin, lomin, lamax, lomax = map(float, bbox.split(","))
        params |= {"lamin": lamin, "lomin": lomin, "lamax": lamax, "lomax": lomax}
    token = opensky_token()
    headers = {"Authorization": f"Bearer {token}"} if token else {}

    r = requests.get(API_URL, params=params, headers=headers, timeout=60)
    r.raise_for_status()
    payload = r.json()

    now = datetime.now(timezone.utc)
    record = {"ingested_at": now.isoformat(), "bbox": bbox or None,
              "api_time": payload.get("time"), "states": payload.get("states") or []}

    path = f"{os.environ['VOLUME_PATH']}/fecha={now:%Y-%m-%d}/states_{now:%Y%m%dT%H%M%SZ}.json"
    up = requests.put(
        f"{os.environ['DATABRICKS_HOST'].rstrip('/')}/api/2.0/fs/files{path}",
        headers={"Authorization": f"Bearer {os.environ['DATABRICKS_TOKEN']}"},
        params={"overwrite": "true"},
        data=json.dumps(record).encode(),
        timeout=60,
    )
    up.raise_for_status()
    print(f"{len(record['states'])} aviones -> {path}")


if __name__ == "__main__":
    main()
