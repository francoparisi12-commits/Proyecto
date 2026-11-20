# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver: state vectors tipados
# MAGIC Lee los JSON nuevos del Volume con **Auto Loader** (incremental, con checkpoint), explota el array
# MAGIC `states` y lo convierte en columnas con nombre y tipo. Corre en serverless con `availableNow`.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "vuelos")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

RAW_DIR = f"/Volumes/{CATALOG}/{SCHEMA}/raw/states"
CHECKPOINT = f"/Volumes/{CATALOG}/{SCHEMA}/raw/_checkpoints/silver_states"
TABLE = f"{CATALOG}.{SCHEMA}.silver_states"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, LongType, ArrayType)

raw_schema = StructType([
    StructField("ingested_at", StringType()),
    StructField("bbox", StringType()),
    StructField("api_time", LongType()),
    # Cada state vector es un array posicional de tipos mezclados -> lo leemos como strings
    StructField("states", ArrayType(ArrayType(StringType()))),
])

# Orden de campos según la doc de OpenSky (/states/all, extended=1)
FIELDS = [
    ("icao24", "string"), ("callsign", "string"), ("origin_country", "string"),
    ("time_position", "long"), ("last_contact", "long"),
    ("longitude", "double"), ("latitude", "double"), ("baro_altitude_m", "double"),
    ("on_ground", "boolean"), ("velocity_ms", "double"), ("true_track_deg", "double"),
    ("vertical_rate_ms", "double"), ("sensors", "string"), ("geo_altitude_m", "double"),
    ("squawk", "string"), ("spi", "boolean"), ("position_source", "int"), ("category", "int"),
]

raw = (spark.readStream.format("cloudFiles")
       .option("cloudFiles.format", "json")
       .schema(raw_schema)
       .load(RAW_DIR)
       .withColumn("source_file", F.col("_metadata.file_path")))

exploded = raw.select(
    F.to_timestamp("ingested_at").alias("ingested_at"),
    F.to_timestamp(F.from_unixtime("api_time")).alias("snapshot_ts"),
    "source_file",
    F.explode("states").alias("s"),
)

silver = exploded.select(
    "ingested_at", "snapshot_ts", "source_file",
    *[F.col("s")[i].cast(t).alias(name) for i, (name, t) in enumerate(FIELDS)],
).withColumn("callsign", F.trim("callsign")) \
 .withColumn("fecha", F.to_date("snapshot_ts")) \
 .filter(F.col("icao24").isNotNull())

(silver.writeStream
   .option("checkpointLocation", CHECKPOINT)
   .partitionBy("fecha")
   .trigger(availableNow=True)
   .toTable(TABLE)
   .awaitTermination())

display(spark.sql(f"SELECT count(*) filas, max(snapshot_ts) ultimo_snapshot FROM {TABLE}"))
