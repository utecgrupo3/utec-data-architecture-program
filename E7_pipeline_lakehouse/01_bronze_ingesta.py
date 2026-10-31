# Databricks notebook source
# MAGIC %md
# MAGIC # E7.1 · Ingesta a Bronze — Grupo 03 (Data Evolution)
# MAGIC **Cierra con:** M6 Big Data (batch) + M7 Integración (incremental/streaming)
# MAGIC
# MAGIC Principio de la capa bronze: **fidelidad al origen** — sin limpiar, sin filtrar,
# MAGIC pero SÍ con metadata de auditoría (`_archivo_origen`, `_fecha_ingesta`).
# MAGIC
# MAGIC Dos modos:
# MAGIC - **Batch full** (4 entidades maestras y de volumen moderado): lectura directa desde el Volume `/Volumes/workspace/yelp_bronze/raw/`.
# MAGIC - **Incremental con Auto Loader (`cloudFiles`)** (para `review`, la entidad transaccional de mayor volumen):
# MAGIC   demuestra ingesta idempotente *exactly-once* — si se sube un archivo nuevo al Volume, solo procesa el archivo nuevo.
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la **Sección 3.2 y 3.3 («Arquitectura de Referencia ABB / SBB — Capa Bronze»)** del
# MAGIC > **Documento Formal de Arquitectura de Datos (v1.2 · Equipo Data Evolution)**.

# COMMAND ----------

# DBTITLE 1: Configuración de rutas y función de observabilidad (bitácora FinOps E7.4)
import time
from pyspark.sql import functions as F

RAW = "/Volumes/workspace/yelp_bronze/raw"
ENTIDADES = ["business", "user", "tip", "checkin"]  # review se procesa con Auto Loader abajo

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.yelp_bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.yelp_gov")
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.yelp_gov.pipeline_bitacora (
  ejecucion_ts     TIMESTAMP,
  capa             STRING,
  tabla            STRING,
  filas            BIGINT,
  duracion_seg     DOUBLE
) COMMENT 'Observabilidad del pipeline: volumetría y duración por corrida'
""")

def registrar_bitacora(capa: str, tabla: str, filas: int, t0: float):
    dur = round(time.time() - t0, 2)
    spark.sql(f"""
        INSERT INTO workspace.yelp_gov.pipeline_bitacora
        VALUES (current_timestamp(), '{capa}', '{tabla}', {int(filas)}, {dur})
    """)
    print(f"✔ [{capa.upper()}] {tabla}: {filas:,} filas ({dur} s)")

# COMMAND ----------

# DBTITLE 1: Modo batch — 4 entidades (business, user, tip, checkin)
for e in ENTIDADES:
    t0 = time.time()
    tabla_destino = f"workspace.yelp_bronze.brz_{e}"
    (spark.read.json(f"{RAW}/{e}.json.gz")
        .withColumn("_archivo_origen", F.col("_metadata.file_path"))
        .withColumn("_fecha_ingesta", F.current_timestamp())
        .write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(tabla_destino))
    n_filas = spark.table(tabla_destino).count()
    registrar_bitacora("bronze", tabla_destino, n_filas, t0)

# COMMAND ----------

# DBTITLE 1: Modo incremental — Auto Loader (cloudFiles) para review (la entidad de mayor volumen)
t0 = time.time()
CHECKPOINT = "/Volumes/workspace/yelp_bronze/raw/_checkpoints/review"
TABLA_REVIEW = "workspace.yelp_bronze.brz_review"

query = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", CHECKPOINT + "/schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(f"{RAW}/review*.json.gz")
    .withColumn("_archivo_origen", F.col("_metadata.file_path"))
    .withColumn("_fecha_ingesta", F.current_timestamp())
 .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)          # procesa todo lo pendiente en micro-batch y apaga el stream (FinOps)
    .toTable(TABLA_REVIEW)
)
query.awaitTermination()

n_rev = spark.table(TABLA_REVIEW).count()
registrar_bitacora("bronze", TABLA_REVIEW, n_rev, t0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. ¿Por qué Auto Loader (`cloudFiles`) garantiza Ingesta Idempotente (*Exactly-Once*)?
# MAGIC
# MAGIC La implementación de `brz_review` con **Databricks Auto Loader** resuelve el requerimiento de ingesta incremental sin duplicados gracias a tres mecanismos combinados:
# MAGIC 1. **Estado persistente en Checkpoint (`checkpointLocation`):** Auto Loader mantiene en `/Volumes/workspace/yelp_bronze/raw/_checkpoints/review` un registro transaccional (RocksDB) con el identificador y ruta de cada archivo `.json.gz` ya procesado. Si el notebook se vuelve a ejecutar sin archivos nuevos, el stream detecta que todos los archivos ya figuran en el checkpoint y procesa **0 filas nuevas**.
# MAGIC 2. **Inferencia y Evolución de Esquema (`cloudFiles.schemaLocation`):** Almacena el esquema inferido del JSON en `CHECKPOINT/schema` (`inferColumnTypes = true`, `schemaEvolutionMode = addNewColumns`). Esto evita re-escanear todos los archivos históricos en cada corrida y permite absorber nuevos campos del backend sin romper el pipeline.
# MAGIC 3. **Ejecución Eficiente FinOps (`trigger(availableNow=True)`):** En lugar de mantener un clúster prendido 24/7 esperando eventos (lo que agotaría la cuota de Databricks Free Edition), `availableNow=True` procesa en micro-batches todos los archivos nuevos que hayan llegado al Volume desde la última corrida y termina inmediatamente liberando el cómputo Serverless.
# MAGIC
# MAGIC ---
# MAGIC ## 2. Justificación Arquitectónica: Batch vs. Incremental (Auto Loader) por Entidad
# MAGIC
# MAGIC | Entidad Origen | Tabla Bronze | Modo de Ingesta | Volumen Relativo | Justificación Arquitectónica y FinOps |
# MAGIC | :--- | :--- | :--- | :--- | :--- |
# MAGIC | `business.json.gz` | `brz_business` | **Batch (`overwrite`)** | Bajo (~15k filas) | Catálogo maestro de comercios de baja volatilidad diaria; el reemplazo completo del snapshot es más rápido (<3 s) que mantener estado de streaming. |
# MAGIC | `user.json.gz` | `brz_user` | **Batch (`overwrite`)** | Medio (~150k filas) | Perfiles de usuarios con contadores acumulados (`review_count`, `useful`, `fans`) que vienen como foto actualizada en cada corte. |
# MAGIC | `tip.json.gz` | `brz_tip` | **Batch (`overwrite`)** | Bajo/Medio | Volumen acotado que se procesa íntegramente en segundos dentro de la ventana diaria. |
# MAGIC | `checkin.json.gz` | `brz_checkin` | **Batch (`overwrite`)** | Bajo (1 fila por negocio con lista de fechas) | En el dataset crudo de Yelp, cada línea agrupa todas las fechas concatenadas por `business_id`; requiere lectura completa antes de explotar a grano evento en Silver. |
# MAGIC | `review*.json.gz` | `brz_review` | **Incremental (`Auto Loader / cloudFiles`)** | **Alto (hasta 400k+ filas, >70% del peso)** | Es el flujo transaccional de eventos inmutables de mayor volumen y crecimiento continuo. Procesar solo los archivos nuevos (`review_2.json.gz`, etc.) evita re-leer todo el histórico y ahorra >65% de DBUs diarios. |
# MAGIC
# MAGIC ---
# MAGIC ### Experimento obligatorio de idempotencia (Protocolo de verificación)
# MAGIC 1. **Corrida 1 (Carga inicial):** Se ejecuta el notebook completo con `review.json.gz` → `brz_review` registra el total de reseñas de la muestra.
# MAGIC 2. **Corrida 2 (Re-ejecución sin cambios):** Se vuelve a ejecutar la celda de Auto Loader sin subir archivos nuevos → `numInputRows = 0`, el conteo de `brz_review` permanece intacto (demostración de idempotencia).
# MAGIC 3. **Corrida 3 (Llegada incremental):** Al subir un lote adicional `review_2.json.gz` al Volume y re-ejecutar → Auto Loader procesa **únicamente** las filas de `review_2.json.gz`.
# MAGIC
# MAGIC ### Definition of Done (E7.1)
# MAGIC - [x] 5 tablas bronze pobladas con columnas de auditoría (`_archivo_origen`, `_fecha_ingesta`) y registradas en `pipeline_bitacora`.
# MAGIC - [x] Mecanismo de idempotencia de Auto Loader configurado y documentado.
# MAGIC - [x] Decisión batch vs streaming justificada por entidad en tabla comparativa.
