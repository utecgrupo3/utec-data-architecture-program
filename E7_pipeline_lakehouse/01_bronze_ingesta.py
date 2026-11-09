# Databricks notebook source
# MAGIC %md
# MAGIC # E7.1 · Ingesta a Bronze — Grupo 03 (Data Evolution)
# MAGIC **Cierra con:** M6 Big Data (batch) + M7 Integración (incremental/streaming)
# MAGIC
# MAGIC Principio de la capa bronze: **fidelidad al origen** — sin limpiar, sin filtrar,
# MAGIC pero SÍ con metadata de auditoría (`_source_file`, `_ingested_at`).
# MAGIC
# MAGIC Dos modos:
# MAGIC - **Batch full** (4 entidades maestras y de volumen moderado): lectura directa desde el Volume `/Volumes/workspace/yelp_bronze/raw/`.
# MAGIC - **Incremental con Auto Loader (`cloudFiles`)** (para `review`, la entidad transaccional de mayor volumen):
# MAGIC   demuestra ingesta idempotente *exactly-once* — si se sube un archivo nuevo al Volume, solo procesa el archivo nuevo.
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la **Sección 3.2 y 3.3 («Arquitectura de Referencia ABB / SBB — Capa Bronze»)** del
# MAGIC > **Documento Formal de Arquitectura de Datos (v1.3 · Equipo Data Evolution)**.

# COMMAND ----------

# DBTITLE 1,Configuración de rutas y función de observabilidad (bitácora FinOps E7.4)
import time
from pyspark.sql import functions as F

RAW = "/Volumes/workspace/yelp_bronze/raw"
ENTIDADES = ["business", "user", "tip", "checkin"]  # review se procesa con Auto Loader abajo

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.yelp_bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.yelp_gov")
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.yelp_gov.pipeline_run_log (
  executed_at      TIMESTAMP COMMENT 'Momento del registro',
  layer            STRING    COMMENT 'Capa medallón (bronze, silver, gold)',
  table_name       STRING    COMMENT 'Tabla procesada',
  row_count        BIGINT    COMMENT 'Filas resultantes en la tabla',
  duration_sec     DOUBLE    COMMENT 'Duración en segundos de la etapa'
) COMMENT 'Observabilidad del pipeline: volumetría y duración por corrida'
""")

def registrar_bitacora(capa: str, tabla: str, filas: int, t0: float):
    dur = round(time.time() - t0, 2)
    spark.sql(f"""
        INSERT INTO workspace.yelp_gov.pipeline_run_log
        VALUES (current_timestamp(), '{capa}', '{tabla}', {int(filas)}, {dur})
    """)
    print(f"✔ [{capa.upper()}] {tabla}: {filas:,} filas ({dur} s)")

# COMMAND ----------

# DBTITLE 1,Modo batch — 4 entidades (business, user, tip, checkin)
for e in ENTIDADES:
    t0 = time.time()
    tabla_destino = f"workspace.yelp_bronze.brz_{e}"
    (spark.read.json(f"{RAW}/{e}.json*")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
        .write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(tabla_destino))
    n_filas = spark.table(tabla_destino).count()
    registrar_bitacora("bronze", tabla_destino, n_filas, t0)

# COMMAND ----------

# DBTITLE 1,Modo incremental — Auto Loader (cloudFiles) para review (la entidad de mayor volumen)
t0 = time.time()
CHECKPOINT = "/Volumes/workspace/yelp_bronze/raw/_checkpoints/review_v2"  # v2: columnas de auditoría en inglés (_source_file, _ingested_at)
TABLA_REVIEW = "workspace.yelp_bronze.brz_review"

query = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", CHECKPOINT + "/schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(f"{RAW}/review*.json*")
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_ingested_at", F.current_timestamp())
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

# DBTITLE 1,Documentación de Metadatos en Unity Catalog para la Capa Bronze (Tablas y Columnas)
COMENTARIOS_TABLAS_BRONZE = {
    "brz_business": "Capa Bronze (fidelidad al origen): catálogo crudo de negocios locales de YelpConnect, incluyendo estructuras anidadas attributes y hours retenidas bajo Schema-on-Read (ADR-07).",
    "brz_user": "Capa Bronze (fidelidad al origen): perfiles crudos de usuarios registrados en YelpConnect, incluyendo lista de amigos concatenada y contadores de cumplidos.",
    "brz_tip": "Capa Bronze (fidelidad al origen): recomendaciones breves (tips) crudas emitidas por usuarios sobre establecimientos comerciales.",
    "brz_checkin": "Capa Bronze (fidelidad al origen): registros crudos de check-ins por negocio con marcas temporales concatenadas en texto.",
    "brz_review": "Capa Bronze (ingesta incremental Auto Loader / cloudFiles): flujo transaccional crudo de reseñas y calificaciones emitidas por usuarios.",
}

COMENTARIOS_COLUMNAS_BRONZE = {
    "brz_business": {
        "business_id": "Identificador natural único del negocio en el archivo JSON crudo (22 caracteres)",
        "name": "Nombre comercial tal como llega del sistema fuente",
        "address": "Dirección postal en texto libre del establecimiento",
        "city": "Ciudad del establecimiento sin normalizar",
        "state": "Código de estado o provincia sin normalizar",
        "postal_code": "Código postal o ZIP en formato texto",
        "latitude": "Latitud geográfica cruda del negocio",
        "longitude": "Longitud geográfica cruda del negocio",
        "stars": "Calificación promedio publicada en la ficha del negocio (incrementos de 0.5)",
        "review_count": "Conteo bruto acumulado de reseñas en el origen",
        "is_open": "Indicador crudo de apertura en formato entero (1 = abierto, 0 = cerrado)",
        "attributes": "Objeto STRUCT anidado con 39 subcampos opcionales de características del negocio retenido en Bronze bajo Schema-on-Read (ADR-07)",
        "categories": "Cadena de texto con las categorías del negocio separadas por comas (se normaliza en Silver en slv_category y la tabla puente slv_business_category)",
        "hours": "Objeto STRUCT anidado con los horarios de atención por día de la semana (Monday a Sunday) retenido en Bronze (ADR-07)",
    },
    "brz_user": {
        "user_id": "Identificador natural único del usuario en el origen",
        "name": "Nombre de pila del usuario en crudo (PII directa sujeta a enmascaramiento dinámico ADR-06 en capas superiores)",
        "review_count": "Conteo acumulado de reseñas escritas por el usuario",
        "yelping_since": "Fecha y hora de registro del usuario en formato texto crudo",
        "useful": "Total de votos useful enviados por el usuario",
        "funny": "Total de votos funny enviados por el usuario",
        "cool": "Total de votos cool enviados por el usuario",
        "elite": "Años en los que el usuario tuvo estatus Elite concatenados por comas",
        "friends": "Lista cruda de user_id de amigos concatenados por comas (origina slv_friendship en Silver)",
        "fans": "Cantidad de seguidores (fans) del usuario",
        "average_stars": "Promedio histórico de estrellas otorgadas por el usuario",
        "compliment_hot": "Cumplidos recibidos de tipo hot",
        "compliment_more": "Cumplidos recibidos de tipo more",
        "compliment_profile": "Cumplidos recibidos por su perfil",
        "compliment_cute": "Cumplidos recibidos de tipo cute",
        "compliment_list": "Cumplidos recibidos por listas creadas",
        "compliment_note": "Cumplidos recibidos por notas",
        "compliment_plain": "Cumplidos generales recibidos (plain)",
        "compliment_cool": "Cumplidos recibidos de tipo cool",
        "compliment_funny": "Cumplidos recibidos de tipo funny",
        "compliment_writer": "Cumplidos recibidos como escritor de reseñas",
        "compliment_photos": "Cumplidos recibidos por fotografías publicadas",
    },
    "brz_tip": {
        "user_id": "Identificador del usuario autor del tip",
        "business_id": "Identificador del negocio al que refiere el tip",
        "text": "Texto libre del consejo breve (tip)",
        "date": "Fecha y hora del tip en formato texto crudo",
        "compliment_count": "Cantidad de cumplidos recibidos por el tip",
    },
    "brz_checkin": {
        "business_id": "Identificador del negocio donde ocurrieron los check-ins",
        "date": "Cadena con todas las marcas temporales de check-in del negocio concatenadas por comas (se separa en una fila por fecha-hora en slv_checkin, con is_duplicate según DQ-010)",
    },
    "brz_review": {
        "review_id": "Identificador natural único de la reseña en el JSON crudo",
        "user_id": "Identificador del usuario autor de la reseña",
        "business_id": "Identificador del negocio evaluado en la reseña",
        "stars": "Calificación otorgada en crudo (escala 1 a 5)",
        "useful": "Votos de utilidad (useful) recibidos por la reseña",
        "funny": "Votos graciosos (funny) recibidos por la reseña",
        "cool": "Votos positivos (cool) recibidos por la reseña",
        "text": "Texto libre completo de la opinión del usuario",
        "date": "Marca temporal de publicación de la reseña en formato texto crudo",
    },
}

COMENTARIOS_AUDITORIA = {
    "_source_file": "Ruta física del archivo .json/.json.gz de origen en el Volume (/Volumes/workspace/yelp_bronze/raw/)",
    "_ingested_at": "Timestamp exacto de ingesta del registro hacia la capa Bronze (auditoría técnica)",
    "_rescued_data": "Columna de rescate generada por Auto Loader (cloudFiles) para capturar campos no conformes o evolución inesperada de tipos",
}

for tabla_corta, comentario_tabla in COMENTARIOS_TABLAS_BRONZE.items():
    tabla_fqn = f"workspace.yelp_bronze.{tabla_corta}"
    spark.sql(f"COMMENT ON TABLE {tabla_fqn} IS '{comentario_tabla}'")
    cols_reales = set(spark.table(tabla_fqn).columns)
    mapa_cols = {**COMENTARIOS_COLUMNAS_BRONZE.get(tabla_corta, {}), **COMENTARIOS_AUDITORIA}
    for col_name, col_comment in mapa_cols.items():
        if col_name in cols_reales:
            spark.sql(f"ALTER TABLE {tabla_fqn} ALTER COLUMN `{col_name}` COMMENT '{col_comment}'")
    print(f"✔ Comentarios aplicados en {tabla_fqn} ({len(cols_reales)} columnas)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. ¿Por qué Auto Loader (`cloudFiles`) garantiza Ingesta Idempotente (*Exactly-Once*)?
# MAGIC
# MAGIC La implementación de `brz_review` con **Databricks Auto Loader** resuelve el requerimiento de ingesta incremental sin duplicados gracias a tres mecanismos combinados:
# MAGIC 1. **Estado persistente en Checkpoint (`checkpointLocation`):** Auto Loader mantiene en `/Volumes/workspace/yelp_bronze/raw/_checkpoints/review_v2` un registro transaccional (RocksDB) con el identificador y ruta de cada archivo `.json.gz` ya procesado. Si el notebook se vuelve a ejecutar sin archivos nuevos, el stream detecta que todos los archivos ya figuran en el checkpoint y procesa **0 filas nuevas**.
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
# MAGIC - [x] 5 tablas bronze pobladas con columnas de auditoría (`_source_file`, `_ingested_at`) y registradas en `pipeline_run_log`.
# MAGIC - [x] Mecanismo de idempotencia de Auto Loader configurado y documentado.
# MAGIC - [x] Decisión batch vs streaming justificada por entidad en tabla comparativa.
