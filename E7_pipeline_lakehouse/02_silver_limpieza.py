# Databricks notebook source
# MAGIC %md
# MAGIC # E7.2 · Bronze → Silver: Limpieza, Conformación 3FN y Quality Gate (Grupo 03 · Data Evolution)
# MAGIC **Cierra con:** M6 Big Data
# MAGIC
# MAGIC Silver materializa el **modelo físico de E3 (`01_modelo_fisico_ddl.py`)**: mismos nombres, mismos tipos y mismas constraints.
# MAGIC Las transformaciones resuelven directamente los dolores de negocio del caso:
# MAGIC 1. **Deduplicación de reseñas y aislamiento en cuarentena (Marketing ↔ Trust & Safety — Acta 02):** deduplicación física (`review_id`) y lógica (`user_id + business_id + fecha/texto`), enviando registros huérfanos o inválidos a `slv_review_cuarentena` con `motivo_rechazo`.
# MAGIC 2. **Normalización y deduplicación horaria de check-ins (Operaciones Comerciales ↔ Producto — ADR-01):** explosión del string de fechas a grano evento individual y deduplicación por ventana de 60 minutos.
# MAGIC 3. **Categorías como `ARRAY<STRING>` (ADR-03)** y **Grafo Social `slv_amistad` (v1.2 Hackolade)**.
# MAGIC
# MAGIC > **Trazabilidad —** implementa la **Sección 4.2 («Modelo Lógico y Decisiones de Normalización»)** y la **Sección 6 («Reglas de Calidad como Quality Gate»)** del **Documento Formal de Arquitectura de Datos (v1.2)**.

# COMMAND ----------

# DBTITLE 1: Inicialización de esquemas y helper de bitácora FinOps
import time
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.yelp_silver")
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

# DBTITLE 1: 1. Entidad Silver — slv_business (Normalización + Categorías ARRAY<STRING> ADR-03)
t0 = time.time()
brz_bus = spark.table("workspace.yelp_bronze.brz_business")

slv_business = (brz_bus
    .select(
        F.trim(F.col("business_id")).alias("business_id"),
        F.trim(F.col("name")).alias("nombre"),
        F.initcap(F.trim(F.col("city"))).alias("ciudad"),
        F.upper(F.trim(F.col("state"))).alias("estado"),
        F.col("latitude").cast("double").alias("latitud"),
        F.col("longitude").cast("double").alias("longitud"),
        F.col("stars").cast("double").alias("estrellas"),
        F.coalesce(F.col("review_count").cast("int"), F.lit(0)).alias("n_resenas"),
        (F.col("is_open") == 1).alias("esta_abierto"),
        F.when(F.col("categories").isNotNull() & (F.trim(F.col("categories")) != ""),
               F.split(F.col("categories"), ",\\s*"))
         .otherwise(F.array(F.lit("Sin Categoría"))).alias("categorias"),
        F.current_timestamp().alias("fecha_carga"),
    )
    .filter(
        F.col("business_id").isNotNull() &
        F.col("nombre").isNotNull() & (F.col("nombre") != "") &
        F.col("ciudad").isNotNull() & (F.col("ciudad") != "") &
        F.col("latitud").between(-90.0, 90.0) &
        F.col("longitud").between(-180.0, 180.0) &
        (F.col("estrellas").isNull() | F.col("estrellas").between(1.0, 5.0))
    )
    .dropDuplicates(["business_id"])
)

(slv_business.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.yelp_silver.slv_business"))

registrar_bitacora("silver", "workspace.yelp_silver.slv_business",
                   spark.table("workspace.yelp_silver.slv_business").count(), t0)

# COMMAND ----------

# DBTITLE 1: 2. Entidades Silver — slv_user (Perfiles) y slv_amistad (Grafo Social v1.2)
t0 = time.time()
brz_usr = spark.table("workspace.yelp_bronze.brz_user")

# Compatibilidad con E6: si slv_user ya tiene COLUMN MASK aplicada sobre `name`, se retira temporalmente
# antes del overwriteSchema y se vuelve a aplicar al finalizar la conformación.
try:
    spark.sql("ALTER TABLE workspace.yelp_silver.slv_user ALTER COLUMN name DROP MASK")
except Exception:
    pass

slv_user = (brz_usr
    .select(
        F.trim(F.col("user_id")).alias("user_id"),
        F.coalesce(F.nullif(F.trim(F.col("name")), F.lit("")), F.lit("Usuario Anónimo")).alias("name"),
        F.coalesce(F.col("review_count").cast("int"), F.lit(0)).alias("review_count"),
        F.coalesce(F.to_timestamp(F.col("yelping_since")), F.to_timestamp(F.lit("2004-01-01 00:00:00"))).alias("yelping_since"),
        F.coalesce(F.col("useful").cast("int"), F.lit(0)).alias("useful"),
        F.coalesce(F.col("funny").cast("int"), F.lit(0)).alias("funny"),
        F.coalesce(F.col("cool").cast("int"), F.lit(0)).alias("cool"),
        F.coalesce(F.col("fans").cast("int"), F.lit(0)).alias("fans"),
        F.coalesce(F.col("elite").cast("string"), F.lit("")).alias("elite"),
        F.col("average_stars").cast("double").alias("average_stars"),
        F.current_timestamp().alias("fecha_carga"),
    )
    .filter(F.col("user_id").isNotNull() & (F.col("user_id") != ""))
    .dropDuplicates(["user_id"])
)

(slv_user.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.yelp_silver.slv_user"))

registrar_bitacora("silver", "workspace.yelp_silver.slv_user",
                   spark.table("workspace.yelp_silver.slv_user").count(), t0)

# --- Grafo Social: slv_amistad (user_id_1, user_id_2) ---
t0_am = time.time()
slv_amistad = (brz_usr
    .select(
        F.trim(F.col("user_id")).alias("user_id_1"),
        F.explode(F.split(F.col("friends"), ",\\s*")).alias("user_id_2")
    )
    .withColumn("user_id_2", F.trim(F.col("user_id_2")))
    .filter(
        F.col("user_id_1").isNotNull() &
        F.col("user_id_2").isNotNull() &
        (F.col("user_id_2") != "") &
        (F.lower(F.col("user_id_2")) != "none") &
        (F.col("user_id_1") != F.col("user_id_2"))
    )
    .dropDuplicates(["user_id_1", "user_id_2"])
    .withColumn("fecha_carga", F.current_timestamp())
)

(slv_amistad.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.yelp_silver.slv_amistad"))

registrar_bitacora("silver", "workspace.yelp_silver.slv_amistad",
                   spark.table("workspace.yelp_silver.slv_amistad").count(), t0_am)

# COMMAND ----------

# DBTITLE 1: 3. Entidades Silver — slv_review y slv_review_cuarentena (Deduplicación + Cuarentena Acta 02)
# Criterio documentado de deduplicación y cuarentena:
#  1. Duplicado físico: mismo review_id (se conserva el más reciente por _fecha_ingesta).
#  2. Duplicado lógico: mismo (user_id, business_id, md5(lower(trim(text)))) o mismo día.
#  3. Integridad referencial (FK): business_id debe existir en slv_business y user_id en slv_user.
#  4. Validez y completitud: stars BETWEEN 1 AND 5, texto no vacío, fecha_resena entre 2004 y hoy.
#  -> Las filas que incumplen cualquiera de estas reglas van a slv_review_cuarentena con motivo_rechazo.

t0 = time.time()
brz_rev = spark.table("workspace.yelp_bronze.brz_review")
bus_ids = spark.table("workspace.yelp_silver.slv_business").select("business_id")
usr_ids = spark.table("workspace.yelp_silver.slv_user").select("user_id")

rev_prep = (brz_rev
    .select(
        F.trim(F.col("review_id")).alias("review_id"),
        F.trim(F.col("user_id")).alias("user_id"),
        F.trim(F.col("business_id")).alias("business_id"),
        F.col("stars").cast("int").alias("stars"),
        F.coalesce(F.col("useful").cast("int"), F.lit(0)).alias("useful"),
        F.coalesce(F.col("funny").cast("int"), F.lit(0)).alias("funny"),
        F.coalesce(F.col("cool").cast("int"), F.lit(0)).alias("cool"),
        F.col("text").cast("string").alias("text"),
        F.to_timestamp(F.col("date")).alias("fecha_resena"),
        F.col("_fecha_ingesta")
    )
)

# Ventana 1: Duplicados físicos por review_id
w_id = Window.partitionBy("review_id").orderBy(F.col("_fecha_ingesta").desc_nulls_last())
# Ventana 2: Duplicados lógicos (mismo usuario + negocio + texto normalizado)
w_logico = Window.partitionBy(
    "user_id", "business_id", F.md5(F.lower(F.trim(F.coalesce(F.col("text"), F.lit("")))))
).orderBy(F.col("fecha_resena").asc_nulls_last(), F.col("_fecha_ingesta").desc_nulls_last())

rev_eval = (rev_prep
    .withColumn("rn_id", F.row_number().over(w_id))
    .withColumn("rn_logico", F.row_number().over(w_logico))
    .join(bus_ids.withColumnRenamed("business_id", "b_ok"), F.col("business_id") == F.col("b_ok"), "left")
    .join(usr_ids.withColumnRenamed("user_id", "u_ok"), F.col("user_id") == F.col("u_ok"), "left")
    .withColumn(
        "motivo_rechazo",
        F.when(F.col("review_id").isNull() | (F.col("rn_id") > 1), F.lit("DQ-002: DUPLICADO_FISICO_REVIEW_ID"))
         .when(F.col("rn_logico") > 1, F.lit("DQ-002: DUPLICADO_LOGICO_USER_BIZ_TEXTO"))
         .when(F.col("stars").isNull() | ~F.col("stars").between(1, 5), F.lit("DQ-001: STARS_FUERA_DE_RANGO_1_5"))
         .when(F.col("b_ok").isNull(), F.lit("DQ-003: FK_BUSINESS_HUERFANA"))
         .when(F.col("u_ok").isNull(), F.lit("DQ-004: FK_USER_HUERFANA"))
         .when(F.col("fecha_resena").isNull() | (F.col("fecha_resena") < F.lit("2004-01-01")) | (F.col("fecha_resena") > F.current_timestamp()),
               F.lit("DQ-007: FECHA_RESENA_INVALIDA"))
         .when(F.col("text").isNull() | (F.length(F.trim(F.col("text"))) == 0), F.lit("DQ-010: TEXTO_VACIO"))
         .otherwise(F.lit(None))
    )
)

# 3.1 Persistir reseñas en cuarentena (motivo_rechazo IS NOT NULL)
slv_review_cuarentena = (rev_eval
    .filter(F.col("motivo_rechazo").isNotNull())
    .select(
        "review_id", "user_id", "business_id", "stars", "text", "fecha_resena", "motivo_rechazo",
        F.current_timestamp().alias("fecha_rechazo")
    )
)
(slv_review_cuarentena.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.yelp_silver.slv_review_cuarentena"))

registrar_bitacora("silver", "workspace.yelp_silver.slv_review_cuarentena",
                   spark.table("workspace.yelp_silver.slv_review_cuarentena").count(), t0)

# 3.2 Persistir reseñas verificadas y limpias en slv_review (motivo_rechazo IS NULL)
t0_rev = time.time()
slv_review = (rev_eval
    .filter(F.col("motivo_rechazo").isNull())
    .select(
        "review_id", "user_id", "business_id", "stars",
        "useful", "funny", "cool", "text", "fecha_resena",
        F.current_timestamp().alias("fecha_carga")
    )
)
(slv_review.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.yelp_silver.slv_review"))

registrar_bitacora("silver", "workspace.yelp_silver.slv_review",
                   spark.table("workspace.yelp_silver.slv_review").count(), t0_rev)

# COMMAND ----------

# DBTITLE 1: 4. Entidades Silver — slv_tip y slv_checkin (Explosión a grano evento y deduplicación horaria ADR-01)
# --- 4.1 slv_tip ---
t0_tip = time.time()
brz_tip = spark.table("workspace.yelp_bronze.brz_tip")
bus_ids = spark.table("workspace.yelp_silver.slv_business").select("business_id")
usr_ids = spark.table("workspace.yelp_silver.slv_user").select("user_id")

slv_tip = (brz_tip
    .select(
        F.trim(F.col("user_id")).alias("user_id"),
        F.trim(F.col("business_id")).alias("business_id"),
        F.trim(F.col("text")).alias("text"),
        F.to_timestamp(F.col("date")).alias("fecha_tip"),
        F.coalesce(F.col("compliment_count").cast("int"), F.lit(0)).alias("compliment_count"),
    )
    .filter(
        F.col("user_id").isNotNull() &
        F.col("business_id").isNotNull() &
        F.col("text").isNotNull() & (F.length(F.col("text")) > 0) &
        F.col("fecha_tip").isNotNull()
    )
    .join(bus_ids, "business_id", "inner")
    .join(usr_ids, "user_id", "inner")
    .withColumn("tip_id", F.sha2(F.concat_ws("|", "user_id", "business_id", F.col("fecha_tip").cast("string"), "text"), 256))
    .dropDuplicates(["tip_id"])
    .select("tip_id", "user_id", "business_id", "text", "fecha_tip", "compliment_count",
            F.current_timestamp().alias("fecha_carga"))
)

(slv_tip.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.yelp_silver.slv_tip"))

registrar_bitacora("silver", "workspace.yelp_silver.slv_tip",
                   spark.table("workspace.yelp_silver.slv_tip").count(), t0_tip)

# --- 4.2 slv_checkin (Explosión de fechas separadas por coma + Deduplicación horaria ADR-01) ---
t0_chk = time.time()
brz_chk = spark.table("workspace.yelp_bronze.brz_checkin")

slv_checkin = (brz_chk
    .select(
        F.trim(F.col("business_id")).alias("business_id"),
        F.explode(F.split(F.col("date"), ",\\s*")).alias("fecha_str")
    )
    .withColumn("fecha_checkin", F.to_timestamp(F.trim(F.col("fecha_str"))))
    .filter(
        F.col("business_id").isNotNull() &
        F.col("fecha_checkin").isNotNull() &
        (F.col("fecha_checkin") >= F.lit("2004-01-01")) &
        (F.col("fecha_checkin") <= F.current_timestamp())
    )
    .join(bus_ids, "business_id", "inner")
    # ADR-01: Deduplicación por ventana horaria de 60 minutos por establecimiento
    .withColumn("ventana_hora", F.date_trunc("hour", F.col("fecha_checkin")))
    .dropDuplicates(["business_id", "ventana_hora"])
    .withColumn("checkin_id", F.sha2(F.concat_ws("|", F.col("business_id"), F.col("ventana_hora").cast("string")), 256))
    .select("checkin_id", "business_id", "fecha_checkin", F.current_timestamp().alias("fecha_carga"))
)

(slv_checkin.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.yelp_silver.slv_checkin"))

registrar_bitacora("silver", "workspace.yelp_silver.slv_checkin",
                   spark.table("workspace.yelp_silver.slv_checkin").count(), t0_chk)

# COMMAND ----------

# DBTITLE 1: 5. Reconciliación de Consistencia E5 (DQ-011 y DQ-012) y Preservación de Controles E6 (ADR-06)
# 5.1 DQ-011 (Sección 6.2): Ajustar yelping_since a la marca temporal de la primera reseña histórica cuando exista desfase horario
spark.sql("""
MERGE INTO workspace.yelp_silver.slv_user AS u
USING (
  SELECT user_id, min(fecha_resena) AS min_fecha_resena
  FROM workspace.yelp_silver.slv_review
  GROUP BY user_id
) AS r
ON u.user_id = r.user_id AND u.yelping_since > r.min_fecha_resena
WHEN MATCHED THEN UPDATE SET u.yelping_since = r.min_fecha_resena
""")

# 5.2 DQ-012 (Sección 6.2): Sincronizar el promedio de estrellas y conteo de reseñas de slv_business con las reseñas activas en slv_review
spark.sql("""
MERGE INTO workspace.yelp_silver.slv_business AS b
USING (
  SELECT business_id,
         round(avg(CAST(stars AS DOUBLE)), 2) AS avg_stars_activo,
         CAST(count(*) AS INT)                AS cnt_resenas_activo
  FROM workspace.yelp_silver.slv_review
  GROUP BY business_id
) AS r
ON b.business_id = r.business_id
WHEN MATCHED THEN UPDATE SET
  b.estrellas = r.avg_stars_activo,
  b.n_resenas = r.cnt_resenas_activo
""")

# 5.3 Re-aplicar Column Mask de E6 (ADR-06) sobre slv_user.name si la función gobernada ya existe
try:
    spark.sql("""
        ALTER TABLE workspace.yelp_silver.slv_user
        ALTER COLUMN name SET MASK workspace.yelp_gov.mascara_nombre
    """)
    print("✔ Máscara dinámica E6 (workspace.yelp_gov.mascara_nombre) preservada en slv_user.name")
except Exception:
    pass

# COMMAND ----------

# DBTITLE 1: 6. Gate de calidad (E5) — el pipeline falla si la calidad crítica no pasa
# Ejecuta el motor de E5 (01_reglas_calidad) y corta el pipeline si una regla de criticidad alta falla.
resultado = dbutils.notebook.run("../E5_calidad_datos/01_reglas_calidad", 600)

fallas_criticas = spark.sql("""
  SELECT count(*) c
  FROM workspace.yelp_gov.dq_resultados res
  JOIN workspace.yelp_gov.dq_reglas r USING (regla_id)
  WHERE r.criticidad = 'alta' AND NOT res.paso
    AND res.ejecucion_ts = (SELECT max(ejecucion_ts) FROM workspace.yelp_gov.dq_resultados)
""").first().c
assert fallas_criticas == 0, f"❌ {fallas_criticas} reglas críticas fallaron — pipeline detenido (así se ve la calidad como GATE, no como reporte)"
print(f"✔ Gate de calidad superado ({resultado}): 0 fallas críticas en Silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definition of Done (E7.2)
# MAGIC - [x] Todas las entidades silver (`slv_business`, `slv_user`, `slv_amistad`, `slv_review`, `slv_review_cuarentena`, `slv_tip`, `slv_checkin`) pobladas conforme al DDL de E3.
# MAGIC - [x] Deduplicación de reseñas (física por `review_id` y lógica por `user_id + business_id + texto`) con criterio documentado + tabla de cuarentena `slv_review_cuarentena` operativa.
# MAGIC - [x] Check-ins explotados a grano evento (`split` + `explode`) y deduplicados por ventana horaria (**ADR-01**).
# MAGIC - [x] Reconciliación de consistencia (`DQ-011` y `DQ-012`) y preservación de máscara PII (`E6`) aplicadas.
# MAGIC - [x] Gate de calidad integrado invocando `../E5_calidad_datos/01_reglas_calidad` y deteniendo el flujo ante fallas críticas.
