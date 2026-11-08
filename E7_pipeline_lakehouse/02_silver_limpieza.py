# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # E7.2 · Bronze → Silver: Limpieza, Normalización y Quality Gate (Grupo 03 · Data Evolution)
# MAGIC **Cierra con:** M6 Big Data
# MAGIC
# MAGIC Silver materializa el **modelo lógico de `yelp_modelo_logico.drawio` / `yelp_silver.txt`**. Las tablas se crean una sola vez en E3 (`01_modelo_fisico_ddl`), que este notebook ejecuta al inicio; aquí solo se cargan datos, respetando nombres, tipos, `COMMENT` y constraints.
# MAGIC
# MAGIC Transformaciones principales:
# MAGIC 1. **Golden record de negocios (DQ-012):** `master_business_id` agrupa los registros del mismo negocio físico (nombre, dirección y código postal normalizados); sobrevive el que declara más reseñas.
# MAGIC 2. **Categorías normalizadas:** `business.categories` se separa en `slv_category` y la tabla puente `slv_business_category`.
# MAGIC 3. **Reseñas:** las que incumplen reglas bloqueantes van a `slv_review_quarantine`; las duplicadas lógicas (DQ-002) entran a `slv_review` y Gold las marca como no válidas.
# MAGIC 4. **Check-ins a grano evento (DQ-009, DQ-010):** una fila por fecha-hora, con `checkin_id` determinístico e `is_duplicate`.
# MAGIC 5. **Amistades (`slv_friendship`):** un par por amistad, con `user_id_1 < user_id_2` y ambos usuarios existentes.
# MAGIC 6. **Trazabilidad (DQ-015):** `loaded_at` y `source_file` en todas las tablas.

# COMMAND ----------

# DBTITLE 1,DDL de E3: crea las tablas Silver y Gold con su definición oficial (si no existen)
# MAGIC %run ../E3_modelo_fisico/01_modelo_fisico_ddl

# COMMAND ----------

# DBTITLE 1,Inicialización: bitácora y helpers de carga
import time
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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

LOADED_AT = spark.sql("SELECT current_timestamp() AS ts").first().ts  # un solo instante por corrida

def registrar_bitacora(capa: str, tabla: str, filas: int, t0: float):
    dur = round(time.time() - t0, 2)
    spark.sql(f"""
        INSERT INTO workspace.yelp_gov.pipeline_run_log
        VALUES (current_timestamp(), '{capa}', '{tabla}', {int(filas)}, {dur})
    """)
    print(f"✔ [{capa.upper()}] {tabla}: {filas:,} filas ({dur} s)")

def cargar(df, tabla: str, t0: float):
    """Reemplaza el contenido de una tabla ya creada en E3, con las columnas y tipos de su definición.
    Usa insertInto, así se conservan COMMENT, constraints, tags y propiedades de la tabla."""
    esquema = spark.table(tabla).schema
    (df.select([F.col(c.name).cast(c.dataType).alias(c.name) for c in esquema])
       .write.insertInto(tabla, overwrite=True))
    registrar_bitacora("silver", tabla, spark.table(tabla).count(), t0)

def quitar_mascara(tabla: str, columna: str) -> bool:
    """Retira la máscara de E6 antes de reescribir la tabla. Devuelve True si había una."""
    esquema_t, nombre_t = tabla.split(".")[1:]
    tiene = (spark.table("workspace.information_schema.column_masks")
             .filter((F.col("table_schema") == esquema_t) & (F.col("table_name") == nombre_t) & (F.col("column_name") == columna))
             .count() > 0)
    if tiene:
        spark.sql(f"ALTER TABLE {tabla} ALTER COLUMN {columna} DROP MASK")
    return tiene

# COMMAND ----------

# DBTITLE 1,1. slv_business (golden record DQ-012)
t0 = time.time()
brz_bus = spark.table("workspace.yelp_bronze.brz_business")

negocios = (brz_bus
    .select(
        F.trim("business_id").alias("business_id"),
        F.trim("name").alias("name"),
        F.nullif(F.trim("address"), F.lit("")).alias("address"),
        F.initcap(F.trim("city")).alias("city"),
        F.upper(F.trim("state")).alias("state"),
        F.nullif(F.trim("postal_code"), F.lit("")).alias("postal_code"),
        F.col("latitude").cast("decimal(9,6)").alias("latitude"),
        F.col("longitude").cast("decimal(9,6)").alias("longitude"),
        F.col("stars").cast("decimal(2,1)").alias("stars"),
        F.coalesce(F.col("review_count").cast("int"), F.lit(0)).alias("review_count"),
        (F.col("is_open") == 1).alias("is_open"),
        F.col("_source_file").alias("source_file"),
        F.col("_ingested_at"),
    )
    .filter(
        F.col("business_id").isNotNull() & (F.col("business_id") != "") &
        F.col("name").isNotNull() & (F.col("name") != "") &
        (F.col("stars").isNull() | F.col("stars").between(1.0, 5.0))
    )
    .withColumn("_rn", F.row_number().over(Window.partitionBy("business_id").orderBy(F.col("_ingested_at").desc_nulls_last())))
    .filter("_rn = 1")
    .drop("_rn", "_ingested_at")
)

# Golden record: mismo nombre + dirección + código postal normalizados = mismo negocio físico.
# Solo se agrupan negocios con dirección; los que no tienen dirección son su propio maestro.
clave_fisica = F.when(
    F.col("address").isNotNull(),
    F.concat_ws("|",
        F.lower(F.regexp_replace(F.col("name"), r"\s+", " ")),
        F.lower(F.regexp_replace(F.col("address"), r"\s+", " ")),
        F.coalesce(F.col("postal_code"), F.lit("")))
).otherwise(F.col("business_id"))

w_maestro = Window.partitionBy("_clave").orderBy(F.col("review_count").desc(), F.col("business_id").asc())
slv_business = (negocios
    .withColumn("_clave", clave_fisica)
    .withColumn("master_business_id", F.first("business_id").over(w_maestro))
    .withColumn("loaded_at", F.lit(LOADED_AT))
    .drop("_clave")
)
cargar(slv_business, "workspace.yelp_silver.slv_business", t0)

# COMMAND ----------

# DBTITLE 1,2. slv_category y slv_business_category
t0 = time.time()
neg_cat = (brz_bus
    .select(
        F.trim("business_id").alias("business_id"),
        F.explode(F.split(F.col("categories"), ",")).alias("name"),
        F.col("_source_file").alias("source_file"),
    )
    .withColumn("name", F.trim("name"))
    .filter(F.col("name").isNotNull() & (F.col("name") != ""))
    .join(spark.table("workspace.yelp_silver.slv_business").select("business_id"), "business_id", "inner")
    .dropDuplicates(["business_id", "name"])
)

slv_category = (neg_cat
    .groupBy("name").agg(F.first("source_file").alias("source_file"))
    .withColumn("category_id", F.dense_rank().over(Window.orderBy("name")).cast("bigint"))
    .withColumn("loaded_at", F.lit(LOADED_AT))
)
cargar(slv_category, "workspace.yelp_silver.slv_category", t0)

t0 = time.time()
slv_business_category = (neg_cat
    .join(spark.table("workspace.yelp_silver.slv_category").select("category_id", "name"), "name", "inner")
    .withColumn("loaded_at", F.lit(LOADED_AT))
)
cargar(slv_business_category, "workspace.yelp_silver.slv_business_category", t0)

# COMMAND ----------

# DBTITLE 1,3. slv_user y slv_friendship
t0 = time.time()
brz_usr = spark.table("workspace.yelp_bronze.brz_user")
# La máscara se retira para recargar y para que Gold copie el nombre real; la tarea E6 del job la vuelve a aplicar al final.
quitar_mascara("workspace.yelp_silver.slv_user", "name")

# elite viene como "2017,2018,20,20": el dataset parte 2020 en "20,20", por eso se corrige antes de contar años.
anios_elite = F.array_distinct(F.filter(
    F.transform(F.split(F.regexp_replace(F.coalesce(F.col("elite"), F.lit("")), "20,20", "2020"), ","), lambda x: F.trim(x)),
    lambda x: (x != "") & (F.lower(x) != "none")))

slv_user = (brz_usr
    .select(
        F.trim("user_id").alias("user_id"),
        F.coalesce(F.nullif(F.trim("name"), F.lit("")), F.lit("Usuario Anónimo")).alias("name"),
        F.coalesce(F.col("review_count").cast("int"), F.lit(0)).alias("review_count"),
        F.to_date("yelping_since").alias("yelping_since"),
        F.coalesce(F.col("fans").cast("int"), F.lit(0)).alias("fans"),
        F.col("average_stars").cast("decimal(3,2)").alias("average_stars"),
        F.size(anios_elite).alias("elite_year_count"),
        F.coalesce(F.col("useful").cast("int"), F.lit(0)).alias("useful_sent"),
        F.coalesce(F.col("funny").cast("int"), F.lit(0)).alias("funny_sent"),
        F.coalesce(F.col("cool").cast("int"), F.lit(0)).alias("cool_sent"),
        F.col("_source_file").alias("source_file"),
    )
    .filter(F.col("user_id").isNotNull() & (F.col("user_id") != ""))
    .dropDuplicates(["user_id"])
    .withColumn("loaded_at", F.lit(LOADED_AT))
)
cargar(slv_user, "workspace.yelp_silver.slv_user", t0)

# Amistades: el par se guarda una vez (user_id_1 < user_id_2) y solo si ambos usuarios existen en slv_user.
t0 = time.time()
usuarios = spark.table("workspace.yelp_silver.slv_user").select("user_id")
slv_friendship = (brz_usr
    .select(
        F.trim("user_id").alias("a"),
        F.explode(F.split(F.col("friends"), ",")).alias("b"),
        F.col("_source_file").alias("source_file"),
    )
    .withColumn("b", F.trim("b"))
    .filter(F.col("a").isNotNull() & (F.col("b") != "") & (F.lower("b") != "none") & (F.col("a") != F.col("b")))
    .select(F.least("a", "b").alias("user_id_1"), F.greatest("a", "b").alias("user_id_2"), "source_file")
    .join(usuarios.withColumnRenamed("user_id", "user_id_1"), "user_id_1", "inner")
    .join(usuarios.withColumnRenamed("user_id", "user_id_2"), "user_id_2", "inner")
    .dropDuplicates(["user_id_1", "user_id_2"])
    .withColumn("loaded_at", F.lit(LOADED_AT))
)
cargar(slv_friendship, "workspace.yelp_silver.slv_friendship", t0)

# COMMAND ----------

# DBTITLE 1,4. slv_review y slv_review_quarantine
# Reglas bloqueantes (la reseña va a cuarentena con rejection_reason):
#   DQ-001 review_id repetido (se conserva la versión más reciente) · DQ-003 faltan autor, negocio, estrellas o fecha,
#   o el autor no existe en slv_user · DQ-004 estrellas fuera de 1..5 · DQ-005 fecha futura o anterior a 2004 ·
#   DQ-006 el negocio no existe en slv_business.
# Las duplicadas lógicas (DQ-002: mismo usuario, negocio y texto) NO se rechazan: Gold las marca con is_valid = false.
t0 = time.time()
brz_rev = spark.table("workspace.yelp_bronze.brz_review")
bus_ids = spark.table("workspace.yelp_silver.slv_business").select(F.col("business_id").alias("_b_ok"))
usr_ids = spark.table("workspace.yelp_silver.slv_user").select(F.col("user_id").alias("_u_ok"))

rev = (brz_rev
    .select(
        F.trim("review_id").alias("review_id"),
        F.trim("user_id").alias("user_id"),
        F.trim("business_id").alias("business_id"),
        F.col("stars").cast("int").alias("stars"),
        F.to_date("date").alias("review_date"),
        F.col("text").cast("string").alias("text"),
        F.coalesce(F.col("useful").cast("int"), F.lit(0)).alias("useful"),
        F.coalesce(F.col("funny").cast("int"), F.lit(0)).alias("funny"),
        F.coalesce(F.col("cool").cast("int"), F.lit(0)).alias("cool"),
        F.col("_source_file").alias("source_file"),
        F.col("_ingested_at"),
    )
    .withColumn("_rn", F.row_number().over(Window.partitionBy("review_id").orderBy(F.col("_ingested_at").desc_nulls_last())))
    .join(bus_ids, F.col("business_id") == F.col("_b_ok"), "left")
    .join(usr_ids, F.col("user_id") == F.col("_u_ok"), "left")
    .withColumn("rejection_reason",
        F.when(F.col("review_id").isNull() | (F.col("_rn") > 1), F.lit("DQ-001: REVIEW_ID_REPETIDO"))
         .when(F.col("user_id").isNull() | F.col("business_id").isNull() | F.col("stars").isNull() | F.col("review_date").isNull(),
               F.lit("DQ-003: DATOS_OBLIGATORIOS_FALTANTES"))
         .when(~F.col("stars").between(1, 5), F.lit("DQ-004: ESTRELLAS_FUERA_DE_RANGO"))
         .when((F.col("review_date") < F.lit("2004-01-01").cast("date")) | (F.col("review_date") > F.current_date()),
               F.lit("DQ-005: FECHA_INVALIDA"))
         .when(F.col("_b_ok").isNull(), F.lit("DQ-006: NEGOCIO_INEXISTENTE"))
         .when(F.col("_u_ok").isNull(), F.lit("DQ-003: AUTOR_INEXISTENTE"))
    )
)

slv_review_quarantine = (rev
    .filter(F.col("rejection_reason").isNotNull())
    .withColumn("rejected_at", F.lit(LOADED_AT))
)
cargar(slv_review_quarantine, "workspace.yelp_silver.slv_review_quarantine", t0)

t0 = time.time()
slv_review = (rev
    .filter(F.col("rejection_reason").isNull())
    .withColumn("loaded_at", F.lit(LOADED_AT))
)
cargar(slv_review, "workspace.yelp_silver.slv_review", t0)

# COMMAND ----------

# DBTITLE 1,5. slv_tip
t0 = time.time()
bus_ids = spark.table("workspace.yelp_silver.slv_business").select("business_id")
usr_ids = spark.table("workspace.yelp_silver.slv_user").select("user_id")

slv_tip = (spark.table("workspace.yelp_bronze.brz_tip")
    .select(
        F.trim("user_id").alias("user_id"),
        F.trim("business_id").alias("business_id"),
        F.trim("text").alias("text"),
        F.to_date("date").alias("tip_date"),
        F.coalesce(F.col("compliment_count").cast("int"), F.lit(0)).alias("compliment_count"),
        F.col("_source_file").alias("source_file"),
    )
    .filter(F.col("tip_date").isNotNull())
    .join(bus_ids, "business_id", "inner")
    .join(usr_ids, "user_id", "inner")
    .withColumn("tip_id", F.sha2(F.concat_ws("|", "user_id", "business_id", F.col("tip_date").cast("string"), F.coalesce("text", F.lit(""))), 256))
    .dropDuplicates(["tip_id"])
    .withColumn("loaded_at", F.lit(LOADED_AT))
)
cargar(slv_tip, "workspace.yelp_silver.slv_tip", t0)

# COMMAND ----------

# DBTITLE 1,6. slv_checkin (grano evento, DQ-009 y DQ-010)
# DQ-009: las fechas que no se pueden convertir a fecha-hora válida se descartan.
# DQ-010: los check-ins repetidos del mismo negocio a la misma fecha-hora quedan con is_duplicate = true.
t0 = time.time()
slv_checkin = (spark.table("workspace.yelp_bronze.brz_checkin")
    .select(
        F.trim("business_id").alias("business_id"),
        F.posexplode(F.split(F.col("date"), ",")).alias("pos", "fecha_str"),
        F.col("_source_file").alias("source_file"),
    )
    .withColumn("checkin_at", F.to_timestamp(F.trim("fecha_str")))
    .filter(
        F.col("business_id").isNotNull() & F.col("checkin_at").isNotNull() &
        (F.col("checkin_at") >= F.lit("2004-01-01").cast("timestamp")) &
        (F.col("checkin_at") <= F.current_timestamp())
    )
    .join(bus_ids, "business_id", "inner")
    .withColumn("checkin_id", F.sha2(F.concat_ws("|", "business_id", F.col("checkin_at").cast("string"), F.col("pos").cast("string")), 256))
    .withColumn("is_duplicate", F.row_number().over(Window.partitionBy("business_id", "checkin_at").orderBy("pos")) > 1)
    .withColumn("loaded_at", F.lit(LOADED_AT))
)
cargar(slv_checkin, "workspace.yelp_silver.slv_checkin", t0)

# COMMAND ----------

# DBTITLE 1,7. slv_photo (sin datos en la muestra)
# El dataset descargado no incluye photo.json: la tabla queda creada (E3) y vacía.
if spark.catalog.tableExists("workspace.yelp_bronze.brz_photo"):
    t0 = time.time()
    slv_photo = (spark.table("workspace.yelp_bronze.brz_photo")
        .select(F.trim("photo_id").alias("photo_id"), F.trim("business_id").alias("business_id"),
                "caption", "label", F.col("_source_file").alias("source_file"))
        .join(bus_ids, "business_id", "inner")
        .dropDuplicates(["photo_id"])
        .withColumn("loaded_at", F.lit(LOADED_AT)))
    cargar(slv_photo, "workspace.yelp_silver.slv_photo", t0)
else:
    print("ℹ workspace.yelp_bronze.brz_photo no existe: slv_photo queda vacía")

# COMMAND ----------

# DBTITLE 1,8. Corrección de consistencia (DQ-014)
# DQ-014: si la fecha de alta del usuario es posterior a su primera reseña, se ajusta a la fecha de esa reseña.
spark.sql("""
MERGE INTO workspace.yelp_silver.slv_user AS u
USING (
  SELECT user_id, min(review_date) AS first_review_date
  FROM workspace.yelp_silver.slv_review
  GROUP BY user_id
) AS r
ON u.user_id = r.user_id AND (u.yelping_since IS NULL OR u.yelping_since > r.first_review_date)
WHEN MATCHED THEN UPDATE SET u.yelping_since = r.first_review_date
""")

# COMMAND ----------

# DBTITLE 1,9. Quality Gate (E5): evaluación de las 20 reglas
# MAGIC %run ../E5_calidad_datos/01_reglas_calidad

# COMMAND ----------

# DBTITLE 1,9.1 Quality Gate: el pipeline se detiene si falla una regla crítica
df_res = spark.table("workspace.yelp_gov.dq_results")
df_reg = spark.table("workspace.yelp_gov.dq_rules")
max_ts = df_res.agg(F.max("executed_at").alias("max_ts")).first()["max_ts"]

fallas_criticas = (df_res.filter((F.col("executed_at") == F.lit(max_ts)) & (~F.col("passed")))
    .join(df_reg.filter(F.col("criticality") == "critical"), on="rule_id", how="inner"))
n_fallas = fallas_criticas.count()
assert n_fallas == 0, f"❌ {n_fallas} reglas críticas fallaron: {[r.rule_id for r in fallas_criticas.collect()]} — pipeline detenido"
print("✔ Quality Gate superado: 0 fallas críticas en Silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definition of Done (E7.2)
# MAGIC - [x] Las 10 tablas Silver de `yelp_silver.txt` cargadas sin redefinir su DDL (`slv_business`, `slv_category`, `slv_business_category`, `slv_user`, `slv_friendship`, `slv_review`, `slv_review_quarantine`, `slv_tip`, `slv_checkin`, `slv_photo`).
# MAGIC - [x] Golden record (`master_business_id`), categorías normalizadas en tabla puente y check-ins a grano evento con `is_duplicate`.
# MAGIC - [x] Cuarentena de reseñas con `rejection_reason`; duplicadas lógicas conservadas para que Gold las marque como no válidas.
# MAGIC - [x] `loaded_at` y `source_file` en todas las tablas; Quality Gate de E5 integrado.
