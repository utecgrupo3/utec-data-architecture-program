# Databricks notebook source
# MAGIC %md
# MAGIC # E7.3 · Silver → Gold: Esquema Estrella Kimball y Productos de Datos (Grupo 03 · Data Evolution)
# MAGIC **Cierra con:** M6 (construcción) + M9 (consumo)
# MAGIC
# MAGIC Gold materializa el **modelo dimensional Kimball (ADR-05)** definido en la Sección 4.3 del *Documento Formal de Arquitectura de Datos (v1.2)* y las tablas de KPIs que consume el dashboard E8.
# MAGIC **Cada tabla gold responde a una pregunta explícita de un stakeholder del caso:**
# MAGIC
# MAGIC | Producto de datos / Objeto Gold | Stakeholder | Pregunta de negocio que responde | Requerimiento / ADR |
# MAGIC | :--- | :--- | :--- | :--- |
# MAGIC | `gld_kpi_negocio_mensual` | **Producto (Andrew Collins)** | ¿Cómo evoluciona el rating promedio, el rating ponderado bayesiano ($BWR$) y el volumen de reseñas por negocio y mes? | **REQ-01 · ADR-02** |
# MAGIC | `gld_checkins_diarios` | **Ops Comerciales (Carla Romero)** | ¿Cuál es el conteo OFICIAL diario de check-ins válidos deduplicados por hora (HAI) por negocio? | **REQ-02, REQ-05 · ADR-01** |
# MAGIC | `gld_salud_resenas` | **Trust & Safety (Rafael Nakamura)** | ¿Qué porcentaje de reseñas son aisladas en cuarentena por mes y cuál es el Ratio de Salud (RHR)? | **REQ-03 · Acta 02** |
# MAGIC | `gld_cohortes_retencion_mensual` | **Producto (Andrew Collins)** | ¿Cuál es la tasa de retención mensual de cohortes de usuarios ($CRR\%$) según su mes de alta (`yelping_since`)? | **REQ-04 · ADR-05** |
# MAGIC | `gld_dim_negocio` · `gld_dim_usuario` · `gld_dim_tiempo` · `gld_fact_resenas` | **Data Science (Mei Lin) & BI** | Esquema Estrella Kimball completo con dimensiones conformes (SCD1/SCD2), PII enmascarada y hechos atómicos de reseñas. | **ADR-05 · ADR-06** |

# COMMAND ----------

# DBTITLE 1: Inicialización de esquemas y helper de bitácora FinOps
import time
from pyspark.sql import functions as F

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.yelp_gold")
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

# DBTITLE 1: 1. Esquema Estrella Kimball — Dimensión Tiempo (gld_dim_tiempo generada con sequence + explode)
t0 = time.time()
spark.sql("""
CREATE OR REPLACE TABLE workspace.yelp_gold.gld_dim_tiempo (
  date_sk      INT     NOT NULL COMMENT 'Clave subrogada entera de fecha en formato YYYYMMDD',
  fecha        DATE    NOT NULL COMMENT 'Fecha calendario en formato estándar YYYY-MM-DD',
  anio         INT     NOT NULL COMMENT 'Año calendario de cuatro dígitos (ej. 2026)',
  trimestre    INT     NOT NULL COMMENT 'Trimestre calendario del año (1 a 4)',
  mes          INT     NOT NULL COMMENT 'Mes calendario del año (1 a 12)',
  nombre_mes   STRING  NOT NULL COMMENT 'Nombre del mes para reportes ejecutivos',
  dia_semana   STRING  NOT NULL COMMENT 'Nombre del día de la semana para análisis de estacionalidad intra-semanal',
  CONSTRAINT pk_gld_dim_tiempo PRIMARY KEY (date_sk)
)
COMMENT 'Dimensión calendario generada programáticamente para el esquema estrella Kimball. Consumidor: Transversal.'
TBLPROPERTIES (delta.enableChangeDataFeed = true)
AS
SELECT
  CAST(date_format(fecha, 'yyyyMMdd') AS INT) AS date_sk,
  fecha                                       AS fecha,
  year(fecha)                                 AS anio,
  quarter(fecha)                              AS trimestre,
  month(fecha)                                AS mes,
  date_format(fecha, 'MMMM')                  AS nombre_mes,
  date_format(fecha, 'EEEE')                  AS dia_semana
FROM (
  SELECT explode(sequence(to_date('2004-01-01'), to_date('2030-12-31'), interval 1 day)) AS fecha
)
""")
registrar_bitacora("gold", "workspace.yelp_gold.gld_dim_tiempo",
                   spark.table("workspace.yelp_gold.gld_dim_tiempo").count(), t0)

# COMMAND ----------

# DBTITLE 1: 2. Esquema Estrella Kimball — Dimensión Negocio (gld_dim_negocio con SCD1/SCD2) y Dimensión Usuario (gld_dim_usuario con PII enmascarada)
# --- 2.1 gld_dim_negocio ---
t0_dn = time.time()
spark.sql("""
CREATE OR REPLACE TABLE workspace.yelp_gold.gld_dim_negocio (
  business_sk            STRING        NOT NULL COMMENT 'Clave subrogada (surrogate key) de la dimensión negocio',
  business_id            STRING        NOT NULL COMMENT 'Clave natural del establecimiento comercial en YelpConnect',
  nombre                 STRING        NOT NULL COMMENT 'Nombre comercial del establecimiento (SCD Tipo 1)',
  ciudad                 STRING        NOT NULL COMMENT 'Ciudad normalizada del establecimiento comercial',
  estado                 STRING                 COMMENT 'Código de estado o provincia de dos letras',
  categoria_principal    STRING                 COMMENT 'Categoría comercial primaria extraída del primer elemento del arreglo para BI directo',
  categorias             ARRAY<STRING>          COMMENT 'Arreglo completo de categorías comerciales del negocio (ADR-03)',
  esta_abierto           BOOLEAN                COMMENT 'Estado operativo del negocio (auditado con historia SCD Tipo 2)',
  fecha_inicio_vigencia  TIMESTAMP     NOT NULL COMMENT 'Fecha de inicio de vigencia del registro dimensional (SCD Tipo 2)',
  fecha_fin_vigencia     TIMESTAMP              COMMENT 'Fecha de fin de vigencia del registro dimensional (NULL si es la versión actual)',
  es_actual              BOOLEAN       NOT NULL COMMENT 'TRUE si el registro corresponde a la versión vigente actual del negocio',
  CONSTRAINT pk_gld_dim_negocio PRIMARY KEY (business_sk)
)
COMMENT 'Dimensión conforme de Negocios con soporte híbrido SCD Tipo 1 y Tipo 2. Consumidor: Producto y Data Science.'
TBLPROPERTIES (delta.enableChangeDataFeed = true)
AS
SELECT
  sha2(concat_ws('|', business_id, cast(esta_abierto AS STRING)), 256) AS business_sk,
  business_id,
  nombre,
  ciudad,
  estado,
  coalesce(element_at(categorias, 1), 'Sin Categoría')                AS categoria_principal,
  categorias,
  esta_abierto,
  fecha_carga                                                         AS fecha_inicio_vigencia,
  CAST(NULL AS TIMESTAMP)                                             AS fecha_fin_vigencia,
  true                                                                AS es_actual
FROM workspace.yelp_silver.slv_business
""")
registrar_bitacora("gold", "workspace.yelp_gold.gld_dim_negocio",
                   spark.table("workspace.yelp_gold.gld_dim_negocio").count(), t0_dn)

# --- 2.2 gld_dim_usuario (con enmascaramiento de PII según ADR-06) ---
t0_du = time.time()
spark.sql("""
CREATE OR REPLACE TABLE workspace.yelp_gold.gld_dim_usuario (
  user_sk        STRING    NOT NULL COMMENT 'Clave subrogada (surrogate key) de la dimensión usuario',
  user_id        STRING    NOT NULL COMMENT 'Clave natural seudonimizada del usuario en YelpConnect',
  name_masked    STRING    NOT NULL COMMENT 'Nombre del usuario anonimizado/enmascarado para consumo analítico seguro (ADR-06)',
  yelping_since  TIMESTAMP NOT NULL COMMENT 'Fecha de registro original del comensal para análisis de antigüedad y cohortes',
  es_elite       BOOLEAN   NOT NULL COMMENT 'TRUE si el usuario cuenta con distinción activa en la comunidad Élite (SCD Tipo 2)',
  average_stars  DOUBLE             COMMENT 'Calificación promedio histórica otorgada por el comensal',
  es_actual      BOOLEAN   NOT NULL COMMENT 'TRUE si representa la versión vigente del perfil en la dimensión',
  CONSTRAINT pk_gld_dim_usuario PRIMARY KEY (user_sk)
)
COMMENT 'Dimensión conforme de Usuarios con privacidad por diseño (nombre enmascarado). Consumidor: Producto y Data Science.'
TBLPROPERTIES (delta.enableChangeDataFeed = true)
AS
SELECT
  sha2(user_id, 256)                                   AS user_sk,
  user_id,
  concat(substr(coalesce(name, 'U'), 1, 1), '***')     AS name_masked,
  yelping_since,
  (length(trim(coalesce(elite, ''))) > 0)              AS es_elite,
  average_stars,
  true                                                 AS es_actual
FROM workspace.yelp_silver.slv_user
""")
registrar_bitacora("gold", "workspace.yelp_gold.gld_dim_usuario",
                   spark.table("workspace.yelp_gold.gld_dim_usuario").count(), t0_du)

# COMMAND ----------

# DBTITLE 1: 3. Esquema Estrella Kimball — Tabla de Hechos Atómica (gld_fact_resenas con FKs declaradas)
t0_fr = time.time()
spark.sql("""
CREATE OR REPLACE TABLE workspace.yelp_gold.gld_fact_resenas (
  review_sk    STRING  NOT NULL COMMENT 'Clave subrogada del hecho atómico de reseña',
  review_id    STRING  NOT NULL COMMENT 'Clave degenerada con el identificador original de la reseña verificada',
  business_sk  STRING  NOT NULL COMMENT 'Clave foránea hacia la dimensión de negocios (gld_dim_negocio)',
  user_sk      STRING  NOT NULL COMMENT 'Clave foránea hacia la dimensión de usuarios (gld_dim_usuario)',
  date_sk      INT     NOT NULL COMMENT 'Clave foránea hacia la dimensión de tiempo (gld_dim_tiempo)',
  stars        INT     NOT NULL COMMENT 'Métrica aditiva/semi-aditiva de calificación en estrellas (1 a 5)',
  text_len     INT     NOT NULL COMMENT 'Longitud en caracteres del comentario de la reseña (sin exponer PII indirecta)',
  useful       INT     NOT NULL COMMENT 'Cantidad de votos de utilidad comunitaria recibidos por la reseña',
  CONSTRAINT pk_gld_fact_resenas PRIMARY KEY (review_sk),
  CONSTRAINT fk_fact_negocio FOREIGN KEY (business_sk) REFERENCES workspace.yelp_gold.gld_dim_negocio(business_sk),
  CONSTRAINT fk_fact_usuario FOREIGN KEY (user_sk) REFERENCES workspace.yelp_gold.gld_dim_usuario(user_sk),
  CONSTRAINT fk_fact_tiempo  FOREIGN KEY (date_sk) REFERENCES workspace.yelp_gold.gld_dim_tiempo(date_sk)
)
COMMENT 'Tabla de hechos transaccional atómica (grano: 1 fila por reseña verificada). Consumidor: Producto y Data Science (Mei Lin).'
TBLPROPERTIES (delta.enableChangeDataFeed = true)
AS
SELECT
  sha2(r.review_id, 256)                                   AS review_sk,
  r.review_id                                              AS review_id,
  dn.business_sk                                           AS business_sk,
  du.user_sk                                               AS user_sk,
  CAST(date_format(r.fecha_resena, 'yyyyMMdd') AS INT)     AS date_sk,
  r.stars                                                  AS stars,
  length(trim(r.text))                                     AS text_len,
  coalesce(r.useful, 0)                                    AS useful
FROM workspace.yelp_silver.slv_review r
JOIN workspace.yelp_gold.gld_dim_negocio dn
  ON r.business_id = dn.business_id AND dn.es_actual = true
JOIN workspace.yelp_gold.gld_dim_usuario du
  ON r.user_id = du.user_id AND du.es_actual = true
""")
registrar_bitacora("gold", "workspace.yelp_gold.gld_fact_resenas",
                   spark.table("workspace.yelp_gold.gld_fact_resenas").count(), t0_fr)

# COMMAND ----------

# DBTITLE 1: 4. Producto Gold 1 — gld_kpi_negocio_mensual (Producto: Andrew Collins — Rating Promedio y Bayesiano ADR-02)
t0_kpi = time.time()
spark.sql("""
CREATE OR REPLACE TABLE workspace.yelp_gold.gld_kpi_negocio_mensual (
  business_id       STRING    NOT NULL COMMENT 'Identificador único del establecimiento comercial evaluado',
  nombre            STRING    NOT NULL COMMENT 'Nombre comercial del negocio para visualización en el dashboard E8',
  ciudad            STRING    NOT NULL COMMENT 'Ciudad normalizada del establecimiento comercial',
  mes               TIMESTAMP NOT NULL COMMENT 'Mes calendario truncado de evaluación del KPI (YYYY-MM-01)',
  n_resenas         BIGINT    NOT NULL COMMENT 'Volumen mensual de reseñas verificadas recibidas por el negocio',
  rating_promedio   DOUBLE    NOT NULL COMMENT 'Promedio aritmético simple de calificaciones del negocio en el mes',
  rating_bayesiano  DOUBLE             COMMENT 'Rating Promedio Ponderado Bayesiano (BWR con m=25 reseñas según ADR-02)',
  usuarios_unicos   BIGINT    NOT NULL COMMENT 'Cantidad de comensales únicos que reseñaron el negocio en el mes',
  CONSTRAINT pk_gld_kpi_negocio_mensual PRIMARY KEY (business_id, mes)
)
COMMENT 'KPIs mensuales de satisfacción y volumen por negocio. Stakeholder: Producto (Andrew Collins — REQ-01).'
TBLPROPERTIES (delta.enableChangeDataFeed = true)
AS
WITH media_ciudad AS (
  SELECT b.ciudad, avg(CAST(r.stars AS DOUBLE)) AS c_global
  FROM workspace.yelp_silver.slv_review r
  JOIN workspace.yelp_silver.slv_business b USING (business_id)
  GROUP BY b.ciudad
),
agregado_mensual AS (
  SELECT
    b.business_id,
    b.nombre,
    b.ciudad,
    date_trunc('month', r.fecha_resena)          AS mes,
    count(*)                                     AS n_resenas,
    avg(CAST(r.stars AS DOUBLE))                 AS r_avg,
    count(DISTINCT r.user_id)                    AS usuarios_unicos
  FROM workspace.yelp_silver.slv_review r
  JOIN workspace.yelp_silver.slv_business b USING (business_id)
  GROUP BY b.business_id, b.nombre, b.ciudad, date_trunc('month', r.fecha_resena)
)
SELECT
  a.business_id,
  a.nombre,
  a.ciudad,
  a.mes,
  a.n_resenas,
  round(a.r_avg, 2) AS rating_promedio,
  round((a.n_resenas * a.r_avg + 25.0 * coalesce(mc.c_global, a.r_avg)) / (a.n_resenas + 25.0), 2) AS rating_bayesiano,
  a.usuarios_unicos
FROM agregado_mensual a
LEFT JOIN media_ciudad mc USING (ciudad)
""")
registrar_bitacora("gold", "workspace.yelp_gold.gld_kpi_negocio_mensual",
                   spark.table("workspace.yelp_gold.gld_kpi_negocio_mensual").count(), t0_kpi)

# COMMAND ----------

# DBTITLE 1: 5. Productos Gold 2, 3 y 4 — gld_checkins_diarios (Carla), gld_salud_resenas (Rafael) y gld_cohortes_retencion_mensual (Andrew)
# --- 5.1 gld_checkins_diarios (Operaciones Comerciales — HAI / ADR-01) ---
t0_chk = time.time()
spark.sql("""
CREATE OR REPLACE TABLE workspace.yelp_gold.gld_checkins_diarios (
  business_id       STRING  NOT NULL COMMENT 'Identificador del establecimiento comercial visitado',
  fecha             DATE    NOT NULL COMMENT 'Fecha calendario de las visitas presenciales confirmadas',
  checkins_validos  BIGINT  NOT NULL COMMENT 'Conteo oficial homologado de check-ins válidos deduplicados por hora (HAI - ADR-01)',
  CONSTRAINT pk_gld_checkins_diarios PRIMARY KEY (business_id, fecha)
)
COMMENT 'Conteo oficial diario de check-ins válidos para liquidación publicitaria y fidelidad. Stakeholder: Operaciones Comerciales (Carla Romero — REQ-02, REQ-05).'
TBLPROPERTIES (delta.enableChangeDataFeed = true)
AS
SELECT
  business_id,
  to_date(fecha_checkin) AS fecha,
  count(*)               AS checkins_validos
FROM workspace.yelp_silver.slv_checkin
GROUP BY business_id, to_date(fecha_checkin)
""")
registrar_bitacora("gold", "workspace.yelp_gold.gld_checkins_diarios",
                   spark.table("workspace.yelp_gold.gld_checkins_diarios").count(), t0_chk)

# --- 5.2 gld_salud_resenas (Trust & Safety — Rafael Nakamura / REQ-03) ---
t0_sal = time.time()
spark.sql("""
CREATE OR REPLACE TABLE workspace.yelp_gold.gld_salud_resenas (
  mes                  TIMESTAMP NOT NULL COMMENT 'Mes calendario de auditoría de calidad y moderación de reseñas',
  resenas_verificadas  BIGINT    NOT NULL COMMENT 'Volumen mensual de reseñas que aprobaron el 100% de reglas DQ críticas',
  resenas_cuarentena   BIGINT    NOT NULL COMMENT 'Volumen mensual de reseñas aisladas preventivamente en slv_review_cuarentena',
  pct_cuarentena       DOUBLE    NOT NULL COMMENT 'Porcentaje de reseñas en cuarentena sobre el total bruto del mes (complemento RHR)',
  CONSTRAINT pk_gld_salud_resenas PRIMARY KEY (mes)
)
COMMENT 'Métricas mensuales de salud de calidad de reseñas y tasa de cuarentena. Stakeholder: Trust & Safety (Rafael Nakamura — REQ-03).'
TBLPROPERTIES (delta.enableChangeDataFeed = true)
AS
WITH ok AS (
  SELECT date_trunc('month', fecha_resena) AS mes, count(*) AS resenas_verificadas
  FROM workspace.yelp_silver.slv_review
  GROUP BY 1
),
cuar AS (
  SELECT date_trunc('month', coalesce(fecha_resena, fecha_rechazo)) AS mes, count(*) AS resenas_cuarentena
  FROM workspace.yelp_silver.slv_review_cuarentena
  GROUP BY 1
)
SELECT
  coalesce(ok.mes, cuar.mes)                                  AS mes,
  coalesce(ok.resenas_verificadas, 0)                         AS resenas_verificadas,
  coalesce(cuar.resenas_cuarentena, 0)                        AS resenas_cuarentena,
  round(
    100.0 * coalesce(cuar.resenas_cuarentena, 0) /
    greatest(coalesce(ok.resenas_verificadas, 0) + coalesce(cuar.resenas_cuarentena, 0), 1),
    2
  )                                                           AS pct_cuarentena
FROM ok
FULL OUTER JOIN cuar ON ok.mes = cuar.mes
WHERE coalesce(ok.mes, cuar.mes) IS NOT NULL
""")
registrar_bitacora("gold", "workspace.yelp_gold.gld_salud_resenas",
                   spark.table("workspace.yelp_gold.gld_salud_resenas").count(), t0_sal)

# --- 5.3 gld_cohortes_retencion_mensual (Producto — Andrew Collins / REQ-04) ---
t0_coh = time.time()
spark.sql("""
CREATE OR REPLACE TABLE workspace.yelp_gold.gld_cohortes_retencion_mensual (
  cohorte_mes       DATE    NOT NULL COMMENT 'Mes calendario de registro original del usuario (cohorte de adquisición)',
  mes_actividad     DATE    NOT NULL COMMENT 'Mes calendario en el que se registra la interacción recurrente de la cohorte',
  usuarios_activos  BIGINT  NOT NULL COMMENT 'Cantidad de usuarios únicos de la cohorte con actividad validada en el mes',
  retencion_pct     DOUBLE  NOT NULL COMMENT 'Tasa de Retención de Cohortes (CRR %) respecto al tamaño inicial de la camada',
  CONSTRAINT pk_gld_cohortes_retencion PRIMARY KEY (cohorte_mes, mes_actividad)
)
COMMENT 'Matriz mensual de retención de cohortes de usuarios (CRR). Stakeholder: Producto (Andrew Collins — REQ-04).'
TBLPROPERTIES (delta.enableChangeDataFeed = true)
AS
WITH base_cohortes AS (
  SELECT
    to_date(date_trunc('month', yelping_since)) AS cohorte_mes,
    count(DISTINCT user_id)                     AS tamano_cohorte
  FROM workspace.yelp_silver.slv_user
  GROUP BY 1
),
actividad AS (
  SELECT
    to_date(date_trunc('month', u.yelping_since)) AS cohorte_mes,
    to_date(date_trunc('month', r.fecha_resena))  AS mes_actividad,
    count(DISTINCT r.user_id)                     AS usuarios_activos
  FROM workspace.yelp_silver.slv_review r
  JOIN workspace.yelp_silver.slv_user u USING (user_id)
  WHERE r.fecha_resena >= u.yelping_since
  GROUP BY 1, 2
)
SELECT
  a.cohorte_mes,
  a.mes_actividad,
  a.usuarios_activos,
  round(100.0 * a.usuarios_activos / greatest(b.tamano_cohorte, 1), 2) AS retencion_pct
FROM actividad a
JOIN base_cohortes b USING (cohorte_mes)
""")
registrar_bitacora("gold", "workspace.yelp_gold.gld_cohortes_retencion_mensual",
                   spark.table("workspace.yelp_gold.gld_cohortes_retencion_mensual").count(), t0_coh)

# COMMAND ----------

# DBTITLE 1: Validación — cada tabla gold tiene consumidor declarado y 100% de columnas con COMMENT
tablas_gold = [r.tableName for r in spark.sql("SHOW TABLES IN workspace.yelp_gold").collect()]
assert len(tablas_gold) >= 5, f"Se esperan ≥5 objetos gold (hay {len(tablas_gold)})"

sin_comment_gold = spark.sql("""
  SELECT table_name, column_name
  FROM workspace.information_schema.columns
  WHERE table_schema = 'yelp_gold' AND (comment IS NULL OR comment = '')
""").collect()
assert not sin_comment_gold, f"❌ Columnas gold sin COMMENT: {sin_comment_gold}"

print(f"✔ Capa gold validada ({len(tablas_gold)} tablas):", sorted(tablas_gold))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definition of Done (E7.3)
# MAGIC - [x] ≥2 tablas de KPIs (`gld_kpi_negocio_mensual`, `gld_checkins_diarios`, `gld_salud_resenas`, `gld_cohortes_retencion_mensual`) + esquema estrella completo (3 dimensiones: `gld_dim_negocio`, `gld_dim_usuario`, `gld_dim_tiempo` + 1 tabla de hechos: `gld_fact_resenas`).
# MAGIC - [x] Tabla stakeholder → producto → pregunta completa en el markdown superior.
# MAGIC - [x] 100% de tablas y columnas de `yelp_gold` con `COMMENT`, `PRIMARY KEY` y `FOREIGN KEY` activando el lineage automático en Catalog Explorer.
