# Databricks notebook source
# MAGIC %md
# MAGIC # E5 · Reglas de Calidad de Datos + Motor de Evaluación (Grupo 03 · Data Evolution)
# MAGIC **Se define en:** M2/M3 (reglas en lenguaje de negocio) · **Se implementa tras:** M6 (cuando ya hay datos en Silver)
# MAGIC
# MAGIC Enfoque *rules-as-data*: las reglas viven en la tabla `workspace.yelp_gov.dq_rules` (no
# MAGIC hardcodeadas), un motor genérico las ejecuta y persiste resultados en
# MAGIC `workspace.yelp_gov.dq_results`. Ese historial alimenta el dashboard de E8 y el Quality Gate del pipeline E7 (`02_silver_limpieza`).
# MAGIC
# MAGIC **Catálogo (15 reglas, `DQ-001` a `DQ-015`):** es el que citan los comentarios del modelo (`yelp_silver.txt` y `yelp_gold.txt`), por ejemplo DQ-010 para check-ins duplicados y DQ-012 para el golden record.
# MAGIC * Criticidad `critical`: la regla es un **Quality Gate** y detiene el pipeline si no alcanza su umbral.
# MAGIC * Criticidad `high` o `medium`: la regla se mide y se reporta, pero no bloquea la carga (por ejemplo, DQ-002 marca las reseñas duplicadas como no válidas en Gold).

# COMMAND ----------

# DBTITLE 1,1. Inicialización del esquema y tablas de gobierno DQ
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.yelp_gov")

spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.yelp_gov.dq_rules (
  rule_id          STRING  COMMENT 'Identificador único (DQ-001 a DQ-015)',
  target_table     STRING  COMMENT 'Tabla física evaluada en workspace.yelp_silver',
  column_name      STRING  COMMENT 'Columna evaluada (o * si es a nivel tabla)',
  dimension        STRING  COMMENT 'Dimensión DQ: validity|uniqueness|completeness|consistency|accuracy|timeliness',
  description      STRING  COMMENT 'Regla en lenguaje de negocio aprobada por el Data Owner',
  sql_expression   STRING  COMMENT 'Predicado SQL que DEBE cumplirse sobre la tabla objetivo',
  criticality      STRING  COMMENT 'critical|high|medium — las reglas critical actúan como Quality Gate en E7',
  threshold_pct    DOUBLE  COMMENT 'Porcentaje mínimo de filas que deben cumplir la regla (0.0 a 100.0)'
) USING DELTA
COMMENT 'E5 · Catálogo gobernado de reglas de calidad de datos'
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.yelp_gov.dq_results (
  executed_at      TIMESTAMP COMMENT 'Marca temporal exacta de la corrida del motor DQ',
  rule_id          STRING    COMMENT 'Identificador de la regla evaluada (FK hacia dq_rules)',
  target_table     STRING    COMMENT 'Tabla física evaluada en workspace.yelp_silver',
  total_rows       BIGINT    COMMENT 'Total de registros evaluados en la tabla',
  passed_rows      BIGINT    COMMENT 'Total de registros que cumplen satisfactoriamente la regla',
  compliance_pct   DOUBLE    COMMENT 'Porcentaje de cumplimiento (0.00 a 100.00)',
  passed           BOOLEAN   COMMENT 'TRUE si compliance_pct >= threshold_pct de la regla'
) USING DELTA
COMMENT 'E5 · Historial de ejecuciones del motor de calidad (insumo de E7 Quality Gate y E8 Dashboard CDO)'
""")

# COMMAND ----------

# DBTITLE 1,2. Catálogo de 15 reglas de calidad (DQ-001 a DQ-015)
from pyspark.sql import Row

S = "workspace.yelp_silver"

# Clave física del golden record: la misma que usa 02_silver_limpieza para asignar master_business_id.
CLAVE_FISICA = ("concat_ws('|', lower(regexp_replace(name, '\\\\s+', ' ')), "
                "lower(regexp_replace(address, '\\\\s+', ' ')), coalesce(postal_code, ''))")

reglas = [
    Row(rule_id="DQ-001", target_table=f"{S}.slv_review", column_name="review_id", dimension="uniqueness",
        description="Cada reseña se registra una sola vez (review_id único)",
        sql_expression="review_id IS NOT NULL", criticality="critical", threshold_pct=100.0),
    Row(rule_id="DQ-002", target_table=f"{S}.slv_review", column_name="*", dimension="uniqueness",
        description="Un usuario no publica el mismo texto dos veces sobre el mismo negocio. Las repetidas se marcan como no válidas en Gold (is_valid = false) y no bloquean la carga",
        sql_expression=(
            "review_id NOT IN (SELECT review_id FROM ("
            "  SELECT review_id, row_number() OVER (PARTITION BY user_id, business_id, md5(lower(trim(text))) ORDER BY review_date, review_id) AS rn"
            f"  FROM {S}.slv_review WHERE text IS NOT NULL AND length(trim(text)) > 0) WHERE rn > 1)"),
        criticality="high", threshold_pct=99.5),
    Row(rule_id="DQ-003", target_table=f"{S}.slv_review", column_name="*", dimension="completeness",
        description="Toda reseña tiene autor existente, negocio, estrellas y fecha",
        sql_expression=(f"user_id IS NOT NULL AND business_id IS NOT NULL AND stars IS NOT NULL AND review_date IS NOT NULL "
                       f"AND user_id IN (SELECT user_id FROM {S}.slv_user)"),
        criticality="critical", threshold_pct=100.0),
    Row(rule_id="DQ-004", target_table=f"{S}.slv_review", column_name="stars", dimension="validity",
        description="Las estrellas son un número entero de 1 a 5",
        sql_expression="stars BETWEEN 1 AND 5", criticality="critical", threshold_pct=100.0),
    Row(rule_id="DQ-005", target_table=f"{S}.slv_review", column_name="review_date", dimension="validity",
        description="La fecha de la reseña no es futura ni anterior a 2004, año de fundación de Yelp",
        sql_expression="review_date >= DATE'2004-01-01' AND review_date <= current_date()",
        criticality="high", threshold_pct=100.0),
    Row(rule_id="DQ-006", target_table=f"{S}.slv_review", column_name="business_id", dimension="consistency",
        description="Toda reseña corresponde a un negocio existente en la muestra",
        sql_expression=f"business_id IN (SELECT business_id FROM {S}.slv_business)",
        criticality="critical", threshold_pct=100.0),
    Row(rule_id="DQ-007", target_table=f"{S}.slv_business", column_name="stars", dimension="consistency",
        description="La calificación declarada del negocio no difiere en más de 0,5 estrellas del promedio de sus reseñas",
        sql_expression=(
            "business_id NOT IN (SELECT b.business_id FROM "
            f"{S}.slv_business b JOIN (SELECT business_id, avg(stars) AS prom FROM {S}.slv_review GROUP BY business_id) r "
            "ON b.business_id = r.business_id WHERE abs(b.stars - r.prom) > 0.5)"),
        criticality="medium", threshold_pct=90.0),
    Row(rule_id="DQ-008", target_table=f"{S}.slv_business", column_name="review_count", dimension="consistency",
        description="Las reseñas presentes en la muestra no superan las reseñas declaradas del negocio",
        sql_expression=(
            "business_id NOT IN (SELECT b.business_id FROM "
            f"{S}.slv_business b JOIN (SELECT business_id, count(*) AS n FROM {S}.slv_review GROUP BY business_id) r "
            "ON b.business_id = r.business_id WHERE r.n > b.review_count)"),
        criticality="medium", threshold_pct=98.0),
    Row(rule_id="DQ-009", target_table=f"{S}.slv_checkin", column_name="checkin_at", dimension="validity",
        description="Cada fecha de check-in tiene formato de fecha y hora válido (las inválidas se descartan al cargar)",
        sql_expression="checkin_at IS NOT NULL AND checkin_at >= TIMESTAMP'2004-01-01 00:00:00' AND checkin_at <= current_timestamp()",
        criticality="high", threshold_pct=99.9),
    Row(rule_id="DQ-010", target_table=f"{S}.slv_checkin", column_name="*", dimension="uniqueness",
        description="No hay dos check-ins del mismo negocio en la misma fecha y hora; los repetidos quedan con is_duplicate = true y no cuentan",
        sql_expression="NOT is_duplicate", criticality="high", threshold_pct=99.0),
    Row(rule_id="DQ-011", target_table=f"{S}.slv_business", column_name="*", dimension="completeness",
        description="Todo negocio tiene nombre, dirección, ciudad y coordenadas",
        sql_expression="name IS NOT NULL AND address IS NOT NULL AND city IS NOT NULL AND latitude IS NOT NULL AND longitude IS NOT NULL",
        criticality="high", threshold_pct=98.0),
    Row(rule_id="DQ-012", target_table=f"{S}.slv_business", column_name="*", dimension="uniqueness",
        description="Un mismo negocio físico tiene un solo identificador maestro (golden record)",
        sql_expression=(
            f"address IS NULL OR {CLAVE_FISICA} NOT IN ("
            f"  SELECT {CLAVE_FISICA} FROM {S}.slv_business WHERE address IS NOT NULL"
            f"  GROUP BY 1 HAVING count(DISTINCT master_business_id) > 1)"),
        criticality="high", threshold_pct=99.0),
    Row(rule_id="DQ-013", target_table=f"{S}.slv_business", column_name="latitude", dimension="accuracy",
        description="Las coordenadas del negocio caen dentro del área de Philadelphia, la ciudad muestreada",
        sql_expression="latitude BETWEEN 39.80 AND 40.20 AND longitude BETWEEN -75.40 AND -74.90",
        criticality="medium", threshold_pct=99.0),
    Row(rule_id="DQ-014", target_table=f"{S}.slv_user", column_name="yelping_since", dimension="consistency",
        description="La fecha de alta del usuario no es posterior a su primera reseña",
        sql_expression=(
            "user_id NOT IN (SELECT u.user_id FROM "
            f"{S}.slv_user u JOIN (SELECT user_id, min(review_date) AS primera FROM {S}.slv_review GROUP BY user_id) r "
            "ON u.user_id = r.user_id WHERE u.yelping_since > r.primera)"),
        criticality="medium", threshold_pct=99.0),
    Row(rule_id="DQ-015", target_table=f"{S}.slv_review", column_name="loaded_at", dimension="timeliness",
        description="Los datos publicados provienen de la última carga de Bronze",
        sql_expression="loaded_at >= (SELECT max(_ingested_at) FROM workspace.yelp_bronze.brz_review)",
        criticality="high", threshold_pct=100.0),
]

spark.createDataFrame(reglas).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.yelp_gov.dq_rules")
display(spark.table("workspace.yelp_gov.dq_rules").orderBy("rule_id"))

# COMMAND ----------

# DBTITLE 1,3. Motor genérico de evaluación (con historial multi-corrida)
from pyspark.sql import functions as F

def evaluar_reglas():
    resultados = []
    for r in spark.table("workspace.yelp_gov.dq_rules").orderBy("rule_id").collect():
        total = spark.table(r.target_table).count()
        if r.dimension == "uniqueness" and r.column_name != "*":
            # Unicidad de una columna: valores distintos no nulos frente al total de filas.
            ok = spark.sql(f"SELECT count(DISTINCT {r.column_name}) c FROM {r.target_table}").first().c
        else:
            ok = spark.sql(f"SELECT count(*) c FROM {r.target_table} WHERE {r.sql_expression}").first().c
        pct = round(100.0 * ok / total, 2) if total > 0 else 0.0
        resultados.append((r.rule_id, r.target_table, int(total), int(ok), float(pct), bool(pct >= r.threshold_pct)))

    df = spark.createDataFrame(
        resultados,
        "rule_id STRING, target_table STRING, total_rows BIGINT, passed_rows BIGINT, compliance_pct DOUBLE, passed BOOLEAN"
    )
    df_actual = (df
        .withColumn("executed_at", F.current_timestamp())
        .select("executed_at", "rule_id", "target_table", "total_rows", "passed_rows", "compliance_pct", "passed"))
    df_actual.write.mode("append").saveAsTable("workspace.yelp_gov.dq_results")
    return df_actual

df_eval = evaluar_reglas()
display(df_eval)

# COMMAND ----------

# DBTITLE 1,4. Scorecard de calidad por dimensión (insumo del Dashboard E8 — Vista CDO)
# MAGIC %sql
# MAGIC SELECT r.dimension,
# MAGIC        count(*)                                      AS reglas,
# MAGIC        sum(CASE WHEN res.passed THEN 1 ELSE 0 END)     AS reglas_ok,
# MAGIC        sum(CASE WHEN NOT res.passed THEN 1 ELSE 0 END) AS reglas_en_rojo,
# MAGIC        round(avg(res.compliance_pct), 2)           AS cumplimiento_promedio
# MAGIC FROM workspace.yelp_gov.dq_results res
# MAGIC JOIN workspace.yelp_gov.dq_rules r USING (rule_id)
# MAGIC WHERE res.executed_at = (SELECT max(executed_at) FROM workspace.yelp_gov.dq_results)
# MAGIC GROUP BY r.dimension
# MAGIC ORDER BY cumplimiento_promedio;

# COMMAND ----------

# DBTITLE 1,5. Auditoría de reseñas rechazadas en cuarentena
# MAGIC %sql
# MAGIC SELECT rejection_reason,
# MAGIC        count(*)          AS resenas_rechazadas,
# MAGIC        min(review_date)  AS primera_fecha,
# MAGIC        max(review_date)  AS ultima_fecha,
# MAGIC        max(rejected_at)  AS ultima_auditoria
# MAGIC FROM workspace.yelp_silver.slv_review_quarantine
# MAGIC GROUP BY rejection_reason
# MAGIC ORDER BY resenas_rechazadas DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Decisión ante cada incumplimiento
# MAGIC
# MAGIC | Regla | Criticidad / Umbral | Decisión implementada |
# MAGIC | :--- | :--- | :--- |
# MAGIC | **DQ-001** review_id único | Crítica (100%) | **Cuarentena:** el `review_id` repetido se rechaza en `slv_review_quarantine` y se conserva la versión más reciente. |
# MAGIC | **DQ-002** mismo texto, usuario y negocio | Alta (99,5%) | **Marca en Gold:** la reseña entra a Silver y `gld_fact_review` la deja con `is_valid = false`; no cuenta en métricas oficiales. |
# MAGIC | **DQ-003 / DQ-004 / DQ-006** campos obligatorios, estrellas y negocio existente | Crítica (100%) | **Cuarentena + Quality Gate:** se rechazan con `rejection_reason`; si alguna llega a Silver, el pipeline se detiene. `CHECK chk_slv_review_stars` (E3) lo impide en el motor. |
# MAGIC | **DQ-005** fecha plausible | Alta (100%) | **Cuarentena** con `DQ-005: FECHA_INVALIDA`. |
# MAGIC | **DQ-007 / DQ-008** calificación y conteo declarados | Media (90% / 98%) | **Se reporta:** la diferencia se publica como `rating_gap` en `gld_agg_business_month`; no se corrige el dato declarado por Yelp. |
# MAGIC | **DQ-009 / DQ-010** check-ins | Alta (99,9% / 99%) | **Descarte / marca:** fechas inválidas se descartan; repetidos quedan con `is_duplicate = true` y no cuentan. |
# MAGIC | **DQ-011 / DQ-013** dirección y coordenadas | Alta (98%) / Media (99%) | **Aceptación:** los negocios sin dirección o fuera del área se conservan y se excluyen solo de los mapas. |
# MAGIC | **DQ-012** golden record | Alta (99%) | **Corrección en pipeline:** `master_business_id` unifica los registros del mismo negocio físico. |
# MAGIC | **DQ-014** fecha de alta vs. primera reseña | Media (99%) | **Corrección en pipeline:** se ajusta `yelping_since` a la fecha de la primera reseña. |
# MAGIC | **DQ-015** frescura | Alta (100%) | **Se reporta:** `loaded_at` en Silver y en los hechos Gold permite comprobar la carga de origen. |
# MAGIC
# MAGIC ---
# MAGIC ### Definition of Done (E5)
# MAGIC - [x] 15 reglas (`DQ-001` a `DQ-015`) con dimensión, criticidad y umbral.
# MAGIC - [x] Motor ejecutado con historial en `dq_results`.
# MAGIC - [x] Decisión documentada para cada regla (cuarentena, marca, corrección o aceptación).
# MAGIC - [x] Calidad preventiva: constraints `CHECK` declaradas en E3 (`chk_slv_review_stars`, `chk_slv_business_stars`, `chk_slv_friendship_order`).
# MAGIC
# MAGIC > Este notebook se incluye con `%run` desde `E7_pipeline_lakehouse/02_silver_limpieza`, que luego evalúa el Quality Gate.
