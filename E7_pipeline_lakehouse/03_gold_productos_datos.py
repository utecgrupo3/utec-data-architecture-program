# Databricks notebook source
# MAGIC %md
# MAGIC # E7.3 · Silver → Gold: Modelo Dimensional y Productos de Datos (Grupo 03 · Data Evolution)
# MAGIC **Cierra con:** M6 (construcción) + M9 (consumo)
# MAGIC
# MAGIC Gold materializa el **modelo dimensional de `yelp_modelo_logico.drawio` (pestaña Gold) / `yelp_gold.txt`**. Las tablas se crean en E3; aquí solo se cargan.
# MAGIC Gold trabaja con el **negocio maestro** (`business_key = master_business_id`) y solo las **reseñas válidas** (`is_valid = true`) cuentan en las métricas oficiales.
# MAGIC
# MAGIC | Producto de datos | Stakeholder | Pregunta de negocio que responde |
# MAGIC | :--- | :--- | :--- |
# MAGIC | `gld_dim_*`, `gld_bridge_business_category`, `gld_fact_review`, `gld_fact_checkin`, `gld_fact_tip` | **Data Science (Mei Lin) y BI** | Modelo estrella con dimensiones conformes y hechos atómicos |
# MAGIC | `gld_agg_business_month` | **Producto (Andrew Collins)** | ¿Cómo evolucionan cada mes la calificación válida, el volumen, la utilidad y la brecha con la calificación declarada de cada negocio? |
# MAGIC | `gld_agg_business_bayesian_month` | **Producto (Andrew Collins)** | ¿Qué negocios lideran el ranking sin el sesgo de los que tienen pocas reseñas? |
# MAGIC | `gld_agg_business_week`, `gld_agg_business_checkin_day` | **Ops Comerciales (Carla Romero)** | ¿Cuál es el conteo oficial de check-ins y tips por negocio, por semana y por día? |
# MAGIC | `gld_agg_category_month` | **Producto y Marketing** | ¿Cómo se comporta cada categoría principal mes a mes? |
# MAGIC | `gld_agg_review_health_month` | **Trust & Safety (Rafael Nakamura)** | ¿Qué proporción de reseñas se rechaza o se marca como duplicada cada mes? |
# MAGIC | `gld_agg_user_month`, `gld_agg_user_cohort_month` | **Producto (Andrew Collins)** | ¿Cuántos usuarios contribuyen cada mes y cuánto retiene cada cohorte? |

# COMMAND ----------

# DBTITLE 1,Inicialización: bitácora y helper de carga
import time
from pyspark.sql import functions as F

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

def cargar(sql: str, tabla: str):
    """Reemplaza el contenido de una tabla de E3 con el resultado de la consulta, respetando columnas y tipos."""
    t0 = time.time()
    esquema = spark.table(tabla).schema
    (spark.sql(sql)
        .select([F.col(c.name).cast(c.dataType).alias(c.name) for c in esquema])
        .write.insertInto(tabla, overwrite=True))
    registrar_bitacora("gold", tabla, spark.table(tabla).count(), t0)

# Vista temporal con el negocio maestro de cada business_id (insumo de todas las tablas Gold).
spark.sql("""
CREATE OR REPLACE TEMP VIEW v_master AS
SELECT business_id, master_business_id AS business_key
FROM workspace.yelp_silver.slv_business
""")

# COMMAND ----------

# DBTITLE 1,1. Dimensiones: fecha, categoría, puente negocio-categoría y negocio
cargar("""
SELECT
  CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key,
  d                                       AS full_date,
  year(d)                                 AS year,
  month(d)                                AS month,
  date_format(d, 'MMMM')                  AS month_name,
  quarter(d)                              AS quarter,
  CAST(date_trunc('week', d) AS DATE)     AS week_start,
  trunc(d, 'MM')                          AS month_start
FROM (SELECT explode(sequence(DATE'2004-01-01', DATE'2030-12-31', INTERVAL 1 DAY)) AS d)
""", "workspace.yelp_gold.gld_dim_date")

cargar("""
SELECT category_id AS category_key, name
FROM workspace.yelp_silver.slv_category
""", "workspace.yelp_gold.gld_dim_category")

# Categoría principal: la categoría del negocio con más negocios en la muestra; en empate, la primera alfabética.
cargar("""
WITH pares AS (
  SELECT DISTINCT m.business_key, bc.category_id AS category_key
  FROM workspace.yelp_silver.slv_business_category bc
  JOIN v_master m ON bc.business_id = m.business_id
),
popularidad AS (
  SELECT p.category_key, c.name, count(DISTINCT p.business_key) AS negocios
  FROM pares p JOIN workspace.yelp_silver.slv_category c ON p.category_key = c.category_id
  GROUP BY p.category_key, c.name
)
SELECT
  p.business_key,
  p.category_key,
  row_number() OVER (PARTITION BY p.business_key ORDER BY pop.negocios DESC, pop.name ASC) = 1 AS is_primary
FROM pares p JOIN popularidad pop ON p.category_key = pop.category_key
""", "workspace.yelp_gold.gld_bridge_business_category")

cargar("""
SELECT
  b.business_id AS business_key,
  b.name,
  b.city,
  b.state,
  c.name        AS primary_category,
  b.stars       AS declared_stars,
  b.is_open
FROM workspace.yelp_silver.slv_business b
LEFT JOIN workspace.yelp_gold.gld_bridge_business_category br
  ON br.business_key = b.business_id AND br.is_primary
LEFT JOIN workspace.yelp_gold.gld_dim_category c
  ON c.category_key = br.category_key
WHERE b.business_id = b.master_business_id
""", "workspace.yelp_gold.gld_dim_business")

# COMMAND ----------

# DBTITLE 1,2. Dimensión usuario
# La máscara PII de gld_dim_user.name se retira para recargar; la tarea E6 del job la vuelve a aplicar al final.
# slv_user.name llega sin máscara porque la tarea Silver la retiró en esta misma corrida.
tiene_mascara = (spark.table("workspace.information_schema.column_masks")
    .filter((F.col("table_schema") == "yelp_gold") & (F.col("table_name") == "gld_dim_user") & (F.col("column_name") == "name"))
    .count() > 0)
if tiene_mascara:
    spark.sql("ALTER TABLE workspace.yelp_gold.gld_dim_user ALTER COLUMN name DROP MASK")

cargar("""
SELECT
  user_id               AS user_key,
  name,
  yelping_since,
  fans,
  average_stars         AS avg_stars,
  elite_year_count > 0  AS is_elite
FROM workspace.yelp_silver.slv_user
""", "workspace.yelp_gold.gld_dim_user")

# COMMAND ----------

# DBTITLE 1,3. Hechos: reseña (is_valid, DQ-002), check-in y tip
# Una reseña no es válida si el mismo usuario ya publicó el mismo texto sobre el mismo negocio maestro (DQ-002);
# se conserva como válida la primera publicada.
cargar("""
WITH r AS (
  SELECT
    r.review_id, m.business_key, r.user_id AS user_key, r.review_date,
    r.stars, r.useful, r.funny, r.cool, r.loaded_at,
    CASE WHEN r.text IS NOT NULL AND length(trim(r.text)) > 0
         THEN row_number() OVER (
                PARTITION BY r.user_id, m.business_key, md5(lower(trim(r.text)))
                ORDER BY r.review_date, r.review_id)
         ELSE 1 END AS orden_texto
  FROM workspace.yelp_silver.slv_review r
  JOIN v_master m ON r.business_id = m.business_id
)
SELECT
  review_id,
  business_key,
  user_key,
  CAST(date_format(review_date, 'yyyyMMdd') AS INT) AS date_key,
  stars, useful, funny, cool, loaded_at,
  orden_texto = 1                                    AS is_valid,
  CASE WHEN orden_texto > 1 THEN 'DQ-002: mismo usuario, negocio y texto que una reseña anterior' END AS invalid_reason
FROM r
""", "workspace.yelp_gold.gld_fact_review")

cargar("""
SELECT
  c.checkin_id,
  m.business_key,
  CAST(date_format(c.checkin_at, 'yyyyMMdd') AS INT) AS date_key,
  hour(c.checkin_at)                                 AS checkin_hour,
  c.is_duplicate,
  c.loaded_at
FROM workspace.yelp_silver.slv_checkin c
JOIN v_master m ON c.business_id = m.business_id
""", "workspace.yelp_gold.gld_fact_checkin")

cargar("""
SELECT
  t.tip_id,
  t.user_id                                        AS user_key,
  m.business_key,
  CAST(date_format(t.tip_date, 'yyyyMMdd') AS INT) AS date_key,
  t.compliment_count,
  t.loaded_at
FROM workspace.yelp_silver.slv_tip t
JOIN v_master m ON t.business_id = m.business_id
""", "workspace.yelp_gold.gld_fact_tip")

# COMMAND ----------

# DBTITLE 1,4. Agregados del modelo dimensional
# Vistas base con el mes (AAAAMM) y la fecha de cada evento.
spark.sql("""
CREATE OR REPLACE TEMP VIEW v_review AS
SELECT f.*, d.full_date, d.week_start, CAST(date_format(d.full_date, 'yyyyMM') AS INT) AS period_month
FROM workspace.yelp_gold.gld_fact_review f JOIN workspace.yelp_gold.gld_dim_date d USING (date_key)
""")
spark.sql("""
CREATE OR REPLACE TEMP VIEW v_checkin AS
SELECT f.*, d.full_date, d.week_start, CAST(date_format(d.full_date, 'yyyyMM') AS INT) AS period_month
FROM workspace.yelp_gold.gld_fact_checkin f JOIN workspace.yelp_gold.gld_dim_date d USING (date_key)
WHERE NOT f.is_duplicate
""")
spark.sql("""
CREATE OR REPLACE TEMP VIEW v_tip AS
SELECT f.*, d.full_date, d.week_start, CAST(date_format(d.full_date, 'yyyyMM') AS INT) AS period_month
FROM workspace.yelp_gold.gld_fact_tip f JOIN workspace.yelp_gold.gld_dim_date d USING (date_key)
""")

cargar("""
WITH rev AS (
  SELECT business_key, period_month,
         count(*)                                                    AS review_count,
         avg(stars)                                                  AS avg_review_stars,
         count(DISTINCT user_key)                                    AS review_author_count,
         count_if(is_valid)                                          AS valid_review_count,
         avg(CASE WHEN is_valid THEN stars END)                      AS valid_rating,
         count_if(is_valid AND useful > 0)                           AS valid_useful,
         count_if(is_valid AND stars <= 2)                           AS valid_negative,
         count_if(NOT is_valid)                                      AS invalid_count,
         max(CASE WHEN is_valid THEN full_date END)                  AS last_valid_in_month
  FROM v_review GROUP BY business_key, period_month
),
chk AS (SELECT business_key, period_month, count(*) AS checkin_count FROM v_checkin GROUP BY 1, 2),
tip AS (SELECT business_key, period_month, count(*) AS tip_count FROM v_tip GROUP BY 1, 2),
meses AS (
  SELECT business_key, period_month FROM rev UNION
  SELECT business_key, period_month FROM chk UNION
  SELECT business_key, period_month FROM tip
),
base AS (
  SELECT
    m.business_key, m.period_month,
    coalesce(r.review_count, 0)          AS review_count,
    r.avg_review_stars,
    coalesce(r.review_author_count, 0)   AS review_author_count,
    coalesce(c.checkin_count, 0)         AS checkin_count,
    coalesce(t.tip_count, 0)             AS tip_count,
    coalesce(r.valid_review_count, 0)    AS valid_review_count,
    r.valid_rating, r.valid_useful, r.valid_negative, r.invalid_count,
    max(r.last_valid_in_month) OVER (PARTITION BY m.business_key ORDER BY m.period_month
                                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_valid_review
  FROM meses m
  LEFT JOIN rev r ON m.business_key = r.business_key AND m.period_month = r.period_month
  LEFT JOIN chk c ON m.business_key = c.business_key AND m.period_month = c.period_month
  LEFT JOIN tip t ON m.business_key = t.business_key AND m.period_month = t.period_month
)
SELECT
  b.business_key,
  b.period_month,
  b.review_count,
  round(b.avg_review_stars, 2)                                                AS avg_review_stars,
  b.review_author_count,
  b.checkin_count,
  b.tip_count,
  b.review_count + b.checkin_count + b.tip_count                              AS interaction_count,
  b.valid_review_count,
  round(b.valid_rating, 2)                                                    AS valid_rating,
  round(b.valid_useful / nullif(b.valid_review_count, 0), 4)                  AS useful_review_rate,
  datediff(last_day(to_date(CAST(b.period_month AS STRING), 'yyyyMM')), b.last_valid_review) AS days_since_last_review,
  round(d.declared_stars - b.valid_rating, 2)                                 AS rating_gap,
  round(b.valid_negative / nullif(b.valid_review_count, 0), 4)                AS negative_review_rate,
  round(b.invalid_count / nullif(b.review_count, 0), 4)                       AS invalid_content_rate,
  current_timestamp()                                                         AS calculated_at
FROM base b
JOIN workspace.yelp_gold.gld_dim_business d ON b.business_key = d.business_key
""", "workspace.yelp_gold.gld_agg_business_month")

cargar("""
WITH chk AS (SELECT business_key, week_start, count(*) AS checkin_count FROM v_checkin GROUP BY 1, 2),
     tip AS (SELECT business_key, week_start, count(*) AS tip_count FROM v_tip GROUP BY 1, 2)
SELECT
  coalesce(c.business_key, t.business_key) AS business_key,
  coalesce(c.week_start, t.week_start)     AS week_start,
  coalesce(c.checkin_count, 0)             AS checkin_count,
  coalesce(t.tip_count, 0)                 AS tip_count
FROM chk c FULL OUTER JOIN tip t ON c.business_key = t.business_key AND c.week_start = t.week_start
""", "workspace.yelp_gold.gld_agg_business_week")

cargar("""
WITH prim AS (
  SELECT business_key, category_key FROM workspace.yelp_gold.gld_bridge_business_category WHERE is_primary
),
negocios AS (SELECT category_key, count(*) AS business_count FROM prim GROUP BY category_key),
rev AS (
  SELECT p.category_key, r.period_month,
         count(*)                                 AS review_count,
         avg(r.stars)                             AS avg_review_stars,
         avg(CASE WHEN r.is_valid THEN r.stars END) AS valid_rating,
         count_if(r.is_valid AND r.stars <= 2)    AS negative_review_count
  FROM v_review r JOIN prim p ON r.business_key = p.business_key
  GROUP BY p.category_key, r.period_month
),
chk AS (
  SELECT p.category_key, c.period_month, count(*) AS checkin_count
  FROM v_checkin c JOIN prim p ON c.business_key = p.business_key
  GROUP BY p.category_key, c.period_month
),
meses AS (SELECT category_key, period_month FROM rev UNION SELECT category_key, period_month FROM chk)
SELECT
  m.category_key,
  m.period_month,
  n.business_count,
  coalesce(r.review_count, 0)          AS review_count,
  coalesce(c.checkin_count, 0)         AS checkin_count,
  round(r.avg_review_stars, 2)         AS avg_review_stars,
  round(r.valid_rating, 2)             AS valid_rating,
  coalesce(r.negative_review_count, 0) AS negative_review_count
FROM meses m
JOIN negocios n ON m.category_key = n.category_key
LEFT JOIN rev r ON m.category_key = r.category_key AND m.period_month = r.period_month
LEFT JOIN chk c ON m.category_key = c.category_key AND m.period_month = c.period_month
""", "workspace.yelp_gold.gld_agg_category_month")

cargar("""
WITH rev AS (SELECT user_key, period_month, count(*) AS review_count FROM v_review GROUP BY 1, 2),
     tip AS (SELECT user_key, period_month, count(*) AS tip_count FROM v_tip GROUP BY 1, 2)
SELECT
  coalesce(r.user_key, t.user_key)             AS user_key,
  coalesce(r.period_month, t.period_month)     AS period_month,
  coalesce(r.review_count, 0)                  AS review_count,
  coalesce(t.tip_count, 0)                     AS tip_count,
  coalesce(r.review_count, 0) + coalesce(t.tip_count, 0)     AS contribution_count,
  coalesce(r.review_count, 0) + coalesce(t.tip_count, 0) > 0 AS is_contributor
FROM rev r FULL OUTER JOIN tip t ON r.user_key = t.user_key AND r.period_month = t.period_month
""", "workspace.yelp_gold.gld_agg_user_month")

# COMMAND ----------

# DBTITLE 1,5. Agregados de KPIs por stakeholder (ranking bayesiano, check-ins diarios, salud de reseñas, cohortes)
# Calificación bayesiana con m = 25 reseñas y el promedio válido de la ciudad como referencia.
cargar("""
WITH validas AS (
  SELECT r.business_key, r.period_month, r.stars, r.user_key, d.city
  FROM v_review r JOIN workspace.yelp_gold.gld_dim_business d ON r.business_key = d.business_key
  WHERE r.is_valid
),
ciudad AS (SELECT city, avg(stars) AS c FROM validas GROUP BY city),
mes AS (
  SELECT business_key, city, period_month,
         count(*) AS n, avg(stars) AS r, count(DISTINCT user_key) AS autores
  FROM validas GROUP BY business_key, city, period_month
)
SELECT
  m.business_key,
  m.period_month,
  m.n                                                         AS valid_review_count,
  round(m.r, 2)                                               AS valid_rating,
  round((m.n * m.r + 25.0 * coalesce(c.c, m.r)) / (m.n + 25.0), 2) AS bayesian_rating,
  m.autores                                                   AS review_author_count
FROM mes m LEFT JOIN ciudad c ON m.city <=> c.city
""", "workspace.yelp_gold.gld_agg_business_bayesian_month")

cargar("""
SELECT
  business_key,
  date_key,
  count(*)                      AS checkin_count,
  count(DISTINCT checkin_hour)  AS active_hour_count
FROM v_checkin
GROUP BY business_key, date_key
""", "workspace.yelp_gold.gld_agg_business_checkin_day")

cargar("""
WITH rev AS (
  SELECT period_month, count_if(is_valid) AS valid_review_count, count_if(NOT is_valid) AS invalid_review_count
  FROM v_review GROUP BY period_month
),
cua AS (
  SELECT CAST(date_format(coalesce(review_date, CAST(rejected_at AS DATE)), 'yyyyMM') AS INT) AS period_month,
         count(*) AS quarantined_review_count
  FROM workspace.yelp_silver.slv_review_quarantine GROUP BY 1
)
SELECT
  coalesce(r.period_month, q.period_month)        AS period_month,
  coalesce(r.valid_review_count, 0)               AS valid_review_count,
  coalesce(r.invalid_review_count, 0)             AS invalid_review_count,
  coalesce(q.quarantined_review_count, 0)         AS quarantined_review_count,
  round(coalesce(q.quarantined_review_count, 0) /
        nullif(coalesce(r.valid_review_count, 0) + coalesce(r.invalid_review_count, 0) + coalesce(q.quarantined_review_count, 0), 0), 4)
                                                  AS quarantine_rate
FROM rev r FULL OUTER JOIN cua q ON r.period_month = q.period_month
WHERE coalesce(r.period_month, q.period_month) IS NOT NULL
""", "workspace.yelp_gold.gld_agg_review_health_month")

cargar("""
WITH usuarios AS (
  SELECT user_key, trunc(yelping_since, 'MM') AS cohort_start
  FROM workspace.yelp_gold.gld_dim_user WHERE yelping_since IS NOT NULL
),
tamano AS (SELECT cohort_start, count(*) AS cohort_size FROM usuarios GROUP BY cohort_start),
actividad AS (
  SELECT u.cohort_start, trunc(r.full_date, 'MM') AS activity_start, count(DISTINCT r.user_key) AS active_user_count
  FROM v_review r JOIN usuarios u ON r.user_key = u.user_key
  WHERE r.is_valid AND trunc(r.full_date, 'MM') >= u.cohort_start
  GROUP BY 1, 2
)
SELECT
  CAST(date_format(a.cohort_start, 'yyyyMM') AS INT)    AS cohort_month,
  CAST(date_format(a.activity_start, 'yyyyMM') AS INT)  AS activity_month,
  CAST(months_between(a.activity_start, a.cohort_start) AS INT) AS months_since_join,
  t.cohort_size,
  a.active_user_count,
  round(a.active_user_count / t.cohort_size, 4)         AS retention_rate
FROM actividad a JOIN tamano t ON a.cohort_start = t.cohort_start
""", "workspace.yelp_gold.gld_agg_user_cohort_month")

# COMMAND ----------

# DBTITLE 1,Validación — integridad de claves y 100% de columnas con COMMENT
huerfanos = spark.sql("""
SELECT
  (SELECT count(*) FROM workspace.yelp_gold.gld_fact_review f LEFT ANTI JOIN workspace.yelp_gold.gld_dim_business d USING (business_key)) AS review_sin_negocio,
  (SELECT count(*) FROM workspace.yelp_gold.gld_fact_review f LEFT ANTI JOIN workspace.yelp_gold.gld_dim_user u USING (user_key))      AS review_sin_usuario,
  (SELECT count(*) FROM workspace.yelp_gold.gld_fact_review f LEFT ANTI JOIN workspace.yelp_gold.gld_dim_date d USING (date_key))      AS review_sin_fecha,
  (SELECT count(*) FROM workspace.yelp_gold.gld_fact_checkin f LEFT ANTI JOIN workspace.yelp_gold.gld_dim_business d USING (business_key)) AS checkin_sin_negocio,
  (SELECT count(*) FROM workspace.yelp_gold.gld_fact_tip f LEFT ANTI JOIN workspace.yelp_gold.gld_dim_user u USING (user_key))         AS tip_sin_usuario
""").first().asDict()
assert all(v == 0 for v in huerfanos.values()), f"❌ Hechos sin dimensión: {huerfanos}"

sin_comment_gold = spark.sql("""
  SELECT table_name, column_name
  FROM workspace.information_schema.columns
  WHERE table_schema = 'yelp_gold' AND table_name LIKE 'gld\\_%' AND (comment IS NULL OR comment = '')
""").collect()
assert not sin_comment_gold, f"❌ Columnas gold sin COMMENT: {[(r.table_name, r.column_name) for r in sin_comment_gold]}"

tablas_gold = sorted(r.tableName for r in spark.sql("SHOW TABLES IN workspace.yelp_gold").collect() if r.tableName.startswith("gld_"))
print(f"✔ Capa gold validada ({len(tablas_gold)} tablas): {tablas_gold}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definition of Done (E7.3)
# MAGIC - [x] Modelo estrella completo según `yelp_gold.txt`: 4 dimensiones, 1 puente, 3 hechos y 4 agregados.
# MAGIC - [x] 4 agregados de KPIs por stakeholder: `gld_agg_business_bayesian_month`, `gld_agg_business_checkin_day`, `gld_agg_review_health_month`, `gld_agg_user_cohort_month`.
# MAGIC - [x] Hechos sin claves huérfanas y 100% de columnas con `COMMENT`, `PRIMARY KEY` y `FOREIGN KEY`.
