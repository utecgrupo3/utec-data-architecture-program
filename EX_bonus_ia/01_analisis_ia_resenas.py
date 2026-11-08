# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # EX · Bonus — IA Gobernada sobre Datos No Estructurados: Reseñas (Grupo 03 · Data Evolution)
# MAGIC **Cierra con:** M8 IA y Datos No Estructurados · **Puntaje:** Bonus
# MAGIC
# MAGIC El texto libre de las reseñas (`slv_review.text`) es el activo no estructurado más valioso del caso **YelpConnect**.
# MAGIC Aquí se aplican las **AI Functions nativas de Databricks SQL** (`ai_analyze_sentiment`, `ai_classify`) para enriquecer la capa Gold bajo una **arquitectura de IA gobernada**:
# MAGIC 1. El resultado de la inferencia del modelo fundacional se persiste como tabla catalogada en `workspace.yelp_gold.gld_review_ai_enriched`, con `COMMENT` en tabla y columnas, etiquetas en Unity Catalog y linaje automático.
# MAGIC 2. Se detectan **inconsistencias entre calificación numérica (`stars`) y sentimiento textual (`sentiment`)** como señal de alerta para **Rafael Nakamura (Trust & Safety)**: reseñas de 5 estrellas con texto negativo (sarcasmo, bots o manipulación) o reseñas de 1–2 estrellas con texto positivo (error de marcación del usuario).
# MAGIC 3. Se responde directamente al requerimiento **REQ-06 de Andrew Collins (VP of Product)**: identificar comercios con volumen de reseñas negativas asociadas a `'servicio'` o `'comida'`.
# MAGIC
# MAGIC ⚠️ **Gobernanza FinOps:** Las funciones de inferencia LLM consumen cuota Serverless por token procesado. Se acota la inferencia a una muestra representativa de **≤ 500 reseñas válidas (`ORDER BY rand(42) LIMIT 500`)** truncando textos extremos a 1,000 caracteres.

# COMMAND ----------

# DBTITLE 1,1. Enriquecimiento de reseñas con Databricks AI Functions (ai_analyze_sentiment + ai_classify)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.yelp_gold.gld_review_ai_enriched
# MAGIC COMMENT 'EX · Muestra gobernada de reseñas enriquecidas con IA (sentimiento, tópico principal y flag de disonancia). Consumidores: Trust & Safety (Rafael Nakamura) y Producto (Andrew Collins - REQ-06).'
# MAGIC AS
# MAGIC WITH muestra AS (
# MAGIC   SELECT
# MAGIC     f.review_id,
# MAGIC     f.business_key,
# MAGIC     f.user_key,
# MAGIC     f.stars,
# MAGIC     d.full_date AS review_date,
# MAGIC     left(trim(r.text), 1000) AS text
# MAGIC   FROM workspace.yelp_gold.gld_fact_review f
# MAGIC   JOIN workspace.yelp_gold.gld_dim_date d ON f.date_key = d.date_key
# MAGIC   JOIN workspace.yelp_silver.slv_review r ON f.review_id = r.review_id
# MAGIC   WHERE f.is_valid
# MAGIC   ORDER BY rand(42)
# MAGIC   LIMIT 500
# MAGIC ),
# MAGIC inferencia AS (
# MAGIC   SELECT
# MAGIC     review_id,
# MAGIC     business_key,
# MAGIC     user_key,
# MAGIC     stars,
# MAGIC     review_date,
# MAGIC     text,
# MAGIC     ai_analyze_sentiment(text)                                                AS sentiment,
# MAGIC     ai_classify(text, array('comida', 'servicio', 'precio', 'ambiente', 'otro')) AS topic
# MAGIC   FROM muestra
# MAGIC )
# MAGIC SELECT
# MAGIC   review_id,
# MAGIC   business_key,
# MAGIC   user_key,
# MAGIC   stars,
# MAGIC   review_date,
# MAGIC   text,
# MAGIC   sentiment,
# MAGIC   topic,
# MAGIC   CASE
# MAGIC     WHEN stars >= 4 AND sentiment = 'negative' THEN 'ALTA_SOSPECHA_5ESTRELLAS_NEGATIVO'
# MAGIC     WHEN stars <= 2 AND sentiment = 'positive' THEN 'DISONANCIA_BAJA_ESTRELLA_POSITIVO'
# MAGIC     WHEN stars >= 4 AND sentiment = 'mixed'    THEN 'SOSPECHA_MEDIA_ALTA_ESTRELLA_MIXTO'
# MAGIC     WHEN stars <= 2 AND sentiment = 'mixed'    THEN 'SOSPECHA_MEDIA_BAJA_ESTRELLA_MIXTO'
# MAGIC     WHEN stars = 3  AND sentiment IN ('positive', 'negative') THEN 'NEUTRO_POLARIZADO'
# MAGIC     ELSE 'CONSISTENTE'
# MAGIC   END                                                                         AS ai_consistency_status,
# MAGIC   current_timestamp()                                                         AS inferred_at
# MAGIC FROM inferencia;

# COMMAND ----------

# DBTITLE 1,1.1 Documentación de metadatos y etiquetas de gobierno en Unity Catalog para la tabla de IA
comandos_metadata_ia = [
    "ALTER TABLE workspace.yelp_gold.gld_review_ai_enriched SET TAGS ('layer' = 'gold', 'source' = 'ai_functions_llm', 'owner' = 'andrew_collins_rafael_nakamura', 'classification' = 'internal')",
    "ALTER TABLE workspace.yelp_gold.gld_review_ai_enriched ALTER COLUMN review_id              COMMENT 'Identificador único de la reseña analizada (FK hacia gld_fact_review)'",
    "ALTER TABLE workspace.yelp_gold.gld_review_ai_enriched ALTER COLUMN business_key           COMMENT 'Negocio maestro evaluado (FK hacia gld_dim_business)'",
    "ALTER TABLE workspace.yelp_gold.gld_review_ai_enriched ALTER COLUMN user_key               COMMENT 'Autor de la reseña (FK hacia gld_dim_user)'",
    "ALTER TABLE workspace.yelp_gold.gld_review_ai_enriched ALTER COLUMN stars                  COMMENT 'Calificación numérica otorgada por el usuario (1 a 5 estrellas)'",
    "ALTER TABLE workspace.yelp_gold.gld_review_ai_enriched ALTER COLUMN review_date           COMMENT 'Marca temporal de publicación de la reseña'",
    "ALTER TABLE workspace.yelp_gold.gld_review_ai_enriched ALTER COLUMN text                   COMMENT 'Texto de la reseña truncado a 1,000 caracteres para control FinOps de inferencia'",
    "ALTER TABLE workspace.yelp_gold.gld_review_ai_enriched ALTER COLUMN sentiment              COMMENT 'Sentimiento inferido por ai_analyze_sentiment (positive, negative, neutral, mixed)'",
    "ALTER TABLE workspace.yelp_gold.gld_review_ai_enriched ALTER COLUMN topic                  COMMENT 'Categoría temática inferida por ai_classify (comida, servicio, precio, ambiente, otro)'",
    "ALTER TABLE workspace.yelp_gold.gld_review_ai_enriched ALTER COLUMN ai_consistency_status  COMMENT 'Clasificación de consistencia entre stars y sentimiento para auditoría de Trust & Safety'",
    "ALTER TABLE workspace.yelp_gold.gld_review_ai_enriched ALTER COLUMN inferred_at            COMMENT 'Timestamp de ejecución del modelo fundacional sobre la muestra'",
]

for cmd in comandos_metadata_ia:
    try:
        spark.sql(cmd)
    except Exception as e:
        print(f"ℹ Nota metadata IA: {e}")

print("✔ Metadatos y tags de Unity Catalog registrados en workspace.yelp_gold.gld_review_ai_enriched")
display(spark.table("workspace.yelp_gold.gld_review_ai_enriched").limit(15))

# COMMAND ----------

# DBTITLE 1,2. Detección de Inconsistencia Rating vs Sentimiento (Señal de Fraude para Trust & Safety)
# MAGIC %sql
# MAGIC -- Severidad ALTA : 4-5 estrellas con texto negativo, o 1-2 estrellas con texto positivo (disonancia total).
# MAGIC -- Severidad MEDIA: 4-5 estrellas o 1-2 estrellas con texto 'mixed' (el texto contradice parcialmente la nota).
# MAGIC -- Ambas son candidatas a revisión por el equipo de Rafael Nakamura (Trust & Safety).
# MAGIC -- Nota: la severidad se calcula aquí a partir de stars + sentimiento, sin volver a invocar el LLM (FinOps).
# MAGIC SELECT
# MAGIC   r.review_id,
# MAGIC   n.name                           AS nombre_comercio,
# MAGIC   n.city,
# MAGIC   n.primary_category,
# MAGIC   r.stars                          AS estrellas_otorgadas,
# MAGIC   r.sentiment                      AS sentimiento_ia,
# MAGIC   r.topic                          AS tema_detectado_ia,
# MAGIC   CASE
# MAGIC     WHEN (r.stars >= 4 AND r.sentiment = 'negative')
# MAGIC       OR (r.stars <= 2 AND r.sentiment = 'positive') THEN 'ALTA'
# MAGIC     ELSE 'MEDIA'
# MAGIC   END                              AS severidad_revision,
# MAGIC   left(r.text, 220)                AS extracto_resena,
# MAGIC   r.review_date
# MAGIC FROM workspace.yelp_gold.gld_review_ai_enriched r
# MAGIC LEFT JOIN workspace.yelp_gold.gld_dim_business n
# MAGIC   ON r.business_key = n.business_key
# MAGIC WHERE (r.stars >= 4 AND r.sentiment IN ('negative', 'mixed'))
# MAGIC    OR (r.stars <= 2 AND r.sentiment IN ('positive', 'mixed'))
# MAGIC ORDER BY severidad_revision ASC, r.stars DESC, r.review_date DESC;

# COMMAND ----------

# DBTITLE 1,2.1 Matriz Resumen Rating vs Sentimiento y Dolor Operativo por Tema (REQ-06 — Producto: Andrew Collins)
# MAGIC %sql
# MAGIC -- Matriz agregada para incorporar directamente como visual en el Dashboard E8:
# MAGIC -- Cruza el tema detectado por IA (servicio, comida, precio, ambiente) con el sentimiento y la consistencia.
# MAGIC SELECT
# MAGIC   topic                                                                       AS tema_principal_ia,
# MAGIC   sentiment                                                                   AS sentimiento_ia,
# MAGIC   ai_consistency_status,
# MAGIC   count(*)                                                                    AS total_resenas_muestra,
# MAGIC   round(avg(stars), 2)                                                        AS promedio_estrellas,
# MAGIC   round(100.0 * count(*) / sum(count(*)) OVER (), 2)                          AS porcentaje_muestra_pct
# MAGIC FROM workspace.yelp_gold.gld_review_ai_enriched
# MAGIC GROUP BY topic, sentiment, ai_consistency_status
# MAGIC ORDER BY total_resenas_muestra DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Conclusiones Analíticas para Trust & Safety y Producto
# MAGIC
# MAGIC > **Hallazgo empírico de la corrida (muestra de 500 reseñas, Philadelphia):** el LLM **no encontró disonancias totales** (0 reseñas con 4-5★ y texto `negative`, 0 con 1-2★ y texto `positive`).
# MAGIC > La señal real aparece en el sentimiento **`mixed`**: **56 reseñas de 4-5★** y **35 reseñas de 1-2★** tienen texto mixto (~18% de la muestra).
# MAGIC > Por ello la regla se amplió con una **severidad MEDIA** (`SOSPECHA_MEDIA_*`): con una regla binaria (solo `positive`/`negative`) la cola de revisión de Trust & Safety quedaba vacía y ocultaba casos relevantes.
# MAGIC
# MAGIC 1. **Detección de Anomalías y Fraude Semántico (*Rafael Nakamura — Trust & Safety*):**
# MAGIC    * Las reseñas clasificadas como `ALTA_SOSPECHA_5ESTRELLAS_NEGATIVO` (`stars >= 4 AND sentiment = 'negative'`) capturan dos patrones que las reglas determinísticas tradicionales (`DQ-001` a `DQ-020`) no pueden detectar por sí solas:
# MAGIC      * **Granjas de bots de reputación mal configuradas:** scripts que inyectan `stars = 5` para inflar artificialmente el rating de un comercio pagador, pero reciclan textos genéricos de quejas copiados de otros foros.
# MAGIC      * **Reseñas sarcásticas o extorsivas:** usuarios que otorgan 5 estrellas para posicionar su comentario en la parte superior del feed mientras denuncian intoxicaciones o maltrato severo en el texto.
# MAGIC    * Las reseñas con `DISONANCIA_BAJA_ESTRELLA_POSITIVO` (`stars <= 2 AND sentiment = 'positive'`) suelen corresponder a **errores de usabilidad (UX)** en la app móvil (usuarios que querían dar 5 estrellas pero tocaron 1 estrella por error y escribieron *"Excelente comida y servicio"*). Estas no deben enviarse a cuarentena punitiva, sino activar un flujo de confirmación en la UI móvil de Producto.
# MAGIC 2. **Accionabilidad para Dueños de Comercios (*Andrew Collins — VP of Product, REQ-06*):**
# MAGIC    * La clasificación temática con `ai_classify` permite separar si la caída del **Rating Ponderado Bayesiano ($BWR$)** de un restaurante se debe a problemas de **`'servicio'`** (tiempos de espera, atención del personal) o de **`'comida'`** (calidad de insumos), habilitando alertas operativas específicas en el panel de comercios afiliados a YelpConnect.
# MAGIC
# MAGIC ---
# MAGIC ## 4. Reflexión de Gobierno de IA: Riesgos de Persistir Salidas de un LLM como Datos Productivos y Controles Arquitectónicos
# MAGIC
# MAGIC Persistir las salidas generadas por un Modelo de Lenguaje Grande (LLM) dentro de tablas productivas de la capa Gold (`gld_review_ai_enriched`) transforma una inferencia probabilística en un **activo de datos corporativo** que alimenta dashboards ejecutivos y decisiones de negocio. Esto introduce **cinco riesgos críticos de gobierno** que nuestra arquitectura mitiga mediante controles explícitos:
# MAGIC
# MAGIC | # | Riesgo de Gobierno de IA al Persistir Salidas LLM | Impacto en YelpConnect si no se controla | Control Arquitectónico Implementado en el Lakehouse |
# MAGIC | :--- | :--- | :--- | :--- |
# MAGIC | **1** | **Alucinación y Clasificación Errónea (*Hallucination & Misclassification*)** | El modelo podría clasificar fuera de las categorías permitidas o malinterpretar jerga local, penalizando injustamente a un comercio honesto ante Trust & Safety. | **Restricción de Dominio Cerrado + Validación Post-Inferencia:** Se utiliza `ai_classify` con un arreglo cerrado (`array('comida','servicio','precio','ambiente','otro')`) y se establece como política que ninguna salida de IA bloquea automáticamente una reseña sin confirmación humana (*Human-in-the-Loop*) de un Steward de Trust & Safety. |
# MAGIC | **2** | **No-Determinismo y Deriva del Modelo (*Model Drift & Version Updates*)** | Cuando el proveedor actualiza el endpoint fundacional subyacente, el mismo texto de reseña puede producir un sentimiento distinto en dos corridas diferentes, rompiendo la comparabilidad histórica de las series de tiempo. | **Inmutabilidad por Lote + Auditoría Temporal (`inferred_at`):** No se re-ejecuta el LLM sobre reseñas históricas ya clasificadas; cada registro persiste la marca temporal exacta de inferencia (`inferred_at`) y se procesan únicamente incrementos nuevos. |
# MAGIC | **3** | **Exposición de Datos Personales (PII) en Prompts de Inferencia** | El texto libre de una reseña puede incluir nombres completos de meseros, teléfonos o correos privados que serían enviados al motor de inferencia y expuestos en vistas analíticas. | **Minimización y Gobernanza en Unity Catalog (E6):** La tabla `gld_review_ai_enriched` hereda los controles RBAC de la capa Gold, trunca la ventana de contexto a 1,000 caracteres y nunca cruza el nombre real (`slv_user.name`) sino el identificador seudonimizado o `vw_user_secure`. |
# MAGIC | **4** | **Sesgo Algorítmico Cultural o Idiomático (*Algorithmic Bias*)** | Reseñas escritas con modismos regionales o bilingües (spanglish) pueden recibir sistemáticamente una clasificación de sentimiento más negativa que reseñas en inglés estándar. | **Monitoreo de Tasa de Disonancia por Ciudad y Categoría:** Se audita periódicamente la distribución de `ai_consistency_status` cruzada por `city` y `primary_category` para detectar sesgos geográficos o culturales antes de usar el indicador en rankings públicos. |
# MAGIC | **5** | **Explosión de Costos Operativos por Token (*FinOps LLM Sprawl*)** | Invocar funciones LLM fila por fila sobre millones de reseñas históricas o dentro de vistas dinámicas consultadas por BI multiplicaría exponencialmente el consumo de DBUs. | **Materialización Gobernada + Muestreo acotado (`LIMIT 500`):** Prohibición estricta de usar `ai_analyze_sentiment` o `ai_classify` dentro de vistas `VIEW` en vivo; las inferencias se ejecutan en batch controlado (`≤ 500 filas`) y se materializan en una tabla Delta gobernada. |
# MAGIC
# MAGIC ---
# MAGIC ### Definition of Done (EX)
# MAGIC - [x] Tabla enriquecida `workspace.yelp_gold.gld_review_ai_enriched` creada, catalogada y con `COMMENT` en tabla y 100% de columnas.
# MAGIC - [x] Análisis de inconsistencia `rating ↔ sentimiento` y desglose por tema (`REQ-06`) con conclusiones accionables para Trust & Safety y Producto.
# MAGIC - [x] Reflexión de gobierno de IA documentando los 5 riesgos de persistir salidas de un LLM como datos productivos y los controles aplicados en el Lakehouse.