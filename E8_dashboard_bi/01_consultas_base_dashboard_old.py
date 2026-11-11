# Databricks notebook source
# MAGIC %md
# MAGIC # E8 · Dashboard de BI (AI/BI Dashboard + Genie Space) — Grupo 03 (Data Evolution)
# MAGIC **Cierra con:** M9 BI y Visualización
# MAGIC
# MAGIC El entregable comprende **DOS artefactos en el workspace**:
# MAGIC 1. **AI/BI Dashboard** *"Plataforma de Datos YelpConnect — Grupo 03 (Data Evolution)"* con 2 páginas:
# MAGIC    - **Página 1 · Negocio (Producto — Andrew Collins, Ops Comerciales — Carla Romero, Trust & Safety — Rafael Nakamura):** calificación válida y bayesiana, crecimiento de reseñas, check-ins oficiales, retención de cohortes y salud de reseñas.
# MAGIC    - **Página 2 · Gobierno y Operaciones (Chief Data Officer — Laura Hernández):** scorecard de calidad (**E5**), cobertura de diccionario y glosario (**E4**), ownership RACI (**E6**) y observabilidad del pipeline (**E7**).
# MAGIC 2. **Genie Space** sobre `workspace.yelp_gold`, con las instrucciones y sinónimos del Glosario de Negocio (**E4**).
# MAGIC
# MAGIC **Regla de arquitectura:** el dashboard consume **solo** `workspace.yelp_gold` y `workspace.yelp_gov` (nunca `yelp_silver` ni `yelp_bronze`).
# MAGIC
# MAGIC Convención de Gold: los meses se guardan como `period_month` entero `AAAAMM`; para graficar se convierten con `to_date(CAST(period_month AS STRING), 'yyyyMM')`.

# COMMAND ----------

# DBTITLE 1,Q1 — Evolución mensual de reseñas, calificación válida y bayesiana (REQ-01 · Producto: Andrew Collins)
# MAGIC %sql
# MAGIC WITH mensual AS (
# MAGIC   SELECT
# MAGIC     to_date(CAST(a.period_month AS STRING), 'yyyyMM')   AS mes,
# MAGIC     sum(a.valid_review_count)                           AS resenas_validas,
# MAGIC     sum(a.review_count)                                 AS resenas_totales,
# MAGIC     sum(a.review_author_count)                          AS autores_mes,
# MAGIC     round(sum(a.valid_rating * a.valid_review_count) / nullif(sum(a.valid_review_count), 0), 2) AS calificacion_valida
# MAGIC   FROM workspace.yelp_gold.gld_agg_business_month a
# MAGIC   GROUP BY a.period_month
# MAGIC ),
# MAGIC bayes AS (
# MAGIC   SELECT to_date(CAST(period_month AS STRING), 'yyyyMM') AS mes, round(avg(bayesian_rating), 2) AS calificacion_bayesiana
# MAGIC   FROM workspace.yelp_gold.gld_agg_business_bayesian_month
# MAGIC   GROUP BY period_month
# MAGIC )
# MAGIC SELECT
# MAGIC   m.mes,
# MAGIC   m.resenas_validas,
# MAGIC   m.resenas_totales,
# MAGIC   m.autores_mes,
# MAGIC   m.calificacion_valida,
# MAGIC   b.calificacion_bayesiana,
# MAGIC   round(100.0 * (m.resenas_validas - lag(m.resenas_validas) OVER (ORDER BY m.mes))
# MAGIC         / nullif(lag(m.resenas_validas) OVER (ORDER BY m.mes), 0), 2) AS crecimiento_neto_pct
# MAGIC FROM mensual m LEFT JOIN bayes b ON m.mes = b.mes
# MAGIC ORDER BY m.mes;

# COMMAND ----------

# DBTITLE 1,Q2 — Scorecard de calidad de datos por dimensión (Gobierno · CDO: Laura Hernández)
# MAGIC %sql
# MAGIC SELECT
# MAGIC   r.dimension                                    AS dimension_calidad,
# MAGIC   count(*)                                       AS total_reglas,
# MAGIC   sum(CASE WHEN res.passed THEN 1 ELSE 0 END)      AS reglas_en_verde,
# MAGIC   sum(CASE WHEN NOT res.passed THEN 1 ELSE 0 END)  AS reglas_en_rojo,
# MAGIC   round(avg(res.compliance_pct), 2)            AS cumplimiento_promedio_pct,
# MAGIC   min(res.compliance_pct)                      AS cumplimiento_minimo_pct
# MAGIC FROM workspace.yelp_gov.dq_results res
# MAGIC JOIN workspace.yelp_gov.dq_rules r USING (rule_id)
# MAGIC WHERE res.executed_at = (SELECT max(executed_at) FROM workspace.yelp_gov.dq_results)
# MAGIC GROUP BY r.dimension
# MAGIC ORDER BY cumplimiento_promedio_pct DESC;

# COMMAND ----------

# DBTITLE 1,Q3 — Indicadores ejecutivos de gobierno: E4 + E5 + E6 (Gobierno · CDO: Laura Hernández)
# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT round(100.0 * sum(CASE WHEN business_description IS NOT NULL AND length(trim(business_description)) > 0 THEN 1 ELSE 0 END) / count(*), 1)
# MAGIC      FROM workspace.yelp_gov.data_dictionary)                                            AS pct_columnas_documentadas,
# MAGIC   (SELECT count(*) FROM workspace.yelp_gov.data_dictionary)                              AS total_atributos_diccionario,
# MAGIC   (SELECT count(*) FROM workspace.yelp_gov.raci_matrix)                                    AS entidades_con_owner_raci,
# MAGIC   (SELECT count(*) FROM workspace.yelp_gov.business_glossary)                               AS terminos_glosario_aprobados,
# MAGIC   (SELECT count(*) FROM workspace.yelp_gov.dq_rules)                                      AS reglas_dq_activas,
# MAGIC   (SELECT round(avg(compliance_pct), 2)
# MAGIC      FROM workspace.yelp_gov.dq_results
# MAGIC     WHERE executed_at = (SELECT max(executed_at) FROM workspace.yelp_gov.dq_results)) AS indice_global_calidad_pct;

# COMMAND ----------

# DBTITLE 1,Q4 — Top negocios por calificación bayesiana y utilidad (REQ-01 / REQ-02 · Producto: Andrew Collins)
# MAGIC %sql
# MAGIC WITH votos AS (
# MAGIC   SELECT business_key, sum(useful) AS votos_utiles
# MAGIC   FROM workspace.yelp_gold.gld_fact_review
# MAGIC   WHERE is_valid
# MAGIC   GROUP BY business_key
# MAGIC ),
# MAGIC negocio AS (
# MAGIC   SELECT business_key,
# MAGIC          sum(valid_review_count)                                                       AS resenas_validas,
# MAGIC          round(sum(valid_rating * valid_review_count) / nullif(sum(valid_review_count), 0), 2) AS calificacion_valida,
# MAGIC          round(avg(bayesian_rating), 2)                                                AS calificacion_bayesiana
# MAGIC   FROM workspace.yelp_gold.gld_agg_business_bayesian_month
# MAGIC   GROUP BY business_key
# MAGIC )
# MAGIC SELECT
# MAGIC   d.business_key,
# MAGIC   d.name                 AS negocio,
# MAGIC   d.city,
# MAGIC   d.primary_category,
# MAGIC   d.is_open,
# MAGIC   d.declared_stars,
# MAGIC   n.resenas_validas,
# MAGIC   n.calificacion_valida,
# MAGIC   n.calificacion_bayesiana,
# MAGIC   coalesce(v.votos_utiles, 0)                                         AS votos_utiles,
# MAGIC   round(coalesce(v.votos_utiles, 0) / nullif(n.resenas_validas, 0), 2) AS utilidad_por_resena
# MAGIC FROM negocio n
# MAGIC JOIN workspace.yelp_gold.gld_dim_business d ON n.business_key = d.business_key
# MAGIC LEFT JOIN votos v ON n.business_key = v.business_key
# MAGIC WHERE n.resenas_validas >= 5
# MAGIC ORDER BY calificacion_bayesiana DESC, resenas_validas DESC
# MAGIC LIMIT 25;

# COMMAND ----------

# DBTITLE 1,Q5 — Check-ins oficiales por mes y categoría principal (REQ-02 / REQ-05 · Ops Comerciales: Carla Romero)
# MAGIC %sql
# MAGIC -- checkin_count: check-ins no duplicados (DQ-010). active_hour_count: horas distintas con visitas (conteo homologado por ventana de 60 minutos).
# MAGIC SELECT
# MAGIC   d.month_start                              AS mes,
# MAGIC   b.primary_category,
# MAGIC   count(DISTINCT c.business_key)             AS negocios_con_trafico,
# MAGIC   sum(c.checkin_count)                       AS checkins_oficiales,
# MAGIC   sum(c.active_hour_count)                   AS horas_con_afluencia,
# MAGIC   round(avg(c.checkin_count), 2)             AS promedio_checkins_diarios_por_negocio
# MAGIC FROM workspace.yelp_gold.gld_agg_business_checkin_day c
# MAGIC JOIN workspace.yelp_gold.gld_dim_date d     ON c.date_key = d.date_key
# MAGIC JOIN workspace.yelp_gold.gld_dim_business b ON c.business_key = b.business_key
# MAGIC GROUP BY d.month_start, b.primary_category
# MAGIC ORDER BY mes DESC, checkins_oficiales DESC;

# COMMAND ----------

# DBTITLE 1,Q6 — Salud de reseñas: válidas, duplicadas y en cuarentena (REQ-03 · Trust & Safety: Rafael Nakamura)
# MAGIC %sql
# MAGIC SELECT
# MAGIC   to_date(CAST(period_month AS STRING), 'yyyyMM')                    AS mes,
# MAGIC   valid_review_count                                                 AS resenas_validas,
# MAGIC   invalid_review_count                                               AS resenas_duplicadas,
# MAGIC   quarantined_review_count                                           AS resenas_en_cuarentena,
# MAGIC   valid_review_count + invalid_review_count + quarantined_review_count AS total_recibidas,
# MAGIC   round(100.0 * valid_review_count /
# MAGIC         nullif(valid_review_count + invalid_review_count + quarantined_review_count, 0), 2) AS ratio_salud_pct,
# MAGIC   round(100.0 * quarantine_rate, 2)                                  AS tasa_cuarentena_pct
# MAGIC FROM workspace.yelp_gold.gld_agg_review_health_month
# MAGIC ORDER BY mes DESC;

# COMMAND ----------

# DBTITLE 1,Q7 — Retención mensual de cohortes de usuarios (REQ-04 · Producto: Andrew Collins)
# MAGIC %sql
# MAGIC SELECT
# MAGIC   to_date(CAST(cohort_month AS STRING), 'yyyyMM')   AS cohorte,
# MAGIC   to_date(CAST(activity_month AS STRING), 'yyyyMM') AS mes_actividad,
# MAGIC   months_since_join                                 AS mes_relativo,
# MAGIC   cohort_size                                       AS usuarios_cohorte,
# MAGIC   active_user_count                                 AS usuarios_activos,
# MAGIC   round(100.0 * retention_rate, 2)                  AS retencion_pct
# MAGIC FROM workspace.yelp_gold.gld_agg_user_cohort_month
# MAGIC WHERE months_since_join BETWEEN 0 AND 24
# MAGIC ORDER BY cohorte DESC, mes_relativo ASC
# MAGIC LIMIT 100;

# COMMAND ----------

# DBTITLE 1,Q8 — Observabilidad del pipeline medallón (Gobierno y FinOps · CDO / Data Platform)
# MAGIC %sql
# MAGIC SELECT
# MAGIC   executed_at,
# MAGIC   layer,
# MAGIC   table_name,
# MAGIC   row_count                                     AS filas_procesadas,
# MAGIC   duration_sec,
# MAGIC   round(row_count / nullif(duration_sec, 0), 1) AS throughput_filas_por_seg
# MAGIC FROM workspace.yelp_gov.pipeline_run_log
# MAGIC ORDER BY executed_at DESC, layer ASC
# MAGIC LIMIT 50;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Configuración del Genie Space sobre `workspace.yelp_gold`
# MAGIC
# MAGIC ### 2.1. Tablas conectadas al Genie Space
# MAGIC * `gld_dim_business`, `gld_dim_user`, `gld_dim_date`, `gld_dim_category`, `gld_bridge_business_category`
# MAGIC * `gld_fact_review`, `gld_fact_checkin`, `gld_fact_tip`
# MAGIC * `gld_agg_business_month`, `gld_agg_business_bayesian_month`, `gld_agg_business_checkin_day`, `gld_agg_review_health_month`, `gld_agg_user_cohort_month`
# MAGIC
# MAGIC ### 2.2. Instrucciones del sistema y sinónimos del Glosario E4 (copiar en *Genie → Instructions*)
# MAGIC ```text
# MAGIC Contexto de negocio — Plataforma YelpConnect (Grupo 03 · Data Evolution):
# MAGIC 1. "Rating oficial", "calificación justa" o "rating bayesiano": usa bayesian_rating de workspace.yelp_gold.gld_agg_business_bayesian_month (m = 25 reseñas), no el promedio simple, salvo que se pida comparar ambos.
# MAGIC 2. "Afluencia", "visitas físicas" o "check-ins válidos": usa checkin_count de workspace.yelp_gold.gld_agg_business_checkin_day (ya excluye duplicados). "Horas con afluencia": active_hour_count.
# MAGIC 3. "Reseña válida": filas de gld_fact_review con is_valid = true. Las métricas oficiales solo cuentan reseñas válidas.
# MAGIC 4. "Reseña en cuarentena", "spam" o "rechazos de calidad": quarantined_review_count y quarantine_rate de workspace.yelp_gold.gld_agg_review_health_month.
# MAGIC 5. "Retención" o "cohorte": retention_rate de workspace.yelp_gold.gld_agg_user_cohort_month; para retención posterior al alta filtra months_since_join > 0.
# MAGIC 6. "Negocio activo": is_open = true en workspace.yelp_gold.gld_dim_business. "Categoría" de un negocio: primary_category.
# MAGIC 7. Los meses (period_month, cohort_month, activity_month) son enteros AAAAMM.
# MAGIC ```
# MAGIC
# MAGIC ### 2.3. Registro de pruebas en Genie Space
# MAGIC
# MAGIC | # | Pregunta en lenguaje natural | Resultado esperado | Instrucción que lo asegura |
# MAGIC | :--- | :--- | :--- | :--- |
# MAGIC | **P1** | *"¿Cuáles son los 10 negocios abiertos con mejor calificación oficial y cuántas reseñas válidas tienen?"* | Une `gld_agg_business_bayesian_month` con `gld_dim_business` (`is_open = true`) y ordena por `bayesian_rating`. | #1 evita que suban negocios con una sola reseña de 5 estrellas. |
# MAGIC | **P2** | *"¿Cómo evolucionan cada mes las visitas físicas por categoría?"* | Suma `checkin_count` de `gld_agg_business_checkin_day` por `primary_category` y mes. | #2 y #6. |
# MAGIC | **P3** | *"¿Qué porcentaje de reseñas se envió a cuarentena cada mes?"* | Usa `quarantine_rate` de `gld_agg_review_health_month`. | #4. |
# MAGIC | **P4** | *"¿Cuál es la retención promedio de las cohortes después del alta?"* | Promedia `retention_rate` con `months_since_join > 0`. | #5: sin el filtro, el mes 0 infla el promedio. |
# MAGIC
# MAGIC > Las pruebas con Genie deben volver a ejecutarse después del cambio de nombres de las tablas Gold, y registrarse aquí sus resultados reales.
# MAGIC
# MAGIC ---
# MAGIC ## 3. Storytelling ejecutivo por página
# MAGIC
# MAGIC ### Página 1 · Negocio y Producto (*Andrew Collins, VP of Product*)
# MAGIC * **Mensaje clave:** la calificación bayesiana corrige el ranking donde negocios con 1–2 reseñas desplazaban a negocios consolidados, y el conteo oficial de check-ins (sin duplicados) concilia la métrica de tráfico entre Producto y Operaciones Comerciales (**Carla Romero**).
# MAGIC * **Decisión habilitada:** priorizar las categorías con caída de calificación válida y reconocer como "Comercio Recomendado" a los negocios con calificación bayesiana alta y retención comprobada (`Q1`, `Q4`, `Q5`, `Q7`).
# MAGIC
# MAGIC ### Página 2 · Gobierno, calidad y confianza (*Laura Hernández, CDO, y Rafael Nakamura, Trust & Safety*)
# MAGIC * **Mensaje clave:** todas las columnas de Silver y Gold están documentadas en el diccionario E4, las entidades críticas tienen RACI en Unity Catalog (E6) y el Quality Gate (E5) aísla en `slv_review_quarantine` las reseñas que incumplen reglas antes de publicar Gold (`Q2`, `Q3`, `Q6`, `Q8`).
# MAGIC * **Decisión habilitada:** auditar los meses con picos de cuarentena o duplicados sin alterar la calificación pública y certificar que el Lakehouse opera dentro del presupuesto de cómputo.
# MAGIC
# MAGIC ---
# MAGIC ### Definition of Done (E8)
# MAGIC - [x] Consultas `Q1` a `Q8` consumiendo solo `workspace.yelp_gold` y `workspace.yelp_gov`.
# MAGIC - [x] Cada consulta declara el requerimiento (`REQ-01` a `REQ-06`) y el stakeholder.
# MAGIC - [x] Genie Space documentado con instrucciones del Glosario E4.
# MAGIC - [x] Storytelling ejecutivo por página.
