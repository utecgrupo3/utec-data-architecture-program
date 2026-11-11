# Databricks notebook source
# MAGIC %md
# MAGIC # E8 · Dashboard de BI (AI/BI Dashboard + Genie Space) — Grupo 03
# MAGIC **Cierra con:** M9 BI y Visualización
# MAGIC
# MAGIC | Trazabilidad | |
# MAGIC |---|---|
# MAGIC | Implementa | Documento Formal §7 · Requerimientos de información y KPIs (REQ-P0 a REQ-P7 y REQ-G1 a REQ-G6) |
# MAGIC | Entregable | E8 · Dashboard de dos páginas (Producto y Gobierno) y Genie Space |
# MAGIC | Caso de uso | Producto (Andrew Collins): 1 · Experiencia por negocio · 2 · Engagement y retención · 3 · Experiencia por categoría |
# MAGIC | Página de Gobierno | CDO (Laura Hernández): salud de calidad (E5), cobertura de documentación (E4) y ownership (E4/E6) |
# MAGIC
# MAGIC **Reglas del consumo:** el dashboard y Genie leen solo `workspace.yelp_gold` y `workspace.yelp_gov`, nunca silver ni bronze.
# MAGIC El dataset es histórico: todas las ventanas de tiempo se calculan desde **el último mes de la muestra**, no desde la fecha actual.
# MAGIC
# MAGIC Cada celda es el dataset de un visual del dashboard `E8_dashboard_bi/YelpConnect_Producto_y_Gobierno.lvdash.json` y declara el requerimiento que responde.

# COMMAND ----------

# DBTITLE 1,REQ-P0 · Indicadores del último mes de la muestra
# MAGIC %sql
# MAGIC -- REQ-P0 · Página Producto · Andrew Collins: ¿cómo está la experiencia hoy, en una sola mirada? (último mes de la muestra)
# MAGIC WITH ref AS (SELECT to_date(cast(max(period_month) AS STRING), 'yyyyMM') AS mes_ref FROM workspace.yelp_gold.gld_agg_business_bayesian_month),
# MAGIC b12 AS (
# MAGIC   SELECT b.valid_rating, b.valid_review_count
# MAGIC   FROM workspace.yelp_gold.gld_agg_business_bayesian_month b CROSS JOIN ref
# MAGIC   WHERE to_date(cast(b.period_month AS STRING), 'yyyyMM') > add_months(ref.mes_ref, -12)
# MAGIC ),
# MAGIC coh AS (SELECT * FROM workspace.yelp_gold.gld_agg_user_cohort_month),
# MAGIC mx AS (SELECT to_date(cast(max(activity_month) AS STRING), 'yyyyMM') AS m FROM coh),
# MAGIC tam AS (SELECT DISTINCT cohort_month, cohort_size FROM coh),
# MAGIC cat AS (
# MAGIC   SELECT c.category_key
# MAGIC   FROM workspace.yelp_gold.gld_agg_category_month c CROSS JOIN ref
# MAGIC   WHERE to_date(cast(c.period_month AS STRING), 'yyyyMM') > add_months(ref.mes_ref, -12) AND c.review_count > 0
# MAGIC )
# MAGIC SELECT
# MAGIC   (SELECT date_format(mes_ref, 'yyyy-MM') FROM ref)                                                  AS mes_referencia,
# MAGIC   (SELECT round(sum(valid_rating * valid_review_count) / sum(valid_review_count), 2) FROM b12)       AS rating_valido_12m,
# MAGIC   (SELECT sum(active_user_count) FROM coh WHERE activity_month = (SELECT max(activity_month) FROM coh)) AS usuarios_activos_ultimo_mes,
# MAGIC   (SELECT round(100.0 * (SELECT sum(active_user_count) FROM coh WHERE months_since_join = 3)
# MAGIC                 / sum(t.cohort_size), 1)
# MAGIC      FROM tam t CROSS JOIN mx
# MAGIC      WHERE add_months(to_date(cast(t.cohort_month AS STRING), 'yyyyMM'), 3) <= mx.m)                                AS retencion_3_meses_pct,
# MAGIC   (SELECT count(DISTINCT category_key) FROM cat)                                                     AS categorias_evaluadas

# COMMAND ----------

# DBTITLE 1,REQ-P1 · Evolución mensual del rating confiable
# MAGIC %sql
# MAGIC -- REQ-P1 · Página Producto · Andrew Collins: ¿la experiencia mejora o empeora mes a mes, medida con un rating confiable?
# MAGIC WITH ref AS (SELECT to_date(cast(max(period_month) AS STRING), 'yyyyMM') AS mes_ref FROM workspace.yelp_gold.gld_agg_business_bayesian_month)
# MAGIC SELECT to_date(cast(b.period_month AS STRING), 'yyyyMM')                                                     AS mes,
# MAGIC        round(sum(b.valid_rating * b.valid_review_count) / sum(b.valid_review_count), 3)     AS rating_valido,
# MAGIC        round(avg(b.bayesian_rating), 3)                                                     AS rating_bayesiano_promedio,
# MAGIC        sum(b.valid_review_count)                                                            AS resenas_validas
# MAGIC FROM workspace.yelp_gold.gld_agg_business_bayesian_month b CROSS JOIN ref
# MAGIC WHERE to_date(cast(b.period_month AS STRING), 'yyyyMM') > add_months(ref.mes_ref, -36)
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# DBTITLE 1,REQ-P2 · Negocios: rating bayesiano de 12 meses y variación de los últimos 6
# MAGIC %sql
# MAGIC -- REQ-P2 · Página Producto · Andrew Collins: ¿qué negocios lideran la experiencia y cuáles empeoraron en los últimos 6 meses?
# MAGIC WITH ref AS (SELECT to_date(cast(max(period_month) AS STRING), 'yyyyMM') AS mes_ref FROM workspace.yelp_gold.gld_agg_business_bayesian_month),
# MAGIC base AS (
# MAGIC   SELECT b.business_key, to_date(cast(b.period_month AS STRING), 'yyyyMM') AS mes, b.valid_review_count AS v, b.valid_rating AS r
# MAGIC   FROM workspace.yelp_gold.gld_agg_business_bayesian_month b
# MAGIC ),
# MAGIC w AS (
# MAGIC   SELECT base.business_key,
# MAGIC          sum(CASE WHEN mes >  add_months(ref.mes_ref, -6) THEN v END)     AS v_rec,
# MAGIC          sum(CASE WHEN mes >  add_months(ref.mes_ref, -6) THEN v * r END) AS s_rec,
# MAGIC          sum(CASE WHEN mes <= add_months(ref.mes_ref, -6) THEN v END)     AS v_ant,
# MAGIC          sum(CASE WHEN mes <= add_months(ref.mes_ref, -6) THEN v * r END) AS s_ant
# MAGIC   FROM base CROSS JOIN ref
# MAGIC   WHERE mes > add_months(ref.mes_ref, -12)
# MAGIC   GROUP BY base.business_key
# MAGIC ),
# MAGIC c AS (SELECT (sum(coalesce(s_rec, 0)) + sum(coalesce(s_ant, 0))) / (sum(coalesce(v_rec, 0)) + sum(coalesce(v_ant, 0))) AS prom_ciudad FROM w)
# MAGIC SELECT d.name                                                                                AS negocio,
# MAGIC        d.primary_category                                                                    AS categoria,
# MAGIC        coalesce(w.v_rec, 0) + coalesce(w.v_ant, 0)                                           AS resenas_validas_12m,
# MAGIC        round((coalesce(w.s_rec, 0) + coalesce(w.s_ant, 0) + 25 * c.prom_ciudad)
# MAGIC              / (coalesce(w.v_rec, 0) + coalesce(w.v_ant, 0) + 25), 2)                        AS rating_bayesiano_12m,
# MAGIC        round((coalesce(w.s_rec, 0) + 25 * c.prom_ciudad) / (coalesce(w.v_rec, 0) + 25)
# MAGIC            - (coalesce(w.s_ant, 0) + 25 * c.prom_ciudad) / (coalesce(w.v_ant, 0) + 25), 2)   AS variacion_6m
# MAGIC FROM w
# MAGIC JOIN workspace.yelp_gold.gld_dim_business d ON d.business_key = w.business_key
# MAGIC CROSS JOIN c
# MAGIC WHERE coalesce(w.v_rec, 0) + coalesce(w.v_ant, 0) >= 10
# MAGIC ORDER BY variacion_6m ASC

# COMMAND ----------

# DBTITLE 1,REQ-P3 · Señales de pérdida de confianza en el contenido
# MAGIC %sql
# MAGIC -- REQ-P3 · Página Producto · Andrew Collins: ¿dónde se pierde la confianza en el contenido (reseñas negativas, no válidas, brecha con lo declarado)?
# MAGIC WITH ref AS (SELECT to_date(cast(max(period_month) AS STRING), 'yyyyMM') AS mes_ref FROM workspace.yelp_gold.gld_agg_business_bayesian_month)
# MAGIC SELECT to_date(cast(m.period_month AS STRING), 'yyyyMM')                                                                  AS mes,
# MAGIC        round(100.0 * sum(m.negative_review_rate * m.valid_review_count) / sum(m.valid_review_count), 2)  AS pct_resenas_negativas,
# MAGIC        round(100.0 * sum(m.invalid_content_rate * m.review_count) / sum(m.review_count), 2)              AS pct_resenas_no_validas,
# MAGIC        round(sum(m.rating_gap * m.valid_review_count) / sum(m.valid_review_count), 2)                    AS brecha_calificacion
# MAGIC FROM workspace.yelp_gold.gld_agg_business_month m CROSS JOIN ref
# MAGIC WHERE to_date(cast(m.period_month AS STRING), 'yyyyMM') > add_months(ref.mes_ref, -36)
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# DBTITLE 1,REQ-P4 · Usuarios activos y contribuidores por mes
# MAGIC %sql
# MAGIC -- REQ-P4 · Página Producto · Andrew Collins: ¿cuántos usuarios activos y contribuidores tenemos cada mes?
# MAGIC WITH ref AS (SELECT to_date(cast(max(period_month) AS STRING), 'yyyyMM') AS mes_ref FROM workspace.yelp_gold.gld_agg_business_bayesian_month),
# MAGIC a AS (SELECT activity_month AS pm, sum(active_user_count) AS usuarios_activos
# MAGIC       FROM workspace.yelp_gold.gld_agg_user_cohort_month GROUP BY activity_month),
# MAGIC c AS (SELECT period_month AS pm, count_if(is_contributor) AS usuarios_contribuidores
# MAGIC       FROM workspace.yelp_gold.gld_agg_user_month GROUP BY period_month),
# MAGIC j AS (SELECT to_date(cast(coalesce(a.pm, c.pm) AS STRING), 'yyyyMM') AS mes,
# MAGIC              coalesce(a.usuarios_activos, 0) AS usuarios_activos,
# MAGIC              coalesce(c.usuarios_contribuidores, 0) AS usuarios_contribuidores
# MAGIC       FROM a FULL OUTER JOIN c ON a.pm = c.pm)
# MAGIC SELECT j.* FROM j CROSS JOIN ref
# MAGIC WHERE j.mes > add_months(ref.mes_ref, -36)
# MAGIC ORDER BY j.mes

# COMMAND ----------

# DBTITLE 1,REQ-P5 · Retención por año de alta y meses desde el alta
# MAGIC %sql
# MAGIC -- REQ-P5 · Página Producto · Andrew Collins: ¿los usuarios nuevos se quedan? (retención por año de alta)
# MAGIC WITH coh AS (SELECT * FROM workspace.yelp_gold.gld_agg_user_cohort_month),
# MAGIC mx AS (SELECT to_date(cast(max(activity_month) AS STRING), 'yyyyMM') AS m FROM coh),
# MAGIC tam AS (SELECT DISTINCT cohort_month, cohort_size FROM coh),
# MAGIC grilla AS (
# MAGIC   SELECT t.cohort_month, t.cohort_size, k.meses
# MAGIC   FROM tam t
# MAGIC   CROSS JOIN (SELECT explode(sequence(0, 12)) AS meses) k
# MAGIC   CROSS JOIN mx
# MAGIC   WHERE add_months(to_date(cast(t.cohort_month AS STRING), 'yyyyMM'), k.meses) <= mx.m
# MAGIC     AND floor(t.cohort_month / 100) >= year(mx.m) - 7
# MAGIC )
# MAGIC SELECT cast(floor(g.cohort_month / 100) AS STRING)                                    AS cohorte_anio,
# MAGIC        g.meses                                                                        AS meses_desde_alta,
# MAGIC        round(100.0 * sum(coalesce(c.active_user_count, 0)) / sum(g.cohort_size), 1)   AS retencion_pct
# MAGIC FROM grilla g
# MAGIC LEFT JOIN coh c ON c.cohort_month = g.cohort_month AND c.months_since_join = g.meses
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 1, 2

# COMMAND ----------

# DBTITLE 1,REQ-P6 · Rating válido por categoría (12 meses, mínimo 100 reseñas)
# MAGIC %sql
# MAGIC -- REQ-P6 · Página Producto · Andrew Collins: ¿qué tipo de negocio da mejor y peor experiencia?
# MAGIC WITH ref AS (SELECT to_date(cast(max(period_month) AS STRING), 'yyyyMM') AS mes_ref FROM workspace.yelp_gold.gld_agg_business_bayesian_month)
# MAGIC SELECT dc.name                                                                       AS categoria,
# MAGIC        sum(c.review_count)                                                           AS resenas_12m,
# MAGIC        round(sum(c.valid_rating * c.review_count) / sum(c.review_count), 2)          AS rating_valido,
# MAGIC        round(avg(c.business_count), 0)                                               AS negocios
# MAGIC FROM workspace.yelp_gold.gld_agg_category_month c
# MAGIC JOIN workspace.yelp_gold.gld_dim_category dc ON dc.category_key = c.category_key
# MAGIC CROSS JOIN ref
# MAGIC WHERE to_date(cast(c.period_month AS STRING), 'yyyyMM') > add_months(ref.mes_ref, -12)
# MAGIC GROUP BY dc.name
# MAGIC HAVING sum(c.review_count) >= 100
# MAGIC ORDER BY rating_valido ASC

# COMMAND ----------

# DBTITLE 1,REQ-P7 · Concentración de reseñas negativas por categoría (12 meses)
# MAGIC %sql
# MAGIC -- REQ-P7 · Página Producto · Andrew Collins: ¿en qué categorías se concentra la insatisfacción?
# MAGIC WITH ref AS (SELECT to_date(cast(max(period_month) AS STRING), 'yyyyMM') AS mes_ref FROM workspace.yelp_gold.gld_agg_business_bayesian_month)
# MAGIC SELECT dc.name                                                                     AS categoria,
# MAGIC        sum(c.negative_review_count)                                                AS resenas_negativas,
# MAGIC        round(100.0 * sum(c.negative_review_count) / sum(c.review_count), 1)        AS pct_negativas
# MAGIC FROM workspace.yelp_gold.gld_agg_category_month c
# MAGIC JOIN workspace.yelp_gold.gld_dim_category dc ON dc.category_key = c.category_key
# MAGIC CROSS JOIN ref
# MAGIC WHERE to_date(cast(c.period_month AS STRING), 'yyyyMM') > add_months(ref.mes_ref, -12)
# MAGIC GROUP BY dc.name
# MAGIC HAVING sum(c.review_count) >= 100
# MAGIC ORDER BY pct_negativas DESC
# MAGIC LIMIT 15

# COMMAND ----------

# DBTITLE 1,REQ-G1 · Evolución de la calidad por corrida
# MAGIC %sql
# MAGIC -- REQ-G1 · Página Gobierno · Laura Hernández (CDO): ¿la calidad de los datos mejora entre corridas?
# MAGIC SELECT executed_at                                              AS corrida,
# MAGIC        count(*)                                                 AS reglas_evaluadas,
# MAGIC        count_if(passed)                                         AS reglas_que_cumplen,
# MAGIC        round(100.0 * count_if(passed) / count(*), 1)            AS pct_reglas_que_cumplen
# MAGIC FROM workspace.yelp_gov.dq_results
# MAGIC GROUP BY executed_at
# MAGIC ORDER BY executed_at

# COMMAND ----------

# DBTITLE 1,REQ-G2 · Cumplimiento por dimensión (última corrida)
# MAGIC %sql
# MAGIC -- REQ-G2 · Página Gobierno · Laura Hernández (CDO): ¿en qué dimensión de calidad fallamos?
# MAGIC SELECT r.dimension,
# MAGIC        count(*)                                          AS reglas,
# MAGIC        count_if(res.passed)                              AS reglas_que_cumplen,
# MAGIC        round(avg(res.compliance_pct), 2)                 AS cumplimiento_promedio_pct
# MAGIC FROM workspace.yelp_gov.dq_results res
# MAGIC JOIN workspace.yelp_gov.dq_rules r ON r.rule_id = res.rule_id
# MAGIC WHERE res.executed_at = (SELECT max(executed_at) FROM workspace.yelp_gov.dq_results)
# MAGIC GROUP BY r.dimension
# MAGIC ORDER BY cumplimiento_promedio_pct

# COMMAND ----------

# DBTITLE 1,REQ-G3 · Reglas que no cumplen hoy, con acción y responsable
# MAGIC %sql
# MAGIC -- REQ-G3 · Página Gobierno · Laura Hernández (CDO): ¿qué reglas fallan hoy, qué se hace y quién responde?
# MAGIC SELECT res.rule_id                   AS regla,
# MAGIC        r.criticality                 AS criticidad,
# MAGIC        r.dimension                   AS dimension,
# MAGIC        r.description                 AS descripcion,
# MAGIC        res.compliance_pct            AS cumplimiento_pct,
# MAGIC        r.threshold_pct               AS umbral_pct,
# MAGIC        r.failure_action              AS accion,
# MAGIC        r.rule_owner                  AS responsable
# MAGIC FROM workspace.yelp_gov.dq_results res
# MAGIC JOIN workspace.yelp_gov.dq_rules r ON r.rule_id = res.rule_id
# MAGIC WHERE res.executed_at = (SELECT max(executed_at) FROM workspace.yelp_gov.dq_results)
# MAGIC   AND NOT res.passed
# MAGIC ORDER BY CASE r.criticality WHEN 'critical' THEN 1 WHEN 'high' THEN 2 ELSE 3 END, res.rule_id

# COMMAND ----------

# DBTITLE 1,REQ-G4 · Cobertura de documentación por capa
# MAGIC %sql
# MAGIC -- REQ-G4 · Página Gobierno · Laura Hernández (CDO): ¿está documentado todo lo que publicamos?
# MAGIC WITH uc AS (
# MAGIC   SELECT table_schema, concat('workspace.', table_schema, '.', table_name, '.', column_name) AS fq, comment
# MAGIC   FROM workspace.information_schema.columns
# MAGIC   WHERE table_schema IN ('yelp_silver', 'yelp_gold')
# MAGIC ),
# MAGIC dd AS (SELECT DISTINCT physical_name FROM workspace.yelp_gov.data_dictionary)
# MAGIC SELECT replace(uc.table_schema, 'yelp_', '')                                                  AS capa,
# MAGIC        count(*)                                                                               AS columnas,
# MAGIC        count(dd.physical_name)                                                                AS documentadas,
# MAGIC        round(100.0 * count(dd.physical_name) / count(*), 1)                                   AS pct_en_diccionario,
# MAGIC        round(100.0 * count_if(uc.comment IS NOT NULL AND trim(uc.comment) <> '') / count(*), 1) AS pct_con_comentario
# MAGIC FROM uc LEFT JOIN dd ON dd.physical_name = uc.fq
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# DBTITLE 1,REQ-G5 · Ownership: términos y entidades con dueño
# MAGIC %sql
# MAGIC -- REQ-G5 · Página Gobierno · Laura Hernández (CDO): ¿cada término y cada entidad tiene dueño?
# MAGIC SELECT
# MAGIC   (SELECT count(*) FROM workspace.yelp_gov.business_glossary)                                                          AS terminos,
# MAGIC   (SELECT round(100.0 * count_if(term_owner IS NOT NULL AND trim(term_owner) <> '') / count(*), 1)
# MAGIC      FROM workspace.yelp_gov.business_glossary)                                                                         AS pct_terminos_con_dueno,
# MAGIC   (SELECT count(DISTINCT data_domain) FROM workspace.yelp_gov.business_glossary)                                        AS dominios_glosario,
# MAGIC   (SELECT count(*) FROM workspace.yelp_gov.raci_matrix)                                                                 AS entidades_raci,
# MAGIC   (SELECT round(100.0 * count_if(data_owner IS NOT NULL AND data_steward IS NOT NULL) / count(*), 1)
# MAGIC      FROM workspace.yelp_gov.raci_matrix)                                                                               AS pct_entidades_con_owner_y_steward

# COMMAND ----------

# DBTITLE 1,REQ-G6 · Duración del pipeline por día y capa (últimos 14 días)
# MAGIC %sql
# MAGIC -- REQ-G6 · Página Gobierno · Laura Hernández (CDO): ¿el pipeline es estable y cuánto tiempo de cómputo consume?
# MAGIC SELECT date(executed_at)                         AS fecha,
# MAGIC        layer                                     AS capa,
# MAGIC        round(sum(duration_sec) / 60.0, 1)        AS minutos
# MAGIC FROM workspace.yelp_gov.pipeline_run_log
# MAGIC WHERE executed_at >= current_date() - INTERVAL 14 DAYS
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 1, 2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Genie Space · configuración
# MAGIC **Nombre:** YelpConnect · Producto. **Tablas:** `gld_agg_business_bayesian_month`, `gld_agg_business_month`, `gld_agg_category_month`, `gld_agg_user_cohort_month`, `gld_agg_user_month`, `gld_dim_business`, `gld_dim_category`, `gld_dim_date`, `gld_bridge_business_category` (todas de `workspace.yelp_gold`).
# MAGIC
# MAGIC **Instrucciones generales (copiar en el Genie Space):**
# MAGIC ```
# MAGIC Eres el analista de datos del área de Producto de YelpConnect. Responde en español.
# MAGIC - El dataset es histórico (Philadelphia). 'Último mes', 'este año' o 'último año' se cuentan desde el mes más reciente de la tabla consultada, nunca desde la fecha actual.
# MAGIC - Los meses se guardan como enteros AAAAMM (period_month, activity_month, cohort_month).
# MAGIC - Usuario activo: usuario con al menos una reseña válida en el mes (active_user_count). Usuario contribuidor: al menos una reseña o un tip en el mes (is_contributor). No son lo mismo.
# MAGIC - Reseña válida: is_valid = true; las duplicadas no cuentan. La calificación confiable de un negocio es el rating bayesiano (bayesian_rating), con m = 25 reseñas y C = promedio de la ciudad.
# MAGIC - Para promediar ratings entre negocios o meses, pondera por la cantidad de reseñas válidas (valid_review_count).
# MAGIC - Al comparar categorías, exige al menos 100 reseñas en el periodo.
# MAGIC - Check-in válido: marca no duplicada por fecha y hora exactas; los check-ins no identifican al usuario.
# MAGIC ```
# MAGIC
# MAGIC **Sinónimos por columna (del glosario):**
# MAGIC | Columna | Sinónimos |
# MAGIC |---|---|
# MAGIC | `bayesian_rating` | calificación confiable, rating oficial, BWR, rating bayesiano |
# MAGIC | `valid_rating` | rating válido, calificación de reseñas válidas |
# MAGIC | `active_user_count` | usuarios activos, gente que reseña, MAU |
# MAGIC | `is_contributor` | contribuidor, usuario que publica contenido |
# MAGIC | `retention_rate` | retención, permanencia, CRR |
# MAGIC | `cohort_month` | cohorte, mes de alta, camada de registro |
# MAGIC | `negative_review_count` / `negative_review_rate` | reseñas negativas, de 1 o 2 estrellas |
# MAGIC | `invalid_content_rate` | reseñas no válidas, duplicadas |
# MAGIC | `checkin_count` | visitas, check-ins válidos, afluencia, HAI |
# MAGIC | `name` (gld_dim_category) | categoría, rubro, tipo de negocio |
# MAGIC
# MAGIC **Las 5 preguntas de negocio de la sustentación** (cargarlas como *benchmarks* y como consultas de confianza; el SQL de referencia está en las celdas siguientes):
# MAGIC 1. ¿Cuántos usuarios activos hubo en el último mes de la muestra?
# MAGIC 2. ¿Qué 5 categorías tuvieron la peor calificación confiable en el último año?
# MAGIC 3. ¿Qué negocios empeoraron más su rating bayesiano en los últimos 6 meses?
# MAGIC 4. ¿Qué porcentaje de los usuarios que se unieron en 2018 seguía activo 6 meses después?
# MAGIC 5. ¿Cómo evolucionó la calificación confiable de la ciudad mes a mes en el último año?

# COMMAND ----------

# DBTITLE 1,Genie · pregunta 1
# MAGIC %sql
# MAGIC -- ¿Cuántos usuarios activos hubo en el último mes de la muestra?
# MAGIC SELECT to_date(cast(activity_month AS STRING), 'yyyyMM') AS mes, sum(active_user_count) AS usuarios_activos
# MAGIC FROM workspace.yelp_gold.gld_agg_user_cohort_month
# MAGIC WHERE activity_month = (SELECT max(activity_month) FROM workspace.yelp_gold.gld_agg_user_cohort_month)
# MAGIC GROUP BY 1

# COMMAND ----------

# DBTITLE 1,Genie · pregunta 2
# MAGIC %sql
# MAGIC -- ¿Qué 5 categorías tuvieron la peor calificación confiable en el último año?
# MAGIC WITH ref AS (SELECT to_date(cast(max(period_month) AS STRING), 'yyyyMM') AS mes_ref FROM workspace.yelp_gold.gld_agg_business_bayesian_month)
# MAGIC SELECT dc.name AS categoria, round(sum(c.valid_rating * c.review_count) / sum(c.review_count), 2) AS rating_valido, sum(c.review_count) AS resenas
# MAGIC FROM workspace.yelp_gold.gld_agg_category_month c JOIN workspace.yelp_gold.gld_dim_category dc ON dc.category_key = c.category_key CROSS JOIN ref
# MAGIC WHERE to_date(cast(c.period_month AS STRING), 'yyyyMM') > add_months(ref.mes_ref, -12)
# MAGIC GROUP BY dc.name HAVING sum(c.review_count) >= 100
# MAGIC ORDER BY rating_valido ASC LIMIT 5

# COMMAND ----------

# DBTITLE 1,Genie · pregunta 3
# MAGIC %sql
# MAGIC -- ¿Qué negocios empeoraron más su rating bayesiano en los últimos 6 meses?
# MAGIC WITH ref AS (SELECT to_date(cast(max(period_month) AS STRING), 'yyyyMM') AS mes_ref FROM workspace.yelp_gold.gld_agg_business_bayesian_month),
# MAGIC base AS (
# MAGIC   SELECT b.business_key, to_date(cast(b.period_month AS STRING), 'yyyyMM') AS mes, b.valid_review_count AS v, b.valid_rating AS r
# MAGIC   FROM workspace.yelp_gold.gld_agg_business_bayesian_month b
# MAGIC ),
# MAGIC w AS (
# MAGIC   SELECT base.business_key,
# MAGIC          sum(CASE WHEN mes >  add_months(ref.mes_ref, -6) THEN v END)     AS v_rec,
# MAGIC          sum(CASE WHEN mes >  add_months(ref.mes_ref, -6) THEN v * r END) AS s_rec,
# MAGIC          sum(CASE WHEN mes <= add_months(ref.mes_ref, -6) THEN v END)     AS v_ant,
# MAGIC          sum(CASE WHEN mes <= add_months(ref.mes_ref, -6) THEN v * r END) AS s_ant
# MAGIC   FROM base CROSS JOIN ref
# MAGIC   WHERE mes > add_months(ref.mes_ref, -12)
# MAGIC   GROUP BY base.business_key
# MAGIC ),
# MAGIC c AS (SELECT (sum(coalesce(s_rec, 0)) + sum(coalesce(s_ant, 0))) / (sum(coalesce(v_rec, 0)) + sum(coalesce(v_ant, 0))) AS prom_ciudad FROM w)
# MAGIC SELECT d.name                                                                                AS negocio,
# MAGIC        d.primary_category                                                                    AS categoria,
# MAGIC        coalesce(w.v_rec, 0) + coalesce(w.v_ant, 0)                                           AS resenas_validas_12m,
# MAGIC        round((coalesce(w.s_rec, 0) + coalesce(w.s_ant, 0) + 25 * c.prom_ciudad)
# MAGIC              / (coalesce(w.v_rec, 0) + coalesce(w.v_ant, 0) + 25), 2)                        AS rating_bayesiano_12m,
# MAGIC        round((coalesce(w.s_rec, 0) + 25 * c.prom_ciudad) / (coalesce(w.v_rec, 0) + 25)
# MAGIC            - (coalesce(w.s_ant, 0) + 25 * c.prom_ciudad) / (coalesce(w.v_ant, 0) + 25), 2)   AS variacion_6m
# MAGIC FROM w
# MAGIC JOIN workspace.yelp_gold.gld_dim_business d ON d.business_key = w.business_key
# MAGIC CROSS JOIN c
# MAGIC WHERE coalesce(w.v_rec, 0) + coalesce(w.v_ant, 0) >= 10
# MAGIC ORDER BY variacion_6m ASC
# MAGIC LIMIT 10

# COMMAND ----------

# DBTITLE 1,Genie · pregunta 4
# MAGIC %sql
# MAGIC -- ¿Qué porcentaje de los usuarios que se unieron en 2018 seguía activo 6 meses después?
# MAGIC WITH coh AS (SELECT * FROM workspace.yelp_gold.gld_agg_user_cohort_month WHERE floor(cohort_month / 100) = 2018),
# MAGIC tam AS (SELECT DISTINCT cohort_month, cohort_size FROM coh)
# MAGIC SELECT round(100.0 * (SELECT sum(active_user_count) FROM coh WHERE months_since_join = 6) / sum(cohort_size), 1) AS retencion_6_meses_pct
# MAGIC FROM tam

# COMMAND ----------

# DBTITLE 1,Genie · pregunta 5
# MAGIC %sql
# MAGIC -- ¿Cómo evolucionó la calificación confiable de la ciudad mes a mes en el último año?
# MAGIC WITH ref AS (SELECT to_date(cast(max(period_month) AS STRING), 'yyyyMM') AS mes_ref FROM workspace.yelp_gold.gld_agg_business_bayesian_month)
# MAGIC SELECT to_date(cast(b.period_month AS STRING), 'yyyyMM') AS mes,
# MAGIC        round(sum(b.valid_rating * b.valid_review_count) / sum(b.valid_review_count), 2) AS rating_valido,
# MAGIC        round(avg(b.bayesian_rating), 2) AS rating_bayesiano_promedio
# MAGIC FROM workspace.yelp_gold.gld_agg_business_bayesian_month b CROSS JOIN ref
# MAGIC WHERE to_date(cast(b.period_month AS STRING), 'yyyyMM') > add_months(ref.mes_ref, -12)
# MAGIC GROUP BY 1 ORDER BY 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definition of Done (E8)
# MAGIC - [ ] Cada visual responde a un requerimiento declarado en el Documento §7 (REQ-P0 a REQ-P7 · REQ-G1 a REQ-G6).
# MAGIC - [ ] La página de Gobierno muestra salud de calidad (REQ-G1 a G3), cobertura de documentación (REQ-G4) y ownership (REQ-G5).
# MAGIC - [ ] El Genie Space responde correctamente las 5 preguntas usando los sinónimos del glosario.
# MAGIC - [ ] Dashboard publicado y su archivo `.lvdash.json` versionado en Git (rama `feature/E8-dashboard`, Pull Request revisado).
