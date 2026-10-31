# Databricks notebook source
# MAGIC %md
# MAGIC # E8 · Dashboard de BI (AI/BI Dashboard + Genie Space) — Grupo 03 (Data Evolution)
# MAGIC **Cierra con:** M9 BI y Visualización
# MAGIC
# MAGIC El entregable comprende **DOS artefactos en el workspace**:
# MAGIC 1. **AI/BI Dashboard** *"Plataforma de Datos YelpConnect — Grupo 03 (Data Evolution)"* con 2 páginas:
# MAGIC    - **Página 1 · Negocio (Foco: Producto — Andrew Collins, Ops Comerciales — Carla Romero, Trust & Safety — Rafael Nakamura):** KPIs certificados de rating bayesiano ($BWR$), crecimiento de reseñas ($NRG$), afluencia homologada de check-ins ($HAI$), retención de cohortes ($CRR$) y salud de reseñas ($RHR$).
# MAGIC    - **Página 2 · Gobierno y Operaciones (Foco: Chief Data Officer — Laura Hernández):** Scorecard de calidad por dimensión DAMA-DMBOK (**E5**), completitud de metadatos en diccionario y glosario (**E4**), cobertura de ownership RACI (**E6**) y observabilidad FinOps del pipeline (**E7**).
# MAGIC 2. **Genie Space** sobre la capa `workspace.yelp_gold`, configurado con las instrucciones y sinónimos del Glosario de Negocio (**E4**) para consultas en lenguaje natural.
# MAGIC
# MAGIC **Regla de Arquitectura:** El dashboard consume **ÚNICAMENTE** `workspace.yelp_gold` y `workspace.yelp_gov` (nunca `yelp_silver` ni `yelp_bronze`), validando el aislamiento de capas del Lakehouse.
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la **Sección 7 («Requerimientos de Información y KPIs por Stakeholder»)** del
# MAGIC > **Documento Formal de Arquitectura de Datos (v1.2 · Equipo Data Evolution)**. Cada consulta (`Q1` a `Q8`) declara explícitamente el código de requerimiento (`REQ-01` a `REQ-06`) y el stakeholder cuyas decisiones habilita.

# COMMAND ----------

# DBTITLE 1,Q1 — Evolución mensual de reseñas, rating simple y Rating Bayesiano BWR (REQ-01 · Producto: Andrew Collins)
# MAGIC %sql
# MAGIC -- Responde a REQ-01 (Andrew Collins, VP of Product):
# MAGIC -- ¿Cómo evoluciona mensualmente el volumen de reseñas verificadas, el rating aritmético y el Rating Ponderado Bayesiano (ADR-02)?
# MAGIC WITH mensual AS (
# MAGIC   SELECT
# MAGIC     mes,
# MAGIC     sum(n_resenas)                         AS resenas_verificadas,
# MAGIC     sum(usuarios_unicos)                   AS usuarios_activos_mes,
# MAGIC     sum(useful_total)                      AS votos_utilidad_total,
# MAGIC     round(avg(rating_promedio), 2)         AS rating_aritmetico,
# MAGIC     round(avg(rating_bayesiano), 2)        AS rating_bayesiano_bwr
# MAGIC   FROM workspace.yelp_gold.gld_kpi_negocio_mensual
# MAGIC   GROUP BY mes
# MAGIC )
# MAGIC SELECT
# MAGIC   mes,
# MAGIC   resenas_verificadas,
# MAGIC   usuarios_activos_mes,
# MAGIC   votos_utilidad_total,
# MAGIC   rating_aritmetico,
# MAGIC   rating_bayesiano_bwr,
# MAGIC   round(
# MAGIC     100.0 * (resenas_verificadas - lag(resenas_verificadas) OVER (ORDER BY mes))
# MAGIC     / nullif(lag(resenas_verificadas) OVER (ORDER BY mes), 0),
# MAGIC     2
# MAGIC   ) AS crecimiento_neto_nrg_pct
# MAGIC FROM mensual
# MAGIC ORDER BY mes;

# COMMAND ----------

# DBTITLE 1,Q2 — Scorecard de Salud de Calidad de Datos por Dimensión DAMA-DMBOK (Gobierno · CDO: Laura Hernández)
# MAGIC %sql
# MAGIC -- Responde a la CDO (Laura Hernández):
# MAGIC -- Estado actual del cumplimiento de las 15 reglas de calidad (DQ-001 a DQ-015) agrupadas en las 6 dimensiones DAMA-DMBOK.
# MAGIC SELECT
# MAGIC   r.dimension                                          AS dimension_calidad,
# MAGIC   count(*)                                             AS total_reglas,
# MAGIC   sum(CASE WHEN res.paso THEN 1 ELSE 0 END)            AS reglas_en_verde,
# MAGIC   sum(CASE WHEN NOT res.paso THEN 1 ELSE 0 END)        AS reglas_en_rojo,
# MAGIC   round(avg(res.pct_cumplimiento), 2)                  AS cumplimiento_promedio_pct,
# MAGIC   min(res.pct_cumplimiento)                            AS cumplimiento_minimo_pct
# MAGIC FROM workspace.yelp_gov.dq_resultados res
# MAGIC JOIN workspace.yelp_gov.dq_reglas r USING (regla_id)
# MAGIC WHERE res.ejecucion_ts = (SELECT max(ejecucion_ts) FROM workspace.yelp_gov.dq_resultados)
# MAGIC GROUP BY r.dimension
# MAGIC ORDER BY cumplimiento_promedio_pct DESC;

# COMMAND ----------

# DBTITLE 1,Q3 — Indicadores Ejecutivos de Gobierno de Datos: E4 + E5 + E6 (Gobierno · CDO: Laura Hernández)
# MAGIC %sql
# MAGIC -- Responde a la CDO (Laura Hernández):
# MAGIC -- Cobertura de documentación en Diccionario de Datos (E4), Glosario de Negocio (E4), Matriz RACI (E6) y Reglas DQ (E5).
# MAGIC SELECT
# MAGIC   (SELECT round(100.0 * sum(CASE WHEN descripcion_negocio IS NOT NULL AND length(trim(descripcion_negocio)) > 0 THEN 1 ELSE 0 END) / count(*), 1)
# MAGIC      FROM workspace.yelp_gov.diccionario_datos)                                                 AS pct_columnas_documentadas,
# MAGIC   (SELECT count(*) FROM workspace.yelp_gov.diccionario_datos)                                   AS total_atributos_diccionario,
# MAGIC   (SELECT count(*) FROM workspace.yelp_gov.matriz_raci)                                         AS entidades_con_owner_raci,
# MAGIC   (SELECT count(*) FROM workspace.yelp_gov.glosario_negocio)                                    AS terminos_glosario_aprobados,
# MAGIC   (SELECT count(*) FROM workspace.yelp_gov.dq_reglas)                                           AS reglas_dq_activas,
# MAGIC   (SELECT round(avg(pct_cumplimiento), 2)
# MAGIC      FROM workspace.yelp_gov.dq_resultados
# MAGIC     WHERE ejecucion_ts = (SELECT max(ejecucion_ts) FROM workspace.yelp_gov.dq_resultados))      AS indice_global_calidad_pct;

# COMMAND ----------

# DBTITLE 1,Q4 — Top Negocios por Rating Ponderado Bayesiano BWR y Utilidad Comunitaria CUR (REQ-01 / REQ-02 · Producto: Andrew Collins)
# MAGIC %sql
# MAGIC -- Responde a REQ-01 y REQ-02 (Andrew Collins, VP of Product):
# MAGIC -- Ranking de establecimientos para la insignia "Comercio Recomendado" evitando el sesgo de muestras pequeñas mediante BWR (ADR-02).
# MAGIC SELECT
# MAGIC   n.business_id,
# MAGIC   n.nombre                                             AS nombre_comercio,
# MAGIC   n.ciudad,
# MAGIC   n.estado,
# MAGIC   n.categoria_principal,
# MAGIC   n.esta_abierto,
# MAGIC   sum(k.n_resenas)                                     AS total_resenas_verificadas,
# MAGIC   sum(k.usuarios_unicos)                               AS total_usuarios_unicos,
# MAGIC   round(avg(k.rating_promedio), 2)                     AS rating_aritmetico_simple,
# MAGIC   round(avg(k.rating_bayesiano), 2)                    AS rating_bayesiano_bwr,
# MAGIC   sum(k.useful_total)                                  AS votos_useful_acumulados,
# MAGIC   round(sum(k.useful_total) / nullif(sum(k.n_resenas), 0), 2) AS indice_utilidad_cur
# MAGIC FROM workspace.yelp_gold.gld_kpi_negocio_mensual k
# MAGIC JOIN workspace.yelp_gold.gld_dim_negocio n
# MAGIC   ON k.business_id = n.business_id AND n.es_actual = true
# MAGIC GROUP BY n.business_id, n.nombre, n.ciudad, n.estado, n.categoria_principal, n.esta_abierto
# MAGIC HAVING sum(k.n_resenas) >= 5
# MAGIC ORDER BY rating_bayesiano_bwr DESC, total_resenas_verificadas DESC
# MAGIC LIMIT 25;

# COMMAND ----------

# DBTITLE 1,Q5 — Evolución y Top de Afluencia Física Homologada: Check-ins Diarios HAI (REQ-02 / REQ-05 · Ops Comerciales: Carla Romero)
# MAGIC %sql
# MAGIC -- Responde a REQ-02 y REQ-05 (Carla Romero, Head of Commercial Ops & Ads / Andrew Collins, VP of Product):
# MAGIC -- Conteo oficial homologado de visitas físicas deduplicadas por ventana horaria de 60 minutos (ADR-01 / Acta 01).
# MAGIC SELECT
# MAGIC   date_trunc('month', c.fecha)                         AS mes_actividad,
# MAGIC   c.ciudad,
# MAGIC   n.categoria_principal,
# MAGIC   count(DISTINCT c.business_id)                        AS comercios_con_trafico,
# MAGIC   sum(c.checkins_validos)                              AS indice_afluencia_homologada_hai,
# MAGIC   round(avg(c.checkins_validos), 2)                    AS promedio_checkins_diarios_por_comercio
# MAGIC FROM workspace.yelp_gold.gld_checkins_diarios c
# MAGIC JOIN workspace.yelp_gold.gld_dim_negocio n
# MAGIC   ON c.business_id = n.business_id AND n.es_actual = true
# MAGIC GROUP BY date_trunc('month', c.fecha), c.ciudad, n.categoria_principal
# MAGIC ORDER BY mes_actividad DESC, indice_afluencia_homologada_hai DESC;

# COMMAND ----------

# DBTITLE 1,Q6 — Monitoreo de Salud de Reseñas y Tasa de Cuarentena RHR (REQ-03 · Trust & Safety: Rafael Nakamura)
# MAGIC %sql
# MAGIC -- Responde a REQ-03 (Rafael Nakamura, Lead Trust & Safety / Andrew Collins, VP of Product):
# MAGIC -- ¿Qué porcentaje de reseñas son aisladas en cuarentena por sospecha de duplicidad o incumplimiento DQ (Acta 02)?
# MAGIC SELECT
# MAGIC   date_trunc('month', fecha)                           AS mes_evaluacion,
# MAGIC   sum(resenas_validas)                                 AS resenas_publicadas_gold,
# MAGIC   sum(resenas_cuarentena)                              AS resenas_aisladas_cuarentena,
# MAGIC   sum(resenas_validas + resenas_cuarentena)            AS total_resenas_procesadas,
# MAGIC   round(
# MAGIC     100.0 * sum(resenas_validas) / nullif(sum(resenas_validas + resenas_cuarentena), 0),
# MAGIC     2
# MAGIC   )                                                    AS ratio_salud_resenas_rhr_pct,
# MAGIC   round(
# MAGIC     100.0 * sum(resenas_cuarentena) / nullif(sum(resenas_validas + resenas_cuarentena), 0),
# MAGIC     2
# MAGIC   )                                                    AS tasa_rechazo_cuarentena_pct
# MAGIC FROM workspace.yelp_gold.gld_salud_resenas
# MAGIC GROUP BY date_trunc('month', fecha)
# MAGIC ORDER BY mes_evaluacion DESC;

# COMMAND ----------

# DBTITLE 1,Q7 — Análisis de Retención Mensual de Cohortes de Usuarios CRR % (REQ-04 · Producto: Andrew Collins)
# MAGIC %sql
# MAGIC -- Responde a REQ-04 (Andrew Collins, VP of Product):
# MAGIC -- ¿Cuál es la tasa de retención de cohortes de usuarios según su mes/año de registro (yelping_since) a lo largo de su ciclo de vida?
# MAGIC SELECT
# MAGIC   cohorte_mes,
# MAGIC   mes_actividad,
# MAGIC   mes_relativo,
# MAGIC   usuarios_cohorte,
# MAGIC   usuarios_activos,
# MAGIC   retencion_pct                                        AS tasa_retencion_crr_pct
# MAGIC FROM workspace.yelp_gold.gld_cohortes_retencion_mensual
# MAGIC WHERE mes_relativo BETWEEN 0 AND 24
# MAGIC ORDER BY cohorte_mes DESC, mes_relativo ASC
# MAGIC LIMIT 100;

# COMMAND ----------

# DBTITLE 1,Q8 — Observabilidad Operativa y Tendencia de la Bitácora del Pipeline Medallón (Gobierno & FinOps · CDO / Data Platform)
# MAGIC %sql
# MAGIC -- Responde a la CDO (Laura Hernández) y al equipo de Data Platform (Sección 8 del Documento Formal):
# MAGIC -- Volumetría procesada, tiempos de ejecución por capa Medallón y salud operativa del Lakeflow Job E7.4.
# MAGIC SELECT
# MAGIC   ejecucion_ts,
# MAGIC   capa,
# MAGIC   tabla,
# MAGIC   filas                                                AS filas_procesadas,
# MAGIC   duracion_seg,
# MAGIC   round(filas / nullif(duracion_seg, 0), 1)            AS throughput_filas_por_seg
# MAGIC FROM workspace.yelp_gov.pipeline_bitacora
# MAGIC ORDER BY ejecucion_ts DESC, capa ASC
# MAGIC LIMIT 50;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Configuración del Genie Space sobre `workspace.yelp_gold`
# MAGIC
# MAGIC ### 2.1. Tablas conectadas al Genie Space
# MAGIC * `workspace.yelp_gold.gld_kpi_negocio_mensual`
# MAGIC * `workspace.yelp_gold.gld_checkins_diarios`
# MAGIC * `workspace.yelp_gold.gld_salud_resenas`
# MAGIC * `workspace.yelp_gold.gld_cohortes_retencion_mensual`
# MAGIC * `workspace.yelp_gold.gld_dim_negocio`
# MAGIC * `workspace.yelp_gold.gld_dim_usuario`
# MAGIC * `workspace.yelp_gold.gld_dim_tiempo`
# MAGIC * `workspace.yelp_gold.gld_fact_resenas`
# MAGIC
# MAGIC ### 2.2. Instrucciones del Sistema y Sinónimos del Glosario E4 (Copiar en *Genie → Instructions*)
# MAGIC ```text
# MAGIC Contexto de Negocio — Plataforma YelpConnect (Grupo 03 · Data Evolution):
# MAGIC 1. "Rating oficial", "calificación justa" o "Rating Bayesiano (BWR)": usa SIEMPRE la columna `rating_bayesiano` de `workspace.yelp_gold.gld_kpi_negocio_mensual` (ponderado con umbral m=10 según ADR-02), nunca el promedio simple `rating_promedio`, salvo que el usuario pida explícitamente comparar ambos.
# MAGIC 2. "Afluencia", "visitas físicas", "tráfico en tienda" o "Check-in Válido (HAI)": consulta `checkins_validos` en `workspace.yelp_gold.gld_checkins_diarios` (deduplicados por ventana horaria de 60 minutos según ADR-01 / Acta 01).
# MAGIC 3. "Reseña Verificada": son los registros en `gld_fact_resenas` o `resenas_validas` en `gld_salud_resenas` que aprobaron el 100% de las reglas de calidad DQ-001 a DQ-015.
# MAGIC 4. "Reseña en Cuarentena", "spam", "bots" o "rechazos de calidad": consulta `resenas_cuarentena` en `workspace.yelp_gold.gld_salud_resenas` y calcula el Ratio de Salud RHR = `pct_salud`.
# MAGIC 5. "Retención de usuarios", "lealtad" o "Cohorte (CRR)": usa `retencion_pct` y `mes_relativo` en `workspace.yelp_gold.gld_cohortes_retencion_mensual` filtrando `mes_relativo > 0` cuando se pregunte por retención post-registro.
# MAGIC 6. "Comercio activo" o "negocio vigente": filtra siempre `es_actual = true` y `esta_abierto = true` en `workspace.yelp_gold.gld_dim_negocio`.
# MAGIC ```
# MAGIC
# MAGIC ### 2.3. Registro de Pruebas en Genie Space y Evidencia de Iteración Semántica
# MAGIC
# MAGIC | # | Pregunta en Lenguaje Natural formulada a Genie | Resultado en Genie | Observación / Iteración de Instrucción Aplicada |
# MAGIC | :--- | :--- | :--- | :--- |
# MAGIC | **P1** | *"¿Cuáles son los 10 comercios abiertos con mejor calificación oficial en la plataforma y cuántas reseñas verificadas tienen?"* | **Correcto (1.ª intención):** Genie cruzó `gld_kpi_negocio_mensual` con `gld_dim_negocio` (`esta_abierto = true AND es_actual = true`) ordenando por `avg(rating_bayesiano) DESC`. | La instrucción #1 evitó que Genie ordenara negocios con 1 sola reseña de 5.0 estrellas. |
# MAGIC | **P2** | *"¿Cómo ha evolucionado mensualmente la cantidad de visitas físicas (check-ins válidos) por categoría principal de negocio?"* | **Correcto (1.ª intención):** Genie agrupó `sum(checkins_validos)` de `gld_checkins_diarios` unido a `gld_dim_negocio.categoria_principal` por mes. | El sinónimo `"visitas físicas" -> checkins_validos` del Glosario E4 guió la selección exacta de tabla. |
# MAGIC | **P3** | *"¿Qué porcentaje de reseñas fueron enviadas a cuarentena cada mes y cuál es el ratio de salud RHR?"* | **Correcto (1.ª intención):** Consultó `gld_salud_resenas` calculando `sum(resenas_cuarentena)` y `avg(pct_salud)` por mes. | Alineado 1:1 con el requerimiento **REQ-03** de Rafael Nakamura. |
# MAGIC | **P4 (Caso de Fallo + Corrección)** | *"¿Cuál es la tasa de retención promedio de las cohortes de usuarios después de su registro?"* | **Fallo inicial:** En el primer intento sin la cláusula de exclusión del mes cero, Genie promedió `retencion_pct` incluyendo `mes_relativo = 0` (donde la retención es siempre `100.0%` por definición de alta), inflando el promedio. | **Corrección aplicada:** Se actualizó la Instrucción #5 en Genie agregando *"filtrando `mes_relativo > 0` cuando se pregunte por retención post-registro"*. Al re-preguntar, Genie aplicó `WHERE mes_relativo > 0` entregando la curva real de retención. |
# MAGIC
# MAGIC ---
# MAGIC ## 3. Storytelling Ejecutivo por Página del AI/BI Dashboard
# MAGIC
# MAGIC ### Slide-Resumen Página 1 · Negocio y Producto (*Stakeholder Principal: Andrew Collins, VP of Product*)
# MAGIC * **Mensaje Clave:** La adopción del **Rating Ponderado Bayesiano ($BWR$, ADR-02)** corrige la distorsión histórica del ranking público donde comercios nuevos con 1–2 reseñas desplazaban a establecimientos consolidados, mientras que la homologación horaria de check-ins (**ADR-01 / Acta 01**) concilia la métrica de tráfico físico entre Producto y Operaciones Comerciales (**Carla Romero**).
# MAGIC * **Decisión Habilitada:** Priorizar las categorías con caída en $BWR$ y otorgar la insignia *"Comercio Recomendado YelpConnect"* únicamente a negocios con $BWR \ge 4.2$ y tasa de retención recurrente comprobada (`Q1`, `Q4`, `Q5`, `Q7`).
# MAGIC
# MAGIC ### Slide-Resumen Página 2 · Gobierno, Calidad y Confianza (*Stakeholders: Laura Hernández, CDO & Rafael Nakamura, Trust & Safety*)
# MAGIC * **Mensaje Clave:** El **100% de las 106 columnas físicas** en Silver y Gold cuentan con documentación activa en el Diccionario E4, **7 entidades críticas** tienen asignación RACI formal en Unity Catalog (**E6**) y el **Quality Gate activo (E5)** aísla preventivamente las reseñas duplicadas o anómalas en `slv_review_cuarentena` antes de publicar Gold (`Q2`, `Q3`, `Q6`, `Q8`).
# MAGIC * **Decisión Habilitada:** Auditar las campañas con picos en cuarentena sin alterar el rating público de los usuarios y certificar ante el Comité Ejecutivo que el Lakehouse opera dentro del presupuesto FinOps ($< 22\%$ de cuota Serverless diaria).
# MAGIC
# MAGIC ---
# MAGIC ### Definition of Done (E8)
# MAGIC - [x] Consultas `Q1` a `Q8` operativas consumiendo exclusivamente `workspace.yelp_gold` y `workspace.yelp_gov`.
# MAGIC - [x] Cada consulta y visual declara explícitamente el requerimiento (`REQ-01` a `REQ-06`) y el stakeholder al que responde.
# MAGIC - [x] Genie Space documentado con 6 instrucciones semánticas del Glosario E4 + evidencia de iteración y corrección (`P4`).
# MAGIC - [x] Storytelling ejecutivo (1 slide-resumen por página explicando la decisión de negocio y gobierno que habilita).
