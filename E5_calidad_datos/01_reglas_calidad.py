# Databricks notebook source
# MAGIC %md
# MAGIC # E5 · Reglas de Calidad de Datos + Motor de Evaluación
# MAGIC **Se define en:** M2/M3 (reglas en lenguaje de negocio) · **Se implementa tras:** M6 (cuando ya hay datos en silver)
# MAGIC
# MAGIC Enfoque *rules-as-data*: las reglas viven en la tabla `yelp_gov.dq_rules` (no
# MAGIC hardcodeadas), un motor genérico las ejecuta y persiste resultados en
# MAGIC `yelp_gov.dq_results`. Ese historial alimenta el dashboard de E8 y el job de E7.
# MAGIC
# MAGIC **20 reglas (DQ-001 a DQ-020)** cubriendo las 6 dimensiones de calidad DAMA-DMBOK (validity, uniqueness,
# MAGIC completeness, consistency, accuracy y timeliness) y los dolores del caso: duplicidad de reseñas (Marketing),
# MAGIC check-ins inconsistentes (Ads), integridad referencial, golden record de negocios y las categorías
# MAGIC que alimentan los productos de Producto. Criticidad: **critical** = Quality Gate · **high** · **medium**. Nombres de tablas y columnas en inglés, como definió el equipo; descripciones en español.
# MAGIC Cada regla incluye su umbral justificado, la acción si falla y el Data Owner responsable.
# MAGIC El mismo catálogo está en `Reglas_calidad_v2.xlsx`, anexo del Documento Formal.
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la sección «Reglas de calidad de datos (definición en lenguaje de negocio)» del
# MAGIC > **Documento Formal de Arquitectura de Datos**. Aquí se implementan y ejecutan esas reglas: el documento define QUÉ debe cumplirse y por qué; la plataforma demuestra que se cumple.

# COMMAND ----------

# DBTITLE 1,Catálogo de reglas (20 reglas · 6 dimensiones · 7 tablas silver)
from pyspark.sql import Row

reglas = [
    # --- slv_review ---
    Row(rule_id='DQ-001', target_table='workspace.yelp_silver.slv_review',
        column_name='review_id', dimension='uniqueness',
        description='Cada reseña tiene identificador y aparece una sola vez (review_id único)',
        sql_expression='review_id IS NOT NULL AND review_id IN (SELECT review_id FROM workspace.yelp_silver.slv_review GROUP BY review_id HAVING count(*) = 1)',
        criticality='critical', threshold_pct=100.0,
        threshold_rationale='review_id es la clave primaria de la reseña. Una reseña repetida infla el volumen y altera el rating bayesiano de Producto. No hay margen aceptable: si falla, el defecto está en la deduplicación de silver.',
        failure_action='Bloquear la publicación en Gold y corregir la deduplicación en 02_silver_limpieza.',
        rule_owner='Andrew Collins – Producto'),
    Row(rule_id='DQ-002', target_table='workspace.yelp_silver.slv_review',
        column_name='*', dimension='uniqueness',
        description='Un usuario no publica el mismo texto dos veces sobre el mismo negocio. Las repetidas se marcan como no válidas en Gold (is_valid = false) y no bloquean la carga',
        sql_expression='review_id NOT IN (SELECT review_id FROM (  SELECT review_id, row_number() OVER (PARTITION BY user_id, business_id, md5(lower(trim(text))) ORDER BY review_date, review_id) AS rn  FROM workspace.yelp_silver.slv_review WHERE text IS NOT NULL AND length(trim(text)) > 0) WHERE rn > 1)',
        criticality='high', threshold_pct=99.5,
        threshold_rationale='Los textos repetidos del mismo usuario sobre el mismo negocio son reenvíos o spam de campaña. Se tolera hasta 0,5 % porque no bloquean la carga: Gold los excluye con is_valid = false. Un valor mayor sugiere una campaña coordinada.',
        failure_action='Marcar is_valid = false en Gold para que no cuenten en el rating. Si supera el umbral, alertar a Trust & Safety para investigación antifraude.',
        rule_owner='Andrew Collins – Producto; informa a Rafael Nakamura – Trust & Safety'),
    Row(rule_id='DQ-003', target_table='workspace.yelp_silver.slv_review',
        column_name='*', dimension='completeness',
        description='Toda reseña tiene autor, negocio, estrellas y fecha',
        sql_expression='user_id IS NOT NULL AND business_id IS NOT NULL AND stars IS NOT NULL AND review_date IS NOT NULL',
        criticality='critical', threshold_pct=100.0,
        threshold_rationale='Sin autor, negocio, estrellas o fecha, la reseña no puede entrar en ningún KPI de Producto: rating, usuarios activos ni tendencia mensual.',
        failure_action='Enviar a slv_review_quarantine con motivo DQ-003.',
        rule_owner='Andrew Collins – Producto'),
    Row(rule_id='DQ-004', target_table='workspace.yelp_silver.slv_review',
        column_name='stars', dimension='validity',
        description='Las estrellas son un número entero de 1 a 5',
        sql_expression='stars BETWEEN 1 AND 5 AND stars = floor(stars)',
        criticality='critical', threshold_pct=100.0,
        threshold_rationale='Yelp solo permite calificaciones enteras de 1 a 5. Un valor fuera de esa escala distorsiona el rating bayesiano y cualquier promedio.',
        failure_action='Enviar a slv_review_quarantine con motivo DQ-004.',
        rule_owner='Andrew Collins – Producto'),
    Row(rule_id='DQ-005', target_table='workspace.yelp_silver.slv_review',
        column_name='review_date', dimension='validity',
        description='La fecha de la reseña no es futura ni anterior a 2004, año de fundación de Yelp',
        sql_expression="review_date >= DATE'2004-01-01' AND review_date <= current_date()",
        criticality='high', threshold_pct=100.0,
        threshold_rationale='Yelp se fundó en 2004 y una reseña no puede tener fecha futura. Una fecha imposible coloca la reseña en el mes equivocado y altera las tendencias mensuales y las cohortes.',
        failure_action='Enviar a slv_review_quarantine con motivo DQ-005.',
        rule_owner='Andrew Collins – Producto'),
    Row(rule_id='DQ-006', target_table='workspace.yelp_silver.slv_review',
        column_name='business_id', dimension='consistency',
        description='Toda reseña corresponde a un negocio existente en la muestra',
        sql_expression='business_id IN (SELECT business_id FROM workspace.yelp_silver.slv_business)',
        criticality='critical', threshold_pct=100.0,
        threshold_rationale='Una reseña sin negocio en la muestra no puede asignarse a ningún rating. Es una regla de guarda: la carga ya filtra por negocios de Philadelphia, y la regla confirma que ese filtro siguió funcionando.',
        failure_action='Enviar a slv_review_quarantine y revisar el script de muestreo.',
        rule_owner='Andrew Collins – Producto'),
    Row(rule_id='DQ-015', target_table='workspace.yelp_silver.slv_review',
        column_name='loaded_at', dimension='timeliness',
        description='Silver incluye la última carga de Bronze (los datos publicados no están atrasados)',
        sql_expression='(SELECT max(loaded_at) FROM workspace.yelp_silver.slv_review) >= (SELECT max(_ingested_at) FROM workspace.yelp_bronze.brz_review)',
        criticality='high', threshold_pct=100.0,
        threshold_rationale='Si silver no incluye la última carga, el dashboard muestra datos atrasados. Se evalúa sobre la tabla completa y no fila por fila, para que funcione con ingesta incremental: el resultado es 100 % o 0 %.',
        failure_action='Volver a ejecutar el job. Si persiste, alertar al Ingeniero de datos.',
        rule_owner='Laura Hernández – CDO (Data Platform)'),
    Row(rule_id='DQ-016', target_table='workspace.yelp_silver.slv_review',
        column_name='user_id', dimension='consistency',
        description='Toda reseña corresponde a un usuario existente',
        sql_expression='user_id IN (SELECT user_id FROM workspace.yelp_silver.slv_user)',
        criticality='critical', threshold_pct=100.0,
        threshold_rationale='Sin un autor existente no se puede contar al usuario activo ni ubicarlo en su cohorte. Se separó de DQ-003 para distinguir un dato faltante de una referencia rota.',
        failure_action='Enviar a slv_review_quarantine con motivo DQ-016.',
        rule_owner='Andrew Collins – Producto'),
    # --- slv_business ---
    Row(rule_id='DQ-007', target_table='workspace.yelp_silver.slv_business',
        column_name='stars', dimension='consistency',
        description='La calificación declarada del negocio no difiere en más de 0,5 estrellas del promedio de sus reseñas',
        sql_expression='business_id NOT IN (SELECT b.business_id FROM workspace.yelp_silver.slv_business b JOIN (SELECT business_id, avg(stars) AS prom FROM workspace.yelp_silver.slv_review GROUP BY business_id) r ON b.business_id = r.business_id WHERE abs(b.stars - r.prom) > 0.5)',
        criticality='medium', threshold_pct=90.0,
        threshold_rationale='Las estrellas declaradas por Yelp vienen redondeadas a medias estrellas y la muestra no incluye todas las reseñas históricas, así que una diferencia moderada es esperable. El 10 % tolerado corresponde a negocios con pocas reseñas en la muestra: es el mismo problema que corrige el rating bayesiano (m = 25).',
        failure_action='Aceptar el riesgo documentado: el rating oficial de Producto es el bayesiano, no el declarado. Si baja de 90 %, revisar si la muestra quedó incompleta.',
        rule_owner='Andrew Collins – Producto'),
    Row(rule_id='DQ-008', target_table='workspace.yelp_silver.slv_business',
        column_name='review_count', dimension='consistency',
        description='Las reseñas presentes en la muestra no superan las reseñas declaradas del negocio',
        sql_expression='business_id NOT IN (SELECT b.business_id FROM workspace.yelp_silver.slv_business b JOIN (SELECT business_id, count(*) AS n FROM workspace.yelp_silver.slv_review GROUP BY business_id) r ON b.business_id = r.business_id WHERE r.n > b.review_count)',
        criticality='medium', threshold_pct=98.0,
        threshold_rationale='La muestra solo puede contener una parte de las reseñas declaradas por el negocio. Se toleran 2 % de excepciones porque Yelp actualiza review_count en otro momento que la exportación de reseñas. Más que eso indica reseñas duplicadas o asignadas al negocio equivocado.',
        failure_action='Aceptar el riesgo hasta el umbral. Si se supera, revisar DQ-002 y DQ-006.',
        rule_owner='Andrew Collins – Producto'),
    Row(rule_id='DQ-011', target_table='workspace.yelp_silver.slv_business',
        column_name='*', dimension='completeness',
        description='Todo negocio tiene nombre, dirección, ciudad y coordenadas',
        sql_expression='name IS NOT NULL AND address IS NOT NULL AND city IS NOT NULL AND latitude IS NOT NULL AND longitude IS NOT NULL',
        criticality='high', threshold_pct=98.0,
        threshold_rationale='Producto necesita nombre y ubicación para mostrar el negocio y ubicarlo en el mapa. Se tolera 2 % porque Yelp registra negocios sin dirección física, como servicios a domicilio; esos se excluyen solo de los visuales geográficos.',
        failure_action='Aceptar el riesgo hasta el umbral y excluir esos negocios de los mapas.',
        rule_owner='Andrew Collins – Producto'),
    Row(rule_id='DQ-012', target_table='workspace.yelp_silver.slv_business',
        column_name='*', dimension='uniqueness',
        description='Un mismo negocio físico tiene un solo identificador maestro (golden record)',
        sql_expression="address IS NULL OR concat_ws('|', lower(regexp_replace(name, '\\\\s+', ' ')), lower(regexp_replace(address, '\\\\s+', ' ')), coalesce(postal_code, '')) NOT IN (  SELECT concat_ws('|', lower(regexp_replace(name, '\\\\s+', ' ')), lower(regexp_replace(address, '\\\\s+', ' ')), coalesce(postal_code, '')) FROM workspace.yelp_silver.slv_business WHERE address IS NOT NULL  GROUP BY 1 HAVING count(DISTINCT master_business_id) > 1)",
        criticality='high', threshold_pct=99.0,
        threshold_rationale='El golden record debe asignar un solo master_business_id por negocio físico. Se tolera 1 % porque sucursales legítimas pueden compartir nombre y dirección, por ejemplo en un centro comercial.',
        failure_action='Revisión manual del Steward: consolidar el registro o documentar la excepción.',
        rule_owner='Andrew Collins – Producto'),
    Row(rule_id='DQ-013', target_table='workspace.yelp_silver.slv_business',
        column_name='latitude', dimension='accuracy',
        description='Las coordenadas del negocio caen dentro del área de Philadelphia, la ciudad muestreada',
        sql_expression='latitude BETWEEN 39.80 AND 40.20 AND longitude BETWEEN -75.40 AND -74.90',
        criticality='medium', threshold_pct=99.0,
        threshold_rationale='La muestra corresponde solo a Philadelphia. Coordenadas fuera del área indican un error de geocodificación. Se tolera 1 % para negocios en el límite del área metropolitana.',
        failure_action='Excluir de los visuales geográficos y reportar a Producto.',
        rule_owner='Andrew Collins – Producto'),
    Row(rule_id='DQ-017', target_table='workspace.yelp_silver.slv_business',
        column_name='business_id', dimension='completeness',
        description='Todo negocio tiene al menos una categoría asignada',
        sql_expression='business_id IN (SELECT business_id FROM workspace.yelp_silver.slv_business_category)',
        criticality='high', threshold_pct=98.0,
        threshold_rationale='El producto 3 (experiencia por categoría) solo puede analizar negocios categorizados. Se tolera 2 % porque el dataset de Yelp incluye negocios sin categorías: quedan fuera del análisis por categoría, pero sí cuentan en el producto 1. Umbral inicial: confirmarlo con la primera medición.',
        failure_action='Aceptar el riesgo hasta el umbral y excluir esos negocios del análisis por categoría.',
        rule_owner='Andrew Collins – Producto'),
    # --- slv_checkin ---
    Row(rule_id='DQ-009', target_table='workspace.yelp_silver.slv_checkin',
        column_name='checkin_at', dimension='validity',
        description='Cada fecha de check-in tiene formato de fecha y hora válido (las inválidas se descartan al cargar)',
        sql_expression="checkin_at IS NOT NULL AND checkin_at >= TIMESTAMP'2004-01-01 00:00:00' AND checkin_at <= current_timestamp()",
        criticality='high', threshold_pct=100.0,
        threshold_rationale='Es una regla de guarda: las fechas ilegibles se descartan al convertir la lista de texto del JSON. Después de esa conversión no debería quedar ninguna fecha nula o fuera de rango; cualquier fallo indica que la conversión cambió. Por eso el umbral pasa de 99,9 a 100.',
        failure_action='Excluir del conteo de visitas y revisar la conversión en 02_silver_limpieza.',
        rule_owner='Carla Romero – Operaciones Comerciales (Ads)'),
    Row(rule_id='DQ-010', target_table='workspace.yelp_silver.slv_checkin',
        column_name='*', dimension='uniqueness',
        description='Check-in válido: no se cuentan dos marcas del mismo negocio con exactamente la misma fecha y hora (duplicado técnico). Los repetidos quedan con is_duplicate = true',
        sql_expression='NOT is_duplicate',
        criticality='high', threshold_pct=99.0,
        threshold_rationale='Las marcas idénticas son reenvíos técnicos del registro. Se tolera 1 % porque se excluyen del conteo sin perder datos; un valor mayor indica un problema en la fuente. Como el check-in no identifica al usuario, agrupar por ventana de 60 minutos uniría visitas de personas distintas: por eso el conteo oficial (checkin_count) usa fecha y hora exactas y las horas con actividad (active_hour_count) quedan como métrica complementaria para Ads. Pendiente de aprobación conjunta de Producto y Ads.',
        failure_action='Excluir del conteo oficial de visitas (is_duplicate = true). Si supera el umbral, revisar la fuente con el Ingeniero de datos.',
        rule_owner='Carla Romero – Operaciones Comerciales (Ads); consulta a Andrew Collins – Producto'),
    # --- slv_user ---
    Row(rule_id='DQ-014', target_table='workspace.yelp_silver.slv_user',
        column_name='yelping_since', dimension='consistency',
        description='La fecha de alta del usuario no es posterior a su primera reseña',
        sql_expression='user_id NOT IN (SELECT u.user_id FROM workspace.yelp_silver.slv_user u JOIN (SELECT user_id, min(review_date) AS primera FROM workspace.yelp_silver.slv_review GROUP BY user_id) r ON u.user_id = r.user_id WHERE u.yelping_since > r.primera)',
        criticality='medium', threshold_pct=99.0,
        threshold_rationale='Un usuario no puede reseñar antes de registrarse. Se tolera 1 % por diferencias de zona horaria entre la fecha de alta y la de la reseña. Afecta el cálculo de cohortes de Producto.',
        failure_action='Aceptar el riesgo hasta el umbral. Si se supera, usar la fecha de la primera reseña como fecha de alta para la cohorte.',
        rule_owner='Rafael Nakamura – Trust & Safety; informa a Andrew Collins – Producto'),
    # --- slv_business_category ---
    Row(rule_id='DQ-018', target_table='workspace.yelp_silver.slv_business_category',
        column_name='*', dimension='consistency',
        description='Cada relación negocio-categoría apunta a un negocio y una categoría existentes',
        sql_expression='business_id IN (SELECT business_id FROM workspace.yelp_silver.slv_business) AND category_id IN (SELECT category_id FROM workspace.yelp_silver.slv_category)',
        criticality='critical', threshold_pct=100.0,
        threshold_rationale='Una relación rota cuenta negocios en categorías inexistentes, o al revés, y altera todos los indicadores del producto 3.',
        failure_action='Bloquear la actualización de gld_agg_category_month y corregir el pipeline de silver.',
        rule_owner='Andrew Collins – Producto'),
    # --- slv_category ---
    Row(rule_id='DQ-019', target_table='workspace.yelp_silver.slv_category',
        column_name='*', dimension='uniqueness',
        description='Cada categoría aparece una sola vez en el catálogo, sin variantes de mayúsculas o espacios',
        sql_expression='name IS NOT NULL AND lower(trim(name)) IN (SELECT lower(trim(name)) FROM workspace.yelp_silver.slv_category GROUP BY lower(trim(name)) HAVING count(*) = 1)',
        criticality='high', threshold_pct=100.0,
        threshold_rationale='Si una categoría aparece dos veces con distinta escritura, sus negocios se reparten entre ambas y el ranking por categoría sale incorrecto.',
        failure_action='Consolidar las variantes en el catálogo de categorías antes de publicar Gold.',
        rule_owner='Andrew Collins – Producto'),
    # --- slv_tip ---
    Row(rule_id='DQ-020', target_table='workspace.yelp_silver.slv_tip',
        column_name='*', dimension='consistency',
        description='Todo tip tiene un autor y un negocio existentes y una fecha válida',
        sql_expression="user_id IN (SELECT user_id FROM workspace.yelp_silver.slv_user) AND business_id IN (SELECT business_id FROM workspace.yelp_silver.slv_business) AND tip_date BETWEEN DATE'2004-01-01' AND current_date()",
        criticality='high', threshold_pct=100.0,
        threshold_rationale='Los tips no cuentan para Usuario activo (que exige una reseña válida), pero sí para Usuario contribuidor (is_contributor), tip_count e interaction_count del producto 2. Un tip sin autor, negocio o fecha válidos altera esas métricas.',
        failure_action='Excluir del conteo de actividad y revisar 02_silver_limpieza.',
        rule_owner='Andrew Collins – Producto'),
]
spark.createDataFrame(reglas).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.yelp_gov.dq_rules")
spark.sql("COMMENT ON TABLE workspace.yelp_gov.dq_rules IS 'E5 · Catálogo gobernado de reglas de calidad de datos (DQ-001 a DQ-020)'")
COMENTARIOS_DQ = {
    "rule_id": "Identificador único de la regla (DQ-001 a DQ-020)",
    "target_table": "Tabla física evaluada en workspace.yelp_silver",
    "column_name": "Columna evaluada (o * si la regla es a nivel de fila)",
    "dimension": "Dimensión DQ: validity, uniqueness, completeness, consistency, accuracy o timeliness",
    "description": "Regla en lenguaje de negocio aprobada por el Data Owner",
    "sql_expression": "Predicado SQL que debe cumplirse sobre la tabla objetivo",
    "criticality": "critical, high o medium; las reglas critical actúan como Quality Gate en E7",
    "threshold_pct": "Porcentaje mínimo de filas que deben cumplir la regla (0 a 100)",
    "threshold_rationale": "Justificación de negocio del umbral",
    "failure_action": "Acción acordada si la regla no alcanza su umbral",
    "rule_owner": "Data Owner responsable de la regla",
}
for col, txt in COMENTARIOS_DQ.items():
    spark.sql(f"ALTER TABLE workspace.yelp_gov.dq_rules ALTER COLUMN {col} COMMENT '{txt}'")
display(spark.table("workspace.yelp_gov.dq_rules").orderBy("rule_id"))

# COMMAND ----------

# DBTITLE 1,Motor genérico de evaluación (no modificar — extender si hace falta)
from pyspark.sql import functions as F

def evaluar_reglas():
    resultados = []
    for r in spark.table("workspace.yelp_gov.dq_rules").collect():
        total = spark.table(r.target_table).count()
        ok = spark.sql(f"SELECT count(*) c FROM {r.target_table} WHERE {r.sql_expression}").first().c
        # La unicidad requiere tratamiento especial: comparar filas vs distintos
        if r.dimension == "uniqueness" and r.column_name != "*":
            ok = spark.sql(f"SELECT count(DISTINCT {r.column_name}) c FROM {r.target_table}").first().c
        pct = round(100.0 * ok / total, 2) if total else 0.0
        resultados.append((r.rule_id, r.target_table, total, ok, pct, pct >= r.threshold_pct))
    df = spark.createDataFrame(resultados,
        "rule_id STRING, target_table STRING, total_rows BIGINT, passed_rows BIGINT, compliance_pct DOUBLE, passed BOOLEAN"
    ).withColumn("executed_at", F.current_timestamp())
    df.select("executed_at","rule_id","target_table","total_rows","passed_rows","compliance_pct","passed") \
      .write.mode("append").saveAsTable("workspace.yelp_gov.dq_results")
    return df

display(evaluar_reglas())

# COMMAND ----------

# DBTITLE 1,Scorecard de calidad (insumo directo del dashboard E8)
# MAGIC %sql
# MAGIC SELECT r.dimension,
# MAGIC        count(*)                                    AS reglas,
# MAGIC        sum(CASE WHEN res.passed THEN 1 ELSE 0 END) AS reglas_ok,
# MAGIC        round(avg(res.compliance_pct), 2)           AS cumplimiento_promedio
# MAGIC FROM workspace.yelp_gov.dq_results res
# MAGIC JOIN workspace.yelp_gov.dq_rules r USING (rule_id)
# MAGIC WHERE res.executed_at = (SELECT max(executed_at) FROM workspace.yelp_gov.dq_results)
# MAGIC GROUP BY r.dimension ORDER BY cumplimiento_promedio;

# COMMAND ----------

# DBTITLE 1,Reglas fallidas de la última corrida y decisión documentada (extensión)
# MAGIC %sql
# MAGIC SELECT res.rule_id, r.dimension, r.criticality, r.threshold_pct, res.compliance_pct,
# MAGIC        r.failure_action, r.rule_owner
# MAGIC FROM workspace.yelp_gov.dq_results res
# MAGIC JOIN workspace.yelp_gov.dq_rules r USING (rule_id)
# MAGIC WHERE res.executed_at = (SELECT max(executed_at) FROM workspace.yelp_gov.dq_results)
# MAGIC   AND NOT res.passed
# MAGIC ORDER BY CASE r.criticality WHEN 'critical' THEN 1 WHEN 'high' THEN 2 ELSE 3 END, res.rule_id;

# COMMAND ----------

# DBTITLE 1,Historial de corridas (≥2 en fechas distintas) (extensión)
# MAGIC %sql
# MAGIC SELECT date(executed_at) AS fecha, executed_at,
# MAGIC        count(*) AS reglas_evaluadas,
# MAGIC        sum(CASE WHEN passed THEN 1 ELSE 0 END) AS reglas_ok
# MAGIC FROM workspace.yelp_gov.dq_results
# MAGIC GROUP BY date(executed_at), executed_at
# MAGIC ORDER BY executed_at DESC;

# COMMAND ----------

# DBTITLE 1,Bonus — calidad preventiva con constraint CHECK (extensión, desactivada por defecto)
# Detectiva: el motor mide después de cargar. Preventiva: Delta rechaza en la escritura las filas que violan el CHECK.
# Activar solo después de que DQ-004 esté en 100 %: si hay filas que violan la condición, el ALTER falla,
# y una vez activo, cualquier carga con estrellas fuera de escala hace fallar el pipeline.
APLICAR_CHECK = False
if APLICAR_CHECK:
    spark.sql("ALTER TABLE workspace.yelp_silver.slv_review DROP CONSTRAINT IF EXISTS chk_stars_entero_1_5")
    spark.sql("ALTER TABLE workspace.yelp_silver.slv_review ADD CONSTRAINT chk_stars_entero_1_5 "
              "CHECK (stars BETWEEN 1 AND 5 AND stars = floor(stars))")
    print("Constraint preventiva activa sobre slv_review.stars (DQ-004)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definition of Done (E5)
# MAGIC - [x] ≥12 reglas en las 6 dimensiones, con criticidad y umbral justificados (20 reglas; columna `threshold_rationale`).
# MAGIC - [ ] Motor ejecutado con historial en `dq_results` (≥2 corridas en fechas distintas). Se obtiene ejecutando el notebook en dos días distintos; no se insertan corridas artificiales.
# MAGIC - [x] Para cada regla FALLIDA: decisión documentada (cuarentena, corrección en pipeline, o aceptación del riesgo con firma del "Data Owner") en `failure_action` y `rule_owner`, visibles en la celda de reglas fallidas.
# MAGIC - [ ] Bonus: agregar constraint `CHECK` o expectativa en el pipeline E7 para la regla más crítica (celda anterior, activar con `APLICAR_CHECK = True`).
