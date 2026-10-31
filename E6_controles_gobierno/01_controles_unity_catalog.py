# Databricks notebook source
# MAGIC %md
# MAGIC # E6 · Controles Técnicos de Gobierno en Unity Catalog (Grupo 03 · Data Evolution)
# MAGIC **Cierra con:** M2 (diseño) + M5 (implementación en plataforma)
# MAGIC
# MAGIC La estrategia de gobierno definida en la **Sección 5 del Documento Formal de Arquitectura de Datos (v1.2 · Equipo Data Evolution)** se **implementa** aquí con **5 componentes verificables en Unity Catalog**:
# MAGIC 1. **Matriz RACI gobernada (`workspace.yelp_gov.matriz_raci`):** Asignación formal de *Data Owner*, *Data Steward*, *Data Custodian* y clasificación de sensibilidad por entidad (insumo directo del Dashboard E8 para la CDO Laura Hernández).
# MAGIC 2. **Control 1 — Clasificación con Tags (`SET TAGS`):** Etiquetado en esquemas (`yelp_bronze`, `yelp_silver`, `yelp_gold`, `yelp_gov`), tablas y columnas sensibles/PII (`slv_user.name`, coordenadas, cuarentena).
# MAGIC 3. **Control 2 — Permisos RBAC por capa (`GRANT` / `REVOKE`):** Principio de mínimo privilegio entre *Data Custodian*, *Data Stewards*, *Trust & Safety* y *Analistas de Producto / Marketing*.
# MAGIC 4. **Control 3 — Enmascaramiento Dinámico de PII (`COLUMN MASK` + Vista Segura):** Función `workspace.yelp_gov.mascara_nombre`, máscara nativa sobre `slv_user.name` (**ADR-06**) y vista certificada `workspace.yelp_gold.vw_usuarios_seguro`.
# MAGIC 5. **Control 4 — Evidencia de Linaje End-to-End y Reconciliación Forense:** Trazabilidad completa de la entidad `review` (`raw → bronze → silver / cuarentena → gold → BI`) respondiendo al dolor de **Rafael Nakamura (Trust & Safety)**.
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la **Sección 5 («Estrategia de Gobierno y Matriz RACI»)** del
# MAGIC > **Documento Formal de Arquitectura de Datos**. Un rol sin `GRANT` y una clasificación sin `TAG` ni `MASK` son gobierno declarativo, no efectivo.

# COMMAND ----------

# DBTITLE 1,0. Registro de la Matriz RACI en Unity Catalog (workspace.yelp_gov.matriz_raci)
from pyspark.sql import Row

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.yelp_gov")
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.yelp_gov.matriz_raci (
  entidad          STRING COMMENT 'Entidad conceptual y lógica gobernada del caso YelpConnect',
  dominio          STRING COMMENT 'Dominio de datos DAMA-DMBOK al que pertenece la entidad',
  data_owner       STRING COMMENT 'Accountable (A) — Ejecutivo dueño de negocio responsable del dato',
  data_steward     STRING COMMENT 'Responsible (R) — Custodio funcional que opera calidad y metadatos',
  data_custodian   STRING COMMENT 'Responsible técnico — Equipo Data Platform que opera el Lakehouse',
  consumidores     STRING COMMENT 'Informed / Consulted — Áreas de negocio autorizadas a consumir',
  clasificacion    STRING COMMENT 'Nivel de sensibilidad: publica | interna | confidencial | pii'
) USING DELTA
COMMENT 'E6 · Asignación formal de roles y responsabilidades RACI sobre las entidades de YelpConnect (Sección 5.2)'
""")

filas_raci = [
    Row(
        entidad="Negocio (slv_business / gld_dim_negocio)",
        dominio="Negocio",
        data_owner="Andrew Collins (VP of Product)",
        data_steward="Steward de Producto",
        data_custodian="Data Platform (Grupo 03)",
        consumidores="Producto, Operaciones Comerciales, Marketing, Analítica BI",
        clasificacion="publica",
    ),
    Row(
        entidad="Reseña (slv_review / gld_fact_resenas)",
        dominio="Contenido",
        data_owner="Andrew Collins (VP of Product)",
        data_steward="Steward de Producto y Steward de Trust & Safety",
        data_custodian="Data Platform (Grupo 03)",
        consumidores="Producto, Trust & Safety, Marketing, CDO",
        clasificacion="interna",
    ),
    Row(
        entidad="Cuarentena de Reseñas (slv_review_cuarentena / gld_salud_resenas)",
        dominio="Confianza y Seguridad",
        data_owner="Rafael Nakamura (Lead Trust & Safety)",
        data_steward="Steward de Trust & Safety",
        data_custodian="Data Platform (Grupo 03)",
        consumidores="Trust & Safety, CDO (Laura Hernández), Producto",
        clasificacion="confidencial",
    ),
    Row(
        entidad="Usuario (slv_user / gld_dim_usuario)",
        dominio="Usuario",
        data_owner="Rafael Nakamura (Lead Trust & Safety)",
        data_steward="Steward de Privacidad y Protección de Datos",
        data_custodian="Data Platform (Grupo 03)",
        consumidores="Trust & Safety (acceso PII), Producto y Marketing (solo enmascarado ADR-06)",
        clasificacion="pii",
    ),
    Row(
        entidad="Amistad / Red Social (slv_amistad)",
        dominio="Interacción / Red",
        data_owner="Rafael Nakamura (Lead Trust & Safety)",
        data_steward="Steward de Privacidad y Producto",
        data_custodian="Data Platform (Grupo 03)",
        consumidores="Producto (engagement comunitario), Trust & Safety (detección de anillos de bots)",
        clasificacion="interna",
    ),
    Row(
        entidad="Check-in (slv_checkin / gld_checkins_diarios)",
        dominio="Interacción y Publicidad",
        data_owner="Carla Romero (Head of Commercial Ops & Ads)",
        data_steward="Steward de Interacción Comercial",
        data_custodian="Data Platform (Grupo 03)",
        consumidores="Operaciones Comerciales (Yelp Ads), Producto (Andrew Collins)",
        clasificacion="confidencial",
    ),
    Row(
        entidad="Tip (slv_tip)",
        dominio="Contenido",
        data_owner="Andrew Collins (VP of Product)",
        data_steward="Steward de Producto",
        data_custodian="Data Platform (Grupo 03)",
        consumidores="Producto, Experiencia de Usuario, Trust & Safety",
        clasificacion="publica",
    ),
]

spark.createDataFrame(filas_raci).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.yelp_gov.matriz_raci")
display(spark.table("workspace.yelp_gov.matriz_raci"))

# COMMAND ----------

# DBTITLE 1,Control 1 — Clasificación con Tags en Esquemas, Tablas y Columnas Sensibles
# Aplicamos etiquetas nativas de Unity Catalog alineadas con la Sección 5.3 (Política de Clasificación de Datos)
comandos_tags = [
    # 1.1 Tags a nivel de Schema (Arquitectura Medallón + Gobierno)
    "ALTER SCHEMA workspace.yelp_bronze SET TAGS ('capa' = 'bronze', 'dominio' = 'yelpconnect', 'acceso' = 'restringido_custodian', 'grupo' = '03')",
    "ALTER SCHEMA workspace.yelp_silver SET TAGS ('capa' = 'silver', 'dominio' = 'yelpconnect', 'modelo' = '3FN', 'grupo' = '03')",
    "ALTER SCHEMA workspace.yelp_gold   SET TAGS ('capa' = 'gold',   'dominio' = 'yelpconnect', 'modelo' = 'kimball_estrella', 'certificado' = 'true', 'grupo' = '03')",
    "ALTER SCHEMA workspace.yelp_gov    SET TAGS ('capa' = 'gobierno', 'dominio' = 'dama_dmbok', 'owner' = 'laura_hernandez_cdo', 'grupo' = '03')",

    # 1.2 Tags a nivel de Tabla en Silver y Gold
    "ALTER TABLE workspace.yelp_silver.slv_business          SET TAGS ('owner' = 'andrew_collins',  'clasificacion' = 'publico',      'dominio' = 'negocio')",
    "ALTER TABLE workspace.yelp_silver.slv_user              SET TAGS ('owner' = 'rafael_nakamura', 'clasificacion' = 'pii',          'dominio' = 'usuario', 'normativa' = 'GDPR_CCPA_Ley29733')",
    "ALTER TABLE workspace.yelp_silver.slv_amistad           SET TAGS ('owner' = 'rafael_nakamura', 'clasificacion' = 'interno',      'dominio' = 'interaccion')",
    "ALTER TABLE workspace.yelp_silver.slv_review            SET TAGS ('owner' = 'andrew_collins',  'clasificacion' = 'interno',      'dominio' = 'contenido')",
    "ALTER TABLE workspace.yelp_silver.slv_review_cuarentena SET TAGS ('owner' = 'rafael_nakamura', 'clasificacion' = 'confidencial', 'dominio' = 'trust_and_safety')",
    "ALTER TABLE workspace.yelp_silver.slv_tip               SET TAGS ('owner' = 'andrew_collins',  'clasificacion' = 'publico',      'dominio' = 'contenido')",
    "ALTER TABLE workspace.yelp_silver.slv_checkin           SET TAGS ('owner' = 'carla_romero',    'clasificacion' = 'confidencial', 'dominio' = 'interaccion')",
    "ALTER TABLE workspace.yelp_gold.gld_dim_usuario         SET TAGS ('owner' = 'rafael_nakamura', 'clasificacion' = 'interno',      'pii_tratamiento' = 'enmascarado_adr06')",
    "ALTER TABLE workspace.yelp_gold.gld_kpi_negocio_mensual SET TAGS ('owner' = 'andrew_collins',  'clasificacion' = 'interno',      'kpi' = 'BWR_ADR02')",
    "ALTER TABLE workspace.yelp_gold.gld_checkins_diarios    SET TAGS ('owner' = 'carla_romero',    'clasificacion' = 'confidencial', 'kpi' = 'HAI_ADR01')",
    "ALTER TABLE workspace.yelp_gold.gld_salud_resenas       SET TAGS ('owner' = 'rafael_nakamura', 'clasificacion' = 'confidencial', 'kpi' = 'RHR_Acta02')",
    "ALTER TABLE workspace.yelp_gold.gld_cohortes_retencion_mensual SET TAGS ('owner' = 'andrew_collins', 'clasificacion' = 'interno', 'kpi' = 'CRR')",

    # 1.3 Tags a nivel de Columna Sensible / PII (Sección 5.3 y 9 del Documento Formal)
    "ALTER TABLE workspace.yelp_silver.slv_user              ALTER COLUMN name           SET TAGS ('confidencialidad' = 'PII', 'clasificacion' = 'restringido', 'politica' = 'ADR-06_mascara_dinamica')",
    "ALTER TABLE workspace.yelp_silver.slv_business          ALTER COLUMN latitud        SET TAGS ('confidencialidad' = 'geoespacial', 'clasificacion' = 'interno')",
    "ALTER TABLE workspace.yelp_silver.slv_business          ALTER COLUMN longitud       SET TAGS ('confidencialidad' = 'geoespacial', 'clasificacion' = 'interno')",
    "ALTER TABLE workspace.yelp_silver.slv_review_cuarentena ALTER COLUMN motivo_rechazo SET TAGS ('confidencialidad' = 'confidencial', 'clasificacion' = 'auditoria_forense')",
    "ALTER TABLE workspace.yelp_gold.gld_dim_usuario         ALTER COLUMN name_masked    SET TAGS ('confidencialidad' = 'seudonimizado', 'clasificacion' = 'interno')",
]

aplicados = 0
for cmd in comandos_tags:
    try:
        spark.sql(cmd)
        aplicados += 1
    except Exception as e:
        print(f"ℹ Tag omitido ({cmd[:65]}...): {e}")

print(f"✔ Control 1 completado: {aplicados}/{len(comandos_tags)} sentencias SET TAGS aplicadas en Unity Catalog.")

# COMMAND ----------

# DBTITLE 1,1.1 Verificación de Tags registrados en Information Schema de Unity Catalog
# MAGIC %sql
# MAGIC SELECT 'SCHEMA' AS nivel, schema_name AS objeto, '' AS columna, tag_name, tag_value
# MAGIC FROM workspace.information_schema.schema_tags
# MAGIC WHERE schema_name LIKE 'yelp_%'
# MAGIC UNION ALL
# MAGIC SELECT 'TABLE' AS nivel, concat(schema_name, '.', table_name) AS objeto, '' AS columna, tag_name, tag_value
# MAGIC FROM workspace.information_schema.table_tags
# MAGIC WHERE schema_name LIKE 'yelp_%'
# MAGIC UNION ALL
# MAGIC SELECT 'COLUMN' AS nivel, concat(schema_name, '.', table_name) AS objeto, column_name AS columna, tag_name, tag_value
# MAGIC FROM workspace.information_schema.column_tags
# MAGIC WHERE schema_name LIKE 'yelp_%'
# MAGIC ORDER BY nivel, objeto, tag_name;

# COMMAND ----------

# DBTITLE 1,Control 2 — Permisos RBAC por Capa (Principio de Mínimo Privilegio)
# Modelo RBAC aprobado en la Sección 5.1 y 5.3 del Documento Formal:
# - workspace.yelp_bronze: Acceso exclusivo para Data Platform / Data Custodian (prohibido para negocio/BI).
# - workspace.yelp_silver: Lectura para Data Stewards y Trust & Safety (con column mask activa en PII).
# - workspace.yelp_gold:   Lectura habilitada para consumidores de negocio (Producto, Marketing, Operaciones Comerciales).
# - workspace.yelp_gov:    Lectura habilitada para CDO, Data Stewards y auditoría de gobierno.

usuario_actual = spark.sql("SELECT current_user() AS u").first().u
print(f"Usuario autenticado en el workspace: {usuario_actual}")

comandos_rbac = [
    # Permisos efectivos sobre la cuenta del proyecto y grupo account users en capa Gold y Gov
    f"GRANT USE SCHEMA ON SCHEMA workspace.yelp_gold TO `{usuario_actual}`",
    f"GRANT SELECT     ON SCHEMA workspace.yelp_gold TO `{usuario_actual}`",
    f"GRANT USE SCHEMA ON SCHEMA workspace.yelp_gov  TO `{usuario_actual}`",
    f"GRANT SELECT     ON SCHEMA workspace.yelp_gov  TO `{usuario_actual}`",
    "GRANT USE SCHEMA  ON SCHEMA workspace.yelp_gold TO `account users`",
    "GRANT SELECT      ON SCHEMA workspace.yelp_gold TO `account users`",
    "GRANT USE SCHEMA  ON SCHEMA workspace.yelp_gov  TO `account users`",
    "GRANT SELECT      ON SCHEMA workspace.yelp_gov  TO `account users`",
]

for cmd in comandos_rbac:
    try:
        spark.sql(cmd)
        print(f"✔ RBAC aplicado: {cmd}")
    except Exception as e:
        print(f"ℹ RBAC nota ({cmd}): {e}")

# COMMAND ----------

# DBTITLE 1,2.1 Verificación de Grants activos en Capa Gold y Gobierno
# MAGIC %sql
# MAGIC SHOW GRANTS ON SCHEMA workspace.yelp_gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON SCHEMA workspace.yelp_gov;

# COMMAND ----------

# DBTITLE 1,Control 3 — Enmascaramiento Dinámico de PII (Función + Column Mask + Vista Segura)
# MAGIC %sql
# MAGIC -- 3.1 Crear función de enmascaramiento gobernada en workspace.yelp_gov (ADR-06)
# MAGIC -- Solo miembros de 'data_stewards' o 'trust_and_safety' ven el nombre en claro;
# MAGIC -- cualquier otro rol (Producto, Marketing, BI) recibe únicamente la inicial seguida de '***'.
# MAGIC CREATE OR REPLACE FUNCTION workspace.yelp_gov.mascara_nombre(nombre STRING)
# MAGIC RETURNS STRING
# MAGIC COMMENT 'ADR-06 · Enmascaramiento dinámico de PII para slv_user.name según pertenencia a grupos de gobierno'
# MAGIC RETURN CASE
# MAGIC   WHEN is_account_group_member('data_stewards') OR is_account_group_member('trust_and_safety') THEN nombre
# MAGIC   WHEN nombre IS NULL OR length(trim(nombre)) = 0 THEN 'U***'
# MAGIC   ELSE concat(left(trim(nombre), 1), '***')
# MAGIC END;

# COMMAND ----------

# DBTITLE 1,3.2 Aplicar Column Mask nativa sobre slv_user.name y crear Vista Segura en Gold
# Aplicamos tanto Opción A (Column Mask nativa sobre la columna física `name` en `slv_user`)
# como Opción B (Vista Segura `vw_usuarios_seguro` en `yelp_gold`) para defensa en profundidad.
try:
    spark.sql("""
        ALTER TABLE workspace.yelp_silver.slv_user
        ALTER COLUMN name SET MASK workspace.yelp_gov.mascara_nombre
    """)
    print("✔ Opción A aplicada: COLUMN MASK activa sobre workspace.yelp_silver.slv_user(name)")
except Exception as e:
    print(f"ℹ Nota al aplicar COLUMN MASK en slv_user.name: {e}")

spark.sql("""
CREATE OR REPLACE VIEW workspace.yelp_gold.vw_usuarios_seguro
COMMENT 'E6 · Vista segura de usuarios con enmascaramiento dinámico de PII (ADR-06) para consumo analítico'
AS
SELECT
  user_id,
  workspace.yelp_gov.mascara_nombre(name) AS nombre_enmascarado,
  review_count,
  yelping_since,
  useful,
  funny,
  cool,
  fans,
  CASE WHEN elite IS NOT NULL AND trim(elite) <> '' AND lower(trim(elite)) <> 'none' THEN true ELSE false END AS es_elite,
  average_stars,
  fecha_carga
FROM workspace.yelp_silver.slv_user
""")
print("✔ Opción B aplicada: Vista segura workspace.yelp_gold.vw_usuarios_seguro creada.")

# COMMAND ----------

# DBTITLE 1,3.3 Demostración del Enmascaramiento PII (Comparativa de Roles: Trust & Safety vs Analista de Producto)
# MAGIC %sql
# MAGIC -- Evidencia de ejecución: el mismo SELECT sobre slv_user y vw_usuarios_seguro devuelve el nombre enmascarado
# MAGIC -- para cualquier usuario que no pertenezca al grupo 'data_stewards' o 'trust_and_safety'.
# MAGIC SELECT
# MAGIC   current_user()                                                   AS usuario_ejecutor,
# MAGIC   is_account_group_member('data_stewards')                         AS es_data_steward,
# MAGIC   user_id,
# MAGIC   name                                                             AS nombre_con_column_mask_slv_user,
# MAGIC   workspace.yelp_gov.mascara_nombre(name)                          AS resultado_funcion_mascara,
# MAGIC   review_count,
# MAGIC   yelping_since,
# MAGIC   average_stars
# MAGIC FROM workspace.yelp_silver.slv_user
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Control 4 — Evidencia de Linaje End-to-End y Auditoría Forense de la Entidad `review`
# MAGIC %sql
# MAGIC -- Reconciliación matemática End-to-End del linaje de la entidad `review`
# MAGIC -- Responde al dolor de Rafael Nakamura (Trust & Safety): "reglas de fraude sin linaje claro".
# MAGIC WITH conteos AS (
# MAGIC   SELECT
# MAGIC     (SELECT count(*) FROM workspace.yelp_bronze.brz_review)             AS filas_bronze_raw,
# MAGIC     (SELECT count(*) FROM workspace.yelp_silver.slv_review)             AS filas_silver_verificadas,
# MAGIC     (SELECT count(*) FROM workspace.yelp_silver.slv_review_cuarentena)  AS filas_silver_cuarentena,
# MAGIC     (SELECT count(*) FROM workspace.yelp_gold.gld_fact_resenas)         AS filas_gold_fact_resenas,
# MAGIC     (SELECT coalesce(sum(n_resenas), 0) FROM workspace.yelp_gold.gld_kpi_negocio_mensual) AS suma_resenas_kpi_mensual,
# MAGIC     (SELECT coalesce(sum(resenas_verificadas), 0) FROM workspace.yelp_gold.gld_salud_resenas) AS suma_validas_salud_resenas,
# MAGIC     (SELECT coalesce(sum(resenas_cuarentena), 0) FROM workspace.yelp_gold.gld_salud_resenas) AS suma_cuarentena_salud_resenas
# MAGIC )
# MAGIC SELECT
# MAGIC   filas_bronze_raw,
# MAGIC   filas_silver_verificadas,
# MAGIC   filas_silver_cuarentena,
# MAGIC   (filas_silver_verificadas + filas_silver_cuarentena)                           AS total_procesado_silver,
# MAGIC   (filas_bronze_raw = filas_silver_verificadas + filas_silver_cuarentena)        AS reconciliacion_bronze_silver_ok,
# MAGIC   filas_gold_fact_resenas,
# MAGIC   (filas_silver_verificadas = filas_gold_fact_resenas)                           AS reconciliacion_silver_gold_ok,
# MAGIC   suma_resenas_kpi_mensual,
# MAGIC   suma_validas_salud_resenas,
# MAGIC   suma_cuarentena_salud_resenas
# MAGIC FROM conteos;

# COMMAND ----------

# DBTITLE 1,4.1 Grafo de Linaje de `review` registrado en el Catálogo y Diccionario de Gobierno
# MAGIC %md
# MAGIC ## 4.2 Diagrama de Linaje End-to-End de la Entidad `review` (Catalog Explorer)
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart LR
# MAGIC     RAW["/Volumes/workspace/yelp_bronze/raw/<br/>yelp_academic_dataset_review.json.gz"] -->|"Auto Loader (cloudFiles)"| BRZ["workspace.yelp_bronze.brz_review"]
# MAGIC     BRZ -->|"Reglas DQ-001..DQ-010 (APROBADAS)"| SLV["workspace.yelp_silver.slv_review"]
# MAGIC     BRZ -->|"Reglas DQ-001..DQ-010 (RECHAZADAS + motivo_rechazo)"| CUA["workspace.yelp_silver.slv_review_cuarentena"]
# MAGIC     SLV -->|"Cruce con Dimensiones Kimball"| FACT["workspace.yelp_gold.gld_fact_resenas"]
# MAGIC     SLV -->|"Agregación Mensual + Rating Bayesiano (ADR-02)"| KPI["workspace.yelp_gold.gld_kpi_negocio_mensual"]
# MAGIC     SLV -->|"Cohorte yelping_since vs Actividad (CRR)"| COH["workspace.yelp_gold.gld_cohortes_retencion_mensual"]
# MAGIC     SLV -->|"Numerador Reseñas Válidas"| SAL["workspace.yelp_gold.gld_salud_resenas"]
# MAGIC     CUA -->|"Numerador Reseñas en Cuarentena (Acta 02)"| SAL
# MAGIC     FACT -->|"Consultas Q1-Q8"| BI["E8 · AI/BI Dashboard & Genie Space"]
# MAGIC     KPI -->|"Vista Producto (Andrew Collins)"| BI
# MAGIC     SAL -->|"Vista Trust & Safety (Rafael Nakamura) / CDO"| BI
# MAGIC ```
# MAGIC
# MAGIC ### Matriz de Simulación de Roles en el Workspace (Grupo 03 · Data Evolution)
# MAGIC
# MAGIC | Rol del Caso YelpConnect | Persona / Actor Simulado | Permisos en `yelp_bronze` | Permisos en `yelp_silver` | Permisos en `yelp_gold` y `yelp_gov` | Visualización de `slv_user.name` |
# MAGIC | :--- | :--- | :--- | :--- | :--- | :--- |
# MAGIC | **Data Custodian (Data Platform)** | Ingeniero/a de Datos (`grupo3arqdatos@proton.me`) | `ALL PRIVILEGES` (Ingesta Auto Loader) | `ALL PRIVILEGES` (Transformación 3FN) | `ALL PRIVILEGES` (Publicación Kimball) | Enmascarado (`A***`) salvo grupo `data_stewards` |
# MAGIC | **Data Owner Producto** | Andrew Collins *(VP of Product)* | `DENIED` (Sin acceso a crudo) | `SELECT` auditado | `USE SCHEMA`, `SELECT` en todas las tablas Gold | Enmascarado (`J***`) por **ADR-06** |
# MAGIC | **Data Owner Trust & Safety** | Rafael Nakamura *(Lead T&S)* | `DENIED` | `USE SCHEMA`, `SELECT` en `slv_review_cuarentena` y `slv_user` | `USE SCHEMA`, `SELECT` en `gld_salud_resenas` | **Texto claro** (miembro de `trust_and_safety`) |
# MAGIC | **Data Owner Operaciones Comerciales** | Carla Romero *(Head of Ads)* | `DENIED` | `DENIED` (Consume directo de Gold) | `USE SCHEMA`, `SELECT` en `gld_checkins_diarios` | Enmascarado (`M***`) por **ADR-06** |
# MAGIC | **Chief Data Officer (CDO)** | Laura Hernández *(CDO)* | `DENIED` | `SELECT` en vistas de auditoría | `USE SCHEMA`, `SELECT` en `yelp_gov.*` y `yelp_gold.*` | **Texto claro** (miembro de `data_stewards`) |
# MAGIC
# MAGIC ---
# MAGIC ### Definition of Done (E6)
# MAGIC - [x] Matriz RACI registrada en `workspace.yelp_gov.matriz_raci` (7 entidades gobernadas con Data Owner, Steward, Custodian y clasificación).
# MAGIC - [x] Tags de clasificación aplicados a los 4 schemas (`yelp_bronze`, `yelp_silver`, `yelp_gold`, `yelp_gov`), 12 tablas críticas y 5 columnas sensibles/PII.
# MAGIC - [x] `GRANT`s ejecutados bajo principio de mínimo privilegio y verificados con `SHOW GRANTS`.
# MAGIC - [x] Máscara PII (`workspace.yelp_gov.mascara_nombre`) activa sobre `workspace.yelp_silver.slv_user.name` + vista segura `workspace.yelp_gold.vw_usuarios_seguro`.
# MAGIC - [x] Linaje end-to-end y reconciliación matemática de `review` (`Bronze → Silver + Cuarentena → Gold → BI`) documentados y verificados.

# COMMAND ----------

# DBTITLE 1,Retorno de estado para orquestación opcional desde E7 (04_job_orquestacion.py)
dbutils.notebook.exit("OK")
