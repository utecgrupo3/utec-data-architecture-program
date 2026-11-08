# Databricks notebook source
# MAGIC %md
# MAGIC # E6 · Controles Técnicos de Gobierno en Unity Catalog (Grupo 03 · Data Evolution)
# MAGIC **Cierra con:** M2 (diseño) + M5 (implementación en plataforma)
# MAGIC
# MAGIC La estrategia de gobierno se **implementa** aquí con **5 componentes verificables en Unity Catalog**:
# MAGIC 1. **Matriz RACI gobernada (`workspace.yelp_gov.raci_matrix`):** *Data Owner*, *Data Steward*, *Data Custodian* y clasificación de sensibilidad por entidad (insumo del Dashboard E8 para la CDO Laura Hernández).
# MAGIC 2. **Control 1 — Clasificación con Tags (`SET TAGS`):** esquemas, tablas y columnas sensibles/PII (`slv_user.name`, `gld_dim_user.name`, coordenadas, cuarentena).
# MAGIC 3. **Control 2 — Permisos RBAC por capa (`GRANT`):** mínimo privilegio entre *Data Custodian*, *Data Stewards*, *Trust & Safety* y consumidores de negocio.
# MAGIC 4. **Control 3 — Enmascaramiento dinámico de PII (`COLUMN MASK` + vista segura):** función `workspace.yelp_gov.mask_name`, máscara sobre `slv_user.name` y `gld_dim_user.name`, y vista `workspace.yelp_gold.vw_user_secure`.
# MAGIC 5. **Control 4 — Linaje end-to-end y reconciliación:** trazabilidad de la entidad `review` (`raw → bronze → silver / cuarentena → gold → BI`) para Trust & Safety.
# MAGIC
# MAGIC Esta tarea corre al final del job: vuelve a aplicar las máscaras que Silver y Gold retiran para recargar.

# COMMAND ----------

# DBTITLE 1,0. Registro de la Matriz RACI en Unity Catalog (workspace.yelp_gov.raci_matrix)
from pyspark.sql import Row

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.yelp_gov")
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.yelp_gov.raci_matrix (
  entity           STRING COMMENT 'Entidad conceptual y lógica gobernada del caso YelpConnect',
  domain           STRING COMMENT 'Dominio de datos DAMA-DMBOK al que pertenece la entidad',
  data_owner       STRING COMMENT 'Accountable (A) — Ejecutivo dueño de negocio responsable del dato',
  data_steward     STRING COMMENT 'Responsible (R) — Custodio funcional que opera calidad y metadatos',
  data_custodian   STRING COMMENT 'Responsible técnico — Equipo Data Platform que opera el Lakehouse',
  consumers        STRING COMMENT 'Informed / Consulted — Áreas de negocio autorizadas a consumir',
  classification   STRING COMMENT 'Nivel de sensibilidad: public | internal | confidential | pii'
) USING DELTA
COMMENT 'E6 · Asignación formal de roles y responsabilidades RACI sobre las entidades de YelpConnect'
""")

CUSTODIAN = "Data Platform (Grupo 03)"
filas_raci = [
    Row(entity="Negocio (slv_business / gld_dim_business)", domain="Negocio",
        data_owner="Andrew Collins (VP of Product)", data_steward="Steward de Producto", data_custodian=CUSTODIAN,
        consumers="Producto, Operaciones Comerciales, Marketing, Analítica BI", classification="public"),
    Row(entity="Categoría (slv_category / slv_business_category / gld_dim_category / gld_bridge_business_category)", domain="Negocio",
        data_owner="Andrew Collins (VP of Product)", data_steward="Steward de Producto", data_custodian=CUSTODIAN,
        consumers="Producto, Marketing, Analítica BI", classification="public"),
    Row(entity="Reseña (slv_review / gld_fact_review)", domain="Contenido",
        data_owner="Andrew Collins (VP of Product)", data_steward="Steward de Producto y Steward de Trust & Safety", data_custodian=CUSTODIAN,
        consumers="Producto, Trust & Safety, Marketing, CDO", classification="internal"),
    Row(entity="Cuarentena de reseñas (slv_review_quarantine / gld_agg_review_health_month)", domain="Confianza y Seguridad",
        data_owner="Rafael Nakamura (Lead Trust & Safety)", data_steward="Steward de Trust & Safety", data_custodian=CUSTODIAN,
        consumers="Trust & Safety, CDO (Laura Hernández), Producto", classification="confidential"),
    Row(entity="Usuario (slv_user / gld_dim_user)", domain="Usuario",
        data_owner="Rafael Nakamura (Lead Trust & Safety)", data_steward="Steward de Privacidad y Protección de Datos", data_custodian=CUSTODIAN,
        consumers="Trust & Safety (acceso PII), Producto y Marketing (solo enmascarado)", classification="pii"),
    Row(entity="Amistad (slv_friendship)", domain="Interacción / Red",
        data_owner="Rafael Nakamura (Lead Trust & Safety)", data_steward="Steward de Privacidad y Producto", data_custodian=CUSTODIAN,
        consumers="Producto (engagement comunitario), Trust & Safety (detección de anillos de bots)", classification="internal"),
    Row(entity="Check-in (slv_checkin / gld_fact_checkin / gld_agg_business_checkin_day)", domain="Interacción y Publicidad",
        data_owner="Carla Romero (Head of Commercial Ops & Ads)", data_steward="Steward de Interacción Comercial", data_custodian=CUSTODIAN,
        consumers="Operaciones Comerciales (Yelp Ads), Producto (Andrew Collins)", classification="confidential"),
    Row(entity="Tip (slv_tip / gld_fact_tip)", domain="Contenido",
        data_owner="Andrew Collins (VP of Product)", data_steward="Steward de Producto", data_custodian=CUSTODIAN,
        consumers="Producto, Experiencia de Usuario, Trust & Safety", classification="public"),
    Row(entity="Foto (slv_photo)", domain="Contenido",
        data_owner="Andrew Collins (VP of Product)", data_steward="Steward de Producto", data_custodian=CUSTODIAN,
        consumers="Producto, Marketing", classification="public"),
]

spark.createDataFrame(filas_raci).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.yelp_gov.raci_matrix")
display(spark.table("workspace.yelp_gov.raci_matrix"))

# COMMAND ----------

# DBTITLE 1,Control 1 — Clasificación con Tags en esquemas, tablas y columnas sensibles
S, G = "workspace.yelp_silver", "workspace.yelp_gold"
comandos_tags = [
    # 1.1 Esquemas
    "ALTER SCHEMA workspace.yelp_bronze SET TAGS ('layer' = 'bronze', 'domain' = 'yelpconnect', 'access' = 'restricted_custodian', 'team' = '03')",
    "ALTER SCHEMA workspace.yelp_silver SET TAGS ('layer' = 'silver', 'domain' = 'yelpconnect', 'model' = 'logical_normalized', 'team' = '03')",
    "ALTER SCHEMA workspace.yelp_gold   SET TAGS ('layer' = 'gold',   'domain' = 'yelpconnect', 'model' = 'dimensional_star', 'certified' = 'true', 'team' = '03')",
    "ALTER SCHEMA workspace.yelp_gov    SET TAGS ('layer' = 'governance', 'domain' = 'dama_dmbok', 'owner' = 'laura_hernandez_cdo', 'team' = '03')",

    # 1.2 Tablas Silver
    f"ALTER TABLE {S}.slv_business          SET TAGS ('owner' = 'andrew_collins',  'classification' = 'public',      'domain' = 'business')",
    f"ALTER TABLE {S}.slv_category          SET TAGS ('owner' = 'andrew_collins',  'classification' = 'public',      'domain' = 'business')",
    f"ALTER TABLE {S}.slv_business_category SET TAGS ('owner' = 'andrew_collins',  'classification' = 'public',      'domain' = 'business')",
    f"ALTER TABLE {S}.slv_user              SET TAGS ('owner' = 'rafael_nakamura', 'classification' = 'pii',          'domain' = 'user', 'regulation' = 'GDPR_CCPA_Ley29733')",
    f"ALTER TABLE {S}.slv_friendship        SET TAGS ('owner' = 'rafael_nakamura', 'classification' = 'internal',      'domain' = 'interaction')",
    f"ALTER TABLE {S}.slv_review            SET TAGS ('owner' = 'andrew_collins',  'classification' = 'internal',      'domain' = 'content')",
    f"ALTER TABLE {S}.slv_review_quarantine SET TAGS ('owner' = 'rafael_nakamura', 'classification' = 'confidential', 'domain' = 'trust_and_safety')",
    f"ALTER TABLE {S}.slv_tip               SET TAGS ('owner' = 'andrew_collins',  'classification' = 'public',      'domain' = 'content')",
    f"ALTER TABLE {S}.slv_checkin           SET TAGS ('owner' = 'carla_romero',    'classification' = 'confidential', 'domain' = 'interaction')",
    f"ALTER TABLE {S}.slv_photo             SET TAGS ('owner' = 'andrew_collins',  'classification' = 'public',      'domain' = 'content')",

    # 1.3 Tablas Gold
    f"ALTER TABLE {G}.gld_dim_user                    SET TAGS ('owner' = 'rafael_nakamura', 'classification' = 'pii',          'pii_treatment' = 'column_mask')",
    f"ALTER TABLE {G}.gld_dim_business                SET TAGS ('owner' = 'andrew_collins',  'classification' = 'public')",
    f"ALTER TABLE {G}.gld_fact_review                 SET TAGS ('owner' = 'andrew_collins',  'classification' = 'internal')",
    f"ALTER TABLE {G}.gld_fact_checkin                SET TAGS ('owner' = 'carla_romero',    'classification' = 'confidential')",
    f"ALTER TABLE {G}.gld_agg_business_month          SET TAGS ('owner' = 'andrew_collins',  'classification' = 'internal',      'kpi' = 'valid_rating')",
    f"ALTER TABLE {G}.gld_agg_business_bayesian_month SET TAGS ('owner' = 'andrew_collins',  'classification' = 'internal',      'kpi' = 'bayesian_rating')",
    f"ALTER TABLE {G}.gld_agg_business_checkin_day    SET TAGS ('owner' = 'carla_romero',    'classification' = 'confidential', 'kpi' = 'official_checkins')",
    f"ALTER TABLE {G}.gld_agg_review_health_month     SET TAGS ('owner' = 'rafael_nakamura', 'classification' = 'confidential', 'kpi' = 'review_health')",
    f"ALTER TABLE {G}.gld_agg_user_cohort_month       SET TAGS ('owner' = 'andrew_collins',  'classification' = 'internal',      'kpi' = 'cohort_retention')",

    # 1.4 Columnas sensibles / PII
    f"ALTER TABLE {S}.slv_user              ALTER COLUMN name             SET TAGS ('confidentiality' = 'PII', 'classification' = 'restricted', 'policy' = 'dynamic_mask')",
    f"ALTER TABLE {G}.gld_dim_user          ALTER COLUMN name             SET TAGS ('confidentiality' = 'PII', 'classification' = 'restricted', 'policy' = 'dynamic_mask')",
    f"ALTER TABLE {S}.slv_business          ALTER COLUMN latitude         SET TAGS ('confidentiality' = 'geospatial', 'classification' = 'internal')",
    f"ALTER TABLE {S}.slv_business          ALTER COLUMN longitude        SET TAGS ('confidentiality' = 'geospatial', 'classification' = 'internal')",
    f"ALTER TABLE {S}.slv_review            ALTER COLUMN text             SET TAGS ('confidentiality' = 'indirect_PII', 'classification' = 'confidential')",
    f"ALTER TABLE {S}.slv_review_quarantine ALTER COLUMN rejection_reason SET TAGS ('confidentiality' = 'confidential', 'classification' = 'forensic_audit')",
]

# Retira las etiquetas con claves en español de versiones anteriores, para que solo queden las claves en inglés.
CLAVES_ANTIGUAS = ["capa", "dominio", "acceso", "grupo", "modelo", "certificado", "clasificacion", "normativa",
                   "pii_tratamiento", "confidencialidad", "politica", "origen"]
lista = ", ".join(f"'{c}'" for c in CLAVES_ANTIGUAS)
for r in spark.sql(f"SELECT schema_name, collect_set(tag_name) AS tags FROM workspace.information_schema.schema_tags "
                   f"WHERE schema_name LIKE 'yelp_%' AND tag_name IN ({lista}) GROUP BY schema_name").collect():
    spark.sql(f"ALTER SCHEMA workspace.{r.schema_name} UNSET TAGS ({', '.join(repr(x) for x in r.tags)})")
for r in spark.sql(f"SELECT schema_name, table_name, collect_set(tag_name) AS tags FROM workspace.information_schema.table_tags "
                   f"WHERE schema_name LIKE 'yelp_%' AND tag_name IN ({lista}) GROUP BY schema_name, table_name").collect():
    spark.sql(f"ALTER TABLE workspace.{r.schema_name}.{r.table_name} UNSET TAGS ({', '.join(repr(x) for x in r.tags)})")
for r in spark.sql(f"SELECT schema_name, table_name, column_name, collect_set(tag_name) AS tags FROM workspace.information_schema.column_tags "
                   f"WHERE schema_name LIKE 'yelp_%' AND tag_name IN ({lista}) GROUP BY schema_name, table_name, column_name").collect():
    spark.sql(f"ALTER TABLE workspace.{r.schema_name}.{r.table_name} ALTER COLUMN {r.column_name} UNSET TAGS ({', '.join(repr(x) for x in r.tags)})")

aplicados = 0
for cmd in comandos_tags:
    try:
        spark.sql(cmd)
        aplicados += 1
    except Exception as e:
        print(f"ℹ Tag omitido ({cmd[:70]}...): {e}")

print(f"✔ Control 1 completado: {aplicados}/{len(comandos_tags)} sentencias SET TAGS aplicadas en Unity Catalog.")

# COMMAND ----------

# DBTITLE 1,1.1 Verificación de Tags registrados en Information Schema
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

# DBTITLE 1,Control 2 — Permisos RBAC por capa (mínimo privilegio)
# - yelp_bronze: solo Data Platform / Data Custodian.
# - yelp_silver: Data Stewards y Trust & Safety (con column mask en PII).
# - yelp_gold:   consumidores de negocio (Producto, Marketing, Operaciones Comerciales).
# - yelp_gov:    CDO, Data Stewards y auditoría de gobierno.
usuario_actual = spark.sql("SELECT current_user() AS u").first().u
print(f"Usuario autenticado en el workspace: {usuario_actual}")

comandos_rbac = [
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

# DBTITLE 1,2.1 Verificación de Grants en Gold y Gobierno
# MAGIC %sql
# MAGIC SHOW GRANTS ON SCHEMA workspace.yelp_gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON SCHEMA workspace.yelp_gov;

# COMMAND ----------

# DBTITLE 1,Control 3 — Función de enmascaramiento de PII
# MAGIC %sql
# MAGIC -- Solo miembros de 'data_stewards' o 'trust_and_safety' ven el nombre en claro;
# MAGIC -- cualquier otro rol (Producto, Marketing, BI) recibe la inicial seguida de '***'.
# MAGIC CREATE OR REPLACE FUNCTION workspace.yelp_gov.mask_name(name STRING)
# MAGIC RETURNS STRING
# MAGIC COMMENT 'Enmascaramiento dinámico de PII para slv_user.name y gld_dim_user.name según pertenencia a grupos de gobierno'
# MAGIC RETURN CASE
# MAGIC   WHEN is_account_group_member('data_stewards') OR is_account_group_member('trust_and_safety') THEN name
# MAGIC   WHEN name IS NULL OR length(trim(name)) = 0 THEN 'U***'
# MAGIC   ELSE concat(left(trim(name), 1), '***')
# MAGIC END;

# COMMAND ----------

# DBTITLE 1,3.2 Column Mask sobre slv_user.name y gld_dim_user.name + vista segura en Gold
for tabla in ["workspace.yelp_silver.slv_user", "workspace.yelp_gold.gld_dim_user"]:
    try:
        spark.sql(f"ALTER TABLE {tabla} ALTER COLUMN name SET MASK workspace.yelp_gov.mask_name")
        print(f"✔ COLUMN MASK activa sobre {tabla}(name)")
    except Exception as e:
        print(f"ℹ Nota al aplicar COLUMN MASK en {tabla}.name: {e}")

spark.sql("""
CREATE OR REPLACE VIEW workspace.yelp_gold.vw_user_secure (
  user_key       COMMENT 'Identificador del usuario (user_id de Yelp)',
  masked_name    COMMENT 'Nombre del usuario enmascarado según el rol de quien consulta',
  yelping_since  COMMENT 'Fecha en que el usuario se unió a Yelp',
  fans           COMMENT 'Cantidad de seguidores del usuario',
  avg_stars      COMMENT 'Calificación promedio que da el usuario',
  is_elite       COMMENT 'Verdadero si Yelp reconoció al usuario como Elite en al menos un año'
)
COMMENT 'E6 · Vista segura de usuarios con enmascaramiento dinámico de PII para consumo analítico'
AS
SELECT user_key, workspace.yelp_gov.mask_name(name), yelping_since, fans, avg_stars, is_elite
FROM workspace.yelp_gold.gld_dim_user
""")
print("✔ Vista segura workspace.yelp_gold.vw_user_secure creada.")

# COMMAND ----------

# DBTITLE 1,3.3 Demostración del enmascaramiento PII
# MAGIC %sql
# MAGIC SELECT
# MAGIC   current_user()                              AS usuario_ejecutor,
# MAGIC   is_account_group_member('data_stewards')    AS es_data_steward,
# MAGIC   user_key,
# MAGIC   name                                        AS nombre_con_column_mask,
# MAGIC   yelping_since,
# MAGIC   avg_stars
# MAGIC FROM workspace.yelp_gold.gld_dim_user
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Control 4 — Linaje end-to-end y reconciliación de la entidad review
# MAGIC %sql
# MAGIC WITH conteos AS (
# MAGIC   SELECT
# MAGIC     (SELECT count(*) FROM workspace.yelp_bronze.brz_review)                    AS filas_bronze,
# MAGIC     (SELECT count(*) FROM workspace.yelp_silver.slv_review)                    AS filas_silver,
# MAGIC     (SELECT count(*) FROM workspace.yelp_silver.slv_review_quarantine)         AS filas_cuarentena,
# MAGIC     (SELECT count(*) FROM workspace.yelp_gold.gld_fact_review)                 AS filas_gold,
# MAGIC     (SELECT count_if(is_valid) FROM workspace.yelp_gold.gld_fact_review)       AS filas_gold_validas,
# MAGIC     (SELECT coalesce(sum(review_count), 0) FROM workspace.yelp_gold.gld_agg_business_month)      AS suma_agg_business_month,
# MAGIC     (SELECT coalesce(sum(valid_review_count), 0) FROM workspace.yelp_gold.gld_agg_review_health_month) AS suma_validas_health,
# MAGIC     (SELECT coalesce(sum(quarantined_review_count), 0) FROM workspace.yelp_gold.gld_agg_review_health_month) AS suma_cuarentena_health
# MAGIC )
# MAGIC SELECT
# MAGIC   filas_bronze,
# MAGIC   filas_silver,
# MAGIC   filas_cuarentena,
# MAGIC   (filas_bronze = filas_silver + filas_cuarentena)    AS reconciliacion_bronze_silver_ok,
# MAGIC   filas_gold,
# MAGIC   (filas_silver = filas_gold)                         AS reconciliacion_silver_gold_ok,
# MAGIC   filas_gold_validas,
# MAGIC   (suma_agg_business_month = filas_gold)              AS reconciliacion_agg_ok,
# MAGIC   suma_validas_health,
# MAGIC   suma_cuarentena_health
# MAGIC FROM conteos;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Diagrama de linaje end-to-end de la entidad `review`
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart LR
# MAGIC     RAW["/Volumes/workspace/yelp_bronze/raw/<br/>review.json"] -->|"Auto Loader (cloudFiles)"| BRZ["workspace.yelp_bronze.brz_review"]
# MAGIC     BRZ -->|"Pasa reglas bloqueantes"| SLV["workspace.yelp_silver.slv_review"]
# MAGIC     BRZ -->|"Incumple DQ-001/003/004/005/006 + rejection_reason"| CUA["workspace.yelp_silver.slv_review_quarantine"]
# MAGIC     SLV -->|"Negocio maestro + is_valid (DQ-002)"| FACT["workspace.yelp_gold.gld_fact_review"]
# MAGIC     FACT -->|"Agregación mensual"| AGG["workspace.yelp_gold.gld_agg_business_month"]
# MAGIC     FACT -->|"Calificación bayesiana"| BAY["workspace.yelp_gold.gld_agg_business_bayesian_month"]
# MAGIC     FACT -->|"Cohorte por yelping_since"| COH["workspace.yelp_gold.gld_agg_user_cohort_month"]
# MAGIC     FACT -->|"Válidas y duplicadas"| SAL["workspace.yelp_gold.gld_agg_review_health_month"]
# MAGIC     CUA -->|"Rechazadas"| SAL
# MAGIC     AGG -->|"Vista Producto (Andrew Collins)"| BI["E8 · AI/BI Dashboard & Genie Space"]
# MAGIC     BAY --> BI
# MAGIC     SAL -->|"Vista Trust & Safety / CDO"| BI
# MAGIC ```
# MAGIC
# MAGIC ### Matriz de simulación de roles en el workspace
# MAGIC
# MAGIC | Rol del caso | Persona / actor simulado | `yelp_bronze` | `yelp_silver` | `yelp_gold` y `yelp_gov` | `name` en `slv_user` / `gld_dim_user` |
# MAGIC | :--- | :--- | :--- | :--- | :--- | :--- |
# MAGIC | **Data Custodian (Data Platform)** | Ingeniero/a de datos (`grupo3arqdatos@proton.me`) | `ALL PRIVILEGES` | `ALL PRIVILEGES` | `ALL PRIVILEGES` | Enmascarado (`A***`) salvo grupo `data_stewards` |
# MAGIC | **Data Owner Producto** | Andrew Collins *(VP of Product)* | Sin acceso | `SELECT` auditado | `USE SCHEMA`, `SELECT` | Enmascarado |
# MAGIC | **Data Owner Trust & Safety** | Rafael Nakamura *(Lead T&S)* | Sin acceso | `SELECT` en `slv_review_quarantine` y `slv_user` | `SELECT` en `gld_agg_review_health_month` | **Texto claro** (`trust_and_safety`) |
# MAGIC | **Data Owner Operaciones Comerciales** | Carla Romero *(Head of Ads)* | Sin acceso | Sin acceso (consume Gold) | `SELECT` en `gld_agg_business_checkin_day` | Enmascarado |
# MAGIC | **Chief Data Officer (CDO)** | Laura Hernández *(CDO)* | Sin acceso | `SELECT` en vistas de auditoría | `SELECT` en `yelp_gov.*` y `yelp_gold.*` | **Texto claro** (`data_stewards`) |
# MAGIC
# MAGIC ---
# MAGIC ### Definition of Done (E6)
# MAGIC - [x] Matriz RACI en `workspace.yelp_gov.raci_matrix` (9 entidades con Data Owner, Steward, Custodian y clasificación).
# MAGIC - [x] Tags en los 4 esquemas, las tablas críticas de Silver y Gold y las columnas sensibles/PII.
# MAGIC - [x] `GRANT`s con mínimo privilegio verificados con `SHOW GRANTS`.
# MAGIC - [x] Máscara PII activa sobre `slv_user.name` y `gld_dim_user.name` + vista segura `vw_user_secure`.
# MAGIC - [x] Linaje y reconciliación de `review` (`Bronze → Silver + Cuarentena → Gold → BI`).

# COMMAND ----------

# DBTITLE 1,Retorno de estado para orquestación
dbutils.notebook.exit("OK")
