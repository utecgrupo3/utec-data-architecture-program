# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Configuración del Lakehouse del grupo
# MAGIC **Ejecutar UNA sola vez por grupo** (es idempotente: re-ejecutarlo no rompe nada).
# MAGIC
# MAGIC Crea la arquitectura medallón sobre Unity Catalog:
# MAGIC - Schemas `yelp_bronze` / `yelp_silver` / `yelp_gold` (datos) y `yelp_gov` (gobierno, calidad y metadata).
# MAGIC - Volume `raw` para aterrizar los archivos crudos del Yelp Open Dataset.
# MAGIC - Tablas de control que usarán los entregables E4, E5 y E6.
# MAGIC
# MAGIC > ✏️ **TODO (grupo):** completar el widget con el número de grupo antes de ejecutar.

# COMMAND ----------

dbutils.widgets.text("grupo", "03", "Número de grupo")
GRUPO = dbutils.widgets.get("grupo")
CATALOGO = "workspace"  # catálogo por defecto de Free Edition
print(f"Configurando lakehouse del Grupo {GRUPO} en el catálogo '{CATALOGO}'")

# COMMAND ----------

# DBTITLE 1,Schemas de la arquitectura medallón
for schema, comentario in [
    ("yelp_bronze", "Capa Bronze: datos crudos tal como llegan de YelpConnect"),
    ("yelp_silver", "Capa Silver: datos limpios, conformados y con calidad validada"),
    ("yelp_gold",   "Capa Gold: productos de datos para consumo analítico y BI"),
    ("yelp_gov",    "Gobierno: glosario, diccionario, reglas y resultados de calidad"),
]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{schema} COMMENT '{comentario} — Grupo {GRUPO}'")
print("Schemas creados ✔")

# COMMAND ----------

# DBTITLE 1,Volume para datos crudos
spark.sql(f"""
CREATE VOLUME IF NOT EXISTS {CATALOGO}.yelp_bronze.raw
COMMENT 'Zona de aterrizaje de archivos crudos (business, review, user, tip, checkin)'
""")
print("Volume listo → /Volumes/workspace/yelp_bronze/raw/")

# COMMAND ----------

# DBTITLE 1,Tablas de control de gobierno (se llenan en E4, E5 y E6)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOGO}.yelp_gov.business_glossary (
  term                  STRING COMMENT 'Nombre del Término — nombre oficial aprobado del concepto',
  synonyms              STRING COMMENT 'Sinónimos / Alias — otras formas en que el término se conoce',
  business_definition   STRING COMMENT 'Definición de Negocio — explicación comprensible sin tecnicismos',
  technical_definition  STRING COMMENT 'Definición Técnica (opcional) — cómo se representa en la plataforma',
  usage_context         STRING COMMENT 'Contexto / Uso — áreas, procesos o dominios donde se utiliza',
  data_domain           STRING COMMENT 'Dominio de Datos — Negocio, Usuario, Contenido, Interacción, Publicidad, Confianza y Seguridad, Gobierno',
  category              STRING COMMENT 'Categoría — Entidad, Atributo, Rol, Proceso, Medida, Indicador o Concepto',
  usage_example         STRING COMMENT 'Ejemplo de Uso — frase que ilustra el término aplicado al caso',
  related_terms         STRING COMMENT 'Relaciones con otros términos — trazabilidad semántica',
  term_owner            STRING COMMENT 'Propietario del Término — Data Owner del caso responsable de la definición',
  regulatory_source     STRING COMMENT 'Fuente Normativa / Regulatoria — ley, norma o política interna que la soporta',
  updated_at            DATE   COMMENT 'Fecha de Creación / Última Actualización'
) COMMENT 'E4 · Glosario de términos de negocio — mismo esquema que la plantilla institucional del bloque de Gobierno'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOGO}.yelp_gov.data_dictionary (
  element                  STRING COMMENT 'Nombre del Elemento de Datos — nombre técnico del campo',
  business_name            STRING COMMENT 'Nombre de Negocio — nombre usado en el glosario',
  business_description     STRING COMMENT 'Descripción de Negocio — significado del dato',
  domain                   STRING COMMENT 'Dominio / Tema de Datos',
  source_system            STRING COMMENT 'Sistema / Aplicación Fuente donde se origina el dato',
  physical_name            STRING COMMENT 'Nombre Físico — nombre calificado en Unity Catalog (catalogo.schema.tabla.columna)',
  data_type                STRING COMMENT 'Tipo de Dato según el motor (STRING, INT, DOUBLE, TIMESTAMP...)',
  format                   STRING COMMENT 'Formato / Patrón esperado',
  length                   STRING COMMENT 'Longitud / Precisión',
  allowed_values           STRING COMMENT 'Valores Permitidos / Dominio de Valores',
  unit_of_measure          STRING COMMENT 'Unidad de Medida (si aplica)',
  default_value            STRING COMMENT 'Valor por Defecto',
  nullability              STRING COMMENT 'Obligatorio / Nulo',
  key_type                 STRING COMMENT 'Clave Primaria / Foránea',
  quality_rules            STRING COMMENT 'Reglas de Calidad / Validación — debe citar el identificador DQ-nnn de yelp_gov.dq_rules',
  transformation_rules     STRING COMMENT 'Reglas de Transformación / Cálculo (si aplica)',
  update_frequency         STRING COMMENT 'Frecuencia de Actualización',
  data_origin              STRING COMMENT 'Origen del Dato — cómo y desde dónde se genera',
  process_usage            STRING COMMENT 'Uso / Procesos Relacionados',
  related_elements         STRING COMMENT 'Relación con otros elementos',
  data_owner               STRING COMMENT 'Responsable / Data Owner',
  retention_policy         STRING COMMENT 'Política de Retención',
  sensitivity              STRING COMMENT 'Sensibilidad / Clasificación — Público, Interno, Confidencial o Restringido',
  regulatory_source        STRING COMMENT 'Fuente Normativa / Regulatoria',
  updated_at               DATE   COMMENT 'Fecha de Creación / Última Actualización'
) COMMENT 'E4 · Diccionario de datos — mismo esquema que la plantilla institucional del bloque de Gobierno'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOGO}.yelp_gov.dq_rules (
  rule_id          STRING  COMMENT 'Identificador único (DQ-001 a DQ-015)',
  target_table     STRING  COMMENT 'Tabla completa: workspace.yelp_silver.slv_business',
  column_name      STRING  COMMENT 'Columna evaluada (o * si es a nivel tabla)',
  dimension        STRING  COMMENT 'Dimensión DQ: validity|uniqueness|completeness|consistency|accuracy|timeliness',
  description      STRING  COMMENT 'Regla en lenguaje de negocio',
  sql_expression   STRING  COMMENT 'Predicado SQL que DEBE cumplirse (ej. stars BETWEEN 1 AND 5)',
  criticality      STRING  COMMENT 'critical|high|medium',
  threshold_pct    DOUBLE  COMMENT 'Porcentaje mínimo de filas que deben cumplir la regla (0-100)'
) COMMENT 'E5 · Catálogo de reglas de calidad de datos'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOGO}.yelp_gov.dq_results (
  executed_at      TIMESTAMP COMMENT 'Momento de la corrida del motor DQ',
  rule_id          STRING    COMMENT 'Regla evaluada (dq_rules.rule_id)',
  target_table     STRING    COMMENT 'Tabla evaluada',
  total_rows       BIGINT    COMMENT 'Filas evaluadas',
  passed_rows      BIGINT    COMMENT 'Filas que cumplen la regla',
  compliance_pct   DOUBLE    COMMENT 'Porcentaje de cumplimiento (0-100)',
  passed           BOOLEAN   COMMENT 'TRUE si compliance_pct >= threshold_pct de la regla'
) COMMENT 'E5 · Historial de ejecuciones del motor de calidad'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOGO}.yelp_gov.raci_matrix (
  entity           STRING  COMMENT 'Entidad de datos (business, review, user...)',
  domain           STRING  COMMENT 'Dominio de datos',
  data_owner       STRING  COMMENT 'Área/rol accountable del caso Yelp',
  data_steward     STRING  COMMENT 'Steward funcional',
  data_custodian   STRING  COMMENT 'Custodio técnico',
  consumers        STRING  COMMENT 'Áreas que consumen la entidad',
  classification   STRING  COMMENT 'public|internal|confidential|pii'
) COMMENT 'E6 · Asignación de roles y responsabilidades sobre entidades'
""")
print("Tablas de control creadas ✔")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Siguiente paso
# MAGIC 1. Correr en su laptop `datos/preparar_muestra_yelp.py` y subir los `.json.gz` al Volume.
# MAGIC 2. Ejecutar `00_setup/01_verificar_datos_raw`.
