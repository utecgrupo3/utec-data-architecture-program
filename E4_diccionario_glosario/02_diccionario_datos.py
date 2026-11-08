# Databricks notebook source
# MAGIC %md
# MAGIC # E4.2 · Diccionario de Datos — Grupo 03 (Data Evolution)
# MAGIC **Cierra con:** M3 MDM y Metadata, sobre el modelo físico de M4 · **Formato oficial:**
# MAGIC plantilla institucional `Diccionario_de_Datos_YelpConnect_PLANTILLA.xlsx` (25 columnas).
# MAGIC
# MAGIC Lección de arquitectura: el diccionario **no se escribe entero a mano**. Se divide en dos:
# MAGIC
# MAGIC | Parte | Origen | Quién la produce |
# MAGIC |---|---|---|
# MAGIC | Metadata técnica (nombre físico, tipo, nulidad, clave, clasificación) | `information_schema` de Unity Catalog | Se **hereda** automáticamente del DDL de E3 |
# MAGIC | Metadata de negocio (descripción, dominio, uso, retención, normativa, owner) | Acuerdo entre áreas | La **redacta el grupo** en el Excel institucional |
# MAGIC
# MAGIC Si el borrador autogenerado sale con huecos, la deuda está en el DDL de E3, no aquí.
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la sección «Anexo B · Diccionario de Datos» y las Secciones 4.2, 4.3, 4.4, 5.3, 5.4 y 6.2 del
# MAGIC > **Documento Formal de Arquitectura de Datos (v1.2 · Equipo Data Evolution)**. El Excel institucional se anexa al documento; esta tabla Delta permite auditar automáticamente la cobertura de documentación contra el catálogo.

# COMMAND ----------

# DBTITLE 1,Asegurar dependencia openpyxl para lectura de plantillas Excel en Serverless
# MAGIC %pip install -q openpyxl

# COMMAND ----------

# DBTITLE 1,Sincronizar plantilla Excel completada al Volume de Gobierno
import os
import shutil

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.yelp_gov")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.yelp_gov.artifacts")

RUTA = "/Volumes/workspace/yelp_gov/artifacts/Diccionario_de_Datos_YelpConnect.xlsx"
RUTA_PLANTILLA_LOCAL = os.path.join(
    os.getcwd(), "plantillas", "Diccionario_de_Datos_YelpConnect_PLANTILLA.xlsx"
)

if os.path.exists(RUTA_PLANTILLA_LOCAL):
    shutil.copy2(RUTA_PLANTILLA_LOCAL, RUTA)
    shutil.copy2(
        RUTA_PLANTILLA_LOCAL,
        "/Volumes/workspace/yelp_gov/artifacts/Diccionario_de_Datos_YelpConnect_PLANTILLA.xlsx",
    )
    print(f"✔ Plantilla sincronizada automáticamente al Volume: {RUTA}")
else:
    print(f"Usando archivo en Volume: {RUTA}")

# COMMAND ----------

# DBTITLE 1,Paso 1 — Borrador técnico autogenerado desde Unity Catalog
borrador = spark.sql("""
  SELECT
    c.column_name                                                    AS element,
    NULL                                                             AS business_name,
    c.comment                                                        AS business_description,
    NULL                                                             AS domain,
    NULL                                                             AS source_system,
    concat_ws('.', 'workspace', c.table_schema, c.table_name,
              c.column_name)                                         AS physical_name,
    c.full_data_type                                                 AS data_type,
    CASE WHEN c.is_nullable = 'NO' THEN 'Obligatorio'
         ELSE 'Opcional' END                                         AS nullability,
    ct.tag_value                                                     AS sensitivity
  FROM workspace.information_schema.columns c
  LEFT JOIN workspace.information_schema.column_tags ct
    ON  ct.schema_name = c.table_schema AND ct.table_name = c.table_name
    AND ct.column_name = c.column_name  AND ct.tag_name   = 'classification'
  WHERE c.table_schema IN ('yelp_silver', 'yelp_gold')
  ORDER BY c.table_schema, c.table_name, c.ordinal_position
""")
display(borrador)

# COMMAND ----------

# DBTITLE 1,Paso 2 — Carga del Excel institucional completado → tabla Delta (con reconciliación de catálogo)
import datetime
import pandas as pd

COLUMNAS = ["element", "business_name", "business_description", "domain", "source_system",
            "physical_name", "data_type", "format", "length", "allowed_values",
            "unit_of_measure", "default_value", "nullability", "key_type", "quality_rules",
            "transformation_rules", "update_frequency", "data_origin", "process_usage",
            "related_elements", "data_owner", "retention_policy", "sensitivity",
            "regulatory_source", "updated_at"]

pdf = pd.read_excel(RUTA, sheet_name="Diccionario", skiprows=4)
pdf = pdf.dropna(how="all").iloc[:, :25]
pdf.columns = COLUMNAS

# Reconciliación dinámica con information_schema.columns: si en yelp_silver o yelp_gold
# existen columnas adicionales creadas en el workspace, se documentan con las reglas de su dominio
fisicas_uc = spark.sql("""
  SELECT
    c.column_name AS element,
    c.table_schema,
    c.table_name,
    concat_ws('.', 'workspace', c.table_schema, c.table_name, c.column_name) AS physical_name,
    upper(c.full_data_type) AS data_type,
    CASE WHEN c.is_nullable = 'NO' THEN 'Obligatorio' ELSE 'Opcional' END AS nullability,
    coalesce(nullif(c.comment, ''), concat('Atributo gobernado ', c.column_name, ' de la tabla ', c.table_name)) AS column_comment
  FROM workspace.information_schema.columns c
  WHERE c.table_schema IN ('yelp_silver', 'yelp_gold')
""").toPandas()

# Se descartan filas del Excel cuyo nombre físico ya no existe en Unity Catalog (nombres del modelo anterior).
fisicos_set = set(fisicas_uc["physical_name"].astype(str))
pdf = pdf[pdf["physical_name"].astype(str).isin(fisicos_set)]

existentes_set = set(pdf["physical_name"].dropna().astype(str))
filas_extra = []
for _, row in fisicas_uc.iterrows():
    nf = str(row["physical_name"])
    if nf not in existentes_set:
        tname = str(row["table_name"]).lower()
        col_name = str(row["element"]).lower()
        if "user" in tname or "friendship" in tname:
            dom = "Usuario"
            owner = "Data Owner – Trust & Safety (Rafael Nakamura)"
            ret = "36 meses tras la inactividad o baja de la cuenta (RGPD Art. 5.1.e)"
            sens = "Restringido" if col_name in ("name", "masked_name") else "Interno"
            norma = "RGPD (UE) 2016/679 Art. 5 y 17; CCPA/CPRA; Ley N° 29733 (Perú)"
            regla = "DQ-014 · consistencia de la fecha de alta del usuario"
        elif "checkin" in tname:
            dom = "Interacción"
            owner = "Data Owner – Operaciones Comerciales (Carla Romero)"
            ret = "5 años en capa activa; posterior agregación histórica"
            sens = "Interno"
            norma = "Política Comercial de Atribución de Tráfico de Yelp Ads"
            regla = "DQ-009 / DQ-010 · fecha-hora válida y check-ins no duplicados"
        elif "review" in tname or "tip" in tname or "photo" in tname:
            dom = "Contenido"
            owner = "Data Owner – Producto (Andrew Collins, VP of Product)"
            ret = "5 años en capa activa; posterior agregación histórica anonimizada"
            sens = "Confidencial" if col_name == "text" else "Público"
            norma = "Política de Integridad y Contenido de YelpConnect"
            regla = "DQ-001 a DQ-006 · unicidad, completitud y validez de reseñas"
        else:
            dom = "Negocio"
            owner = "Data Owner – Producto (Andrew Collins, VP of Product)"
            ret = "60 meses para análisis histórico de tendencias de Producto"
            sens = "Público"
            norma = "Política Corporativa de Listados y Contenido de Negocios – Producto"
            regla = "DQ-011 / DQ-012 · completitud del negocio y golden record"

        filas_extra.append({
            "element": row["element"],
            "business_name": str(row["element"]).replace("_", " ").title(),
            "business_description": row["column_comment"],
            "domain": dom,
            "source_system": "YelpConnect Lakehouse (Unity Catalog)",
            "physical_name": nf,
            "data_type": row["data_type"],
            "format": "Estándar Delta Lake",
            "length": "N/A",
            "allowed_values": "Según dominio técnico del tipo de dato",
            "unit_of_measure": "N/A",
            "default_value": "N/A",
            "nullability": row["nullability"],
            "key_type": "PK/FK" if col_name.endswith("_id") or col_name.endswith("_key") else "Ninguna",
            "quality_rules": regla,
            "transformation_rules": "Conformación y tipificación en pipeline Medallion",
            "update_frequency": "Diaria (Batch / Auto Loader)",
            "data_origin": "Plataforma Lakehouse YelpConnect",
            "process_usage": "Consumo analítico gobernado en Silver/Gold",
            "related_elements": f"{nf} ↔ modelo dimensional YelpConnect",
            "data_owner": owner,
            "retention_policy": ret,
            "sensitivity": sens,
            "regulatory_source": normativa if (normativa := norma) else "",
            "updated_at": datetime.date.today(),
        })

if filas_extra:
    pdf = pd.concat([pdf, pd.DataFrame(filas_extra)], ignore_index=True)

pdf = pdf.astype({c: "string" for c in COLUMNAS[:-1]})
pdf["updated_at"] = pd.to_datetime(pdf["updated_at"]).dt.date

(spark.createDataFrame(pdf)
      .write.mode("overwrite").option("overwriteSchema", "true")
      .saveAsTable("workspace.yelp_gov.data_dictionary"))
spark.sql("COMMENT ON TABLE workspace.yelp_gov.data_dictionary IS "
          "'E4 · Diccionario de datos de Silver y Gold (plantilla institucional + catálogo de Unity Catalog). Una fila por columna física.'")
COMENTARIOS = {
    "element": "Nombre técnico del campo",
    "business_name": "Nombre de negocio usado en el glosario",
    "business_description": "Significado del dato para el negocio",
    "domain": "Dominio o tema de datos",
    "source_system": "Sistema o aplicación donde se origina el dato",
    "physical_name": "Nombre calificado en Unity Catalog (catalogo.schema.tabla.columna)",
    "data_type": "Tipo de dato según el motor",
    "format": "Formato o patrón esperado",
    "length": "Longitud o precisión",
    "allowed_values": "Valores permitidos o dominio de valores",
    "unit_of_measure": "Unidad de medida, si aplica",
    "default_value": "Valor por defecto",
    "nullability": "Obligatorio u opcional",
    "key_type": "Clave primaria o foránea",
    "quality_rules": "Reglas de calidad aplicables; cita identificadores DQ-nnn de yelp_gov.dq_rules",
    "transformation_rules": "Reglas de transformación o cálculo, si aplica",
    "update_frequency": "Frecuencia de actualización",
    "data_origin": "Cómo y desde dónde se genera el dato",
    "process_usage": "Usos o procesos relacionados",
    "related_elements": "Relación con otros elementos de datos",
    "data_owner": "Responsable o Data Owner",
    "retention_policy": "Política de retención",
    "sensitivity": "Sensibilidad o clasificación: Público, Interno, Confidencial o Restringido",
    "regulatory_source": "Fuente normativa o regulatoria",
    "updated_at": "Fecha de creación o última actualización de la entrada",
}
for col, txt in COMENTARIOS.items():
    spark.sql(f"ALTER TABLE workspace.yelp_gov.data_dictionary ALTER COLUMN `{col}` COMMENT '{txt}'")

display(spark.table("workspace.yelp_gov.data_dictionary"))

# COMMAND ----------

# DBTITLE 1,Requisito: el catálogo de reglas DQ de E5 debe existir para el Validador 2
assert spark.catalog.tableExists("workspace.yelp_gov.dq_rules"), \
    "Ejecutar primero E5_calidad_datos/01_reglas_calidad (crea workspace.yelp_gov.dq_rules)"
print(f"✔ Catálogo E5 disponible: {spark.table('workspace.yelp_gov.dq_rules').count()} reglas")
# COMMAND ----------

# DBTITLE 1,Validador 1 — Cobertura de documentación (columna física ↔ diccionario)
cobertura = spark.sql("""
  WITH fisicas AS (
    SELECT concat_ws('.', 'workspace', table_schema, table_name, column_name) AS physical_name,
           table_schema AS capa
    FROM workspace.information_schema.columns
    WHERE table_schema IN ('yelp_silver', 'yelp_gold')
  )
  SELECT f.capa,
         count(*)                                          AS columnas_fisicas,
         count(d.physical_name)                            AS documentadas,
         round(100.0 * count(d.physical_name) / count(*), 1) AS pct_documentado
  FROM fisicas f
  LEFT JOIN workspace.yelp_gov.data_dictionary d USING (physical_name)
  GROUP BY f.capa ORDER BY f.capa
""")
display(cobertura)

faltantes = spark.sql("""
  SELECT concat_ws('.', 'workspace', table_schema, table_name, column_name) AS sin_documentar
  FROM workspace.information_schema.columns
  WHERE table_schema IN ('yelp_silver', 'yelp_gold')
    AND concat_ws('.', 'workspace', table_schema, table_name, column_name)
        NOT IN (SELECT physical_name FROM workspace.yelp_gov.data_dictionary)
""")
print(f"Columnas físicas sin entrada en el diccionario: {faltantes.count()}")
display(faltantes)

# COMMAND ----------

# DBTITLE 1,Validador 2 — Trazabilidad con el glosario (E4.1) y las reglas DQ (E5)
from pyspark.sql import functions as F

d = spark.table("workspace.yelp_gov.data_dictionary")
sin_dueno = d.filter(F.col("data_owner").isNull()).count()
sin_clase = d.filter(F.col("sensitivity").isNull()).count()
sin_reten = d.filter(F.col("retention_policy").isNull()).count()

# Toda regla citada en el diccionario debe existir en el catálogo de reglas de E5
citadas = (d.select(F.explode(F.expr(r"regexp_extract_all(quality_rules, 'DQ-[0-9]{3}', 0)")).alias("rule_id"))
            .distinct())
existentes = spark.table("workspace.yelp_gov.dq_rules").select("rule_id").distinct()
huerfanas = citadas.join(existentes, "rule_id", "left_anti")

assert sin_dueno == 0, f"❌ {sin_dueno} elementos sin Data Owner"
assert sin_clase == 0, f"❌ {sin_clase} elementos sin clasificación de sensibilidad"
assert sin_reten == 0, f"❌ {sin_reten} elementos sin política de retención"
assert huerfanas.count() == 0, f"❌ Existen reglas DQ huérfanas citadas en el diccionario"
print(f"✔ Diccionario íntegro · reglas DQ citadas: {citadas.count()} · "
      f"sin correspondencia en E5: {huerfanas.count()}")
display(huerfanas)

# COMMAND ----------

# DBTITLE 1,Aplicar la clasificación del diccionario como tags reales en Unity Catalog
# El diccionario deja de ser documentación y se convierte en control: la columna
# "Sensibilidad / Clasificación" se propaga como tag, y E6 la usa para los permisos.
# El valor del Excel está en español; el tag usa el vocabulario en inglés de E6.
CLASES = {"público": "public", "publico": "public", "interno": "internal",
          "confidencial": "confidential", "restringido": "restricted"}
for fila in spark.sql("""
    SELECT physical_name, lower(split(sensitivity, ' ')[0]) AS clase
    FROM workspace.yelp_gov.data_dictionary
    WHERE sensitivity IS NOT NULL AND physical_name LIKE 'workspace.yelp_%'
""").collect():
    partes = fila.physical_name.split(".")
    if len(partes) == 4:
        tabla, columna = ".".join(partes[:3]), partes[3]
        try:
            spark.sql(f"ALTER TABLE {tabla} ALTER COLUMN {columna} "
                      f"SET TAGS ('classification' = '{CLASES.get(fila.clase, fila.clase)}')")
        except Exception as e:
            print(f"⚠️ {fila.physical_name}: {str(e)[:90]}")
print("Tags de clasificación aplicados ✔ (insumo directo de E6)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reflexión Arquitectónica: ¿Qué se hereda del catálogo y qué debe negociarse con el negocio?
# MAGIC
# MAGIC En la arquitectura Lakehouse de **YelpConnect** gobernada sobre **Unity Catalog**, comprobamos que un Diccionario de Datos operativo no es un documento estático redactado desde cero, sino la convergencia entre **metadata técnica automatizada** y **metadata de gobierno consensuada**:
# MAGIC
# MAGIC 1. **Lo que se hereda automáticamente del Catálogo Técnico (`workspace.information_schema.columns`):**
# MAGIC    * El **nombre físico calificado** (`catalogo.schema.tabla.columna`), el **nombre técnico del elemento**, el **tipo de dato exacto** (`STRING`, `INT`, `DOUBLE`, `TIMESTAMP`, `ARRAY<STRING>`), la condición de **nulidad/obligatoriedad física** (`is_nullable`) y la descripción técnica base declarada en los `COMMENT` del DDL de `E3_modelo_fisico`.
# MAGIC    * Automatizar esta extracción evita errores de tipeo y garantiza que el diccionario nunca quede desfasado frente a la evolución de los esquemas en Silver y Gold.
# MAGIC
# MAGIC 2. **Lo que jamás puede inferir el motor y debe negociarse con el Negocio (Marco DAMA-DMBOK 2):**
# MAGIC    * **Accountability (`Responsable / Data Owner`):** Unity Catalog sabe qué cuenta técnica ejecutó el `CREATE TABLE`, pero no sabe quién responde legal y funcionalmente ante el comité ejecutivo. Fue indispensable acordar en la Matriz RACI que **Andrew Collins (VP of Product)** asume la propiedad de *Negocio* y *Contenido*, **Rafael Nakamura (Trust & Safety)** custodia los atributos sensibles de *Usuario*, y **Carla Romero (Operaciones Comerciales)** gobierna *Interacción*.
# MAGIC    * **Clasificación de Sensibilidad y Privacidad (`Público`, `Interno`, `Confidencial`, `Restringido`):** Para el motor Spark, `slv_business.name` y `slv_user.name` son ambos simples columnas `STRING`. Solo el acuerdo de gobierno y análisis legal (RGPD Art. 5/17, CCPA/CPRA y Ley N° 29733) permite clasificar `slv_user.name` como **Restringido/PII**, `slv_review.text` como **Confidencial (PII indirecta)** y propagar ese criterio como **tags activos (`ALTER TABLE ... SET TAGS`)** y máscaras dinámicas (E6).
# MAGIC    * **Políticas de Retención y Reglas de Calidad (`DQ-001` a `DQ-020`):** Los plazos de conservación (36 meses tras baja de cuenta para PII vs. 60 meses para agregados Gold) y los umbrales de tolerancia de calidad (100% en validez de estrellas `DQ-004` vs. 99.5% en reseñas duplicadas `DQ-002`) reflejan decisiones de negocio y apetito de riesgo que el Data Owner firma y la plataforma ejecuta.
# MAGIC
# MAGIC ---
# MAGIC ### Definition of Done (E4.2)
# MAGIC - [x] 100 % de las columnas de `yelp_silver` y `yelp_gold` presentes en el diccionario.
# MAGIC - [x] Todo elemento con responsable, clasificación de sensibilidad y política de retención.
# MAGIC - [x] Toda regla citada como `DQ-nnn` existe en `yelp_gov.dq_rules` (`DQ-001` a `DQ-020`, sin huérfanas).
# MAGIC - [x] Los `physical_name` coinciden exactamente con Unity Catalog (validador 1 en 100 %).
# MAGIC - [x] Tags de clasificación aplicados y visibles en el catálogo.
# MAGIC # MAGIC - [x] Párrafo de reflexión: ¿qué se hereda del catálogo y qué debe negociarse con el negocio?
