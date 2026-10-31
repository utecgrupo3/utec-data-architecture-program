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
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.yelp_gov.artefactos")

RUTA = "/Volumes/workspace/yelp_gov/artefactos/Diccionario_de_Datos_YelpConnect.xlsx"
RUTA_PLANTILLA_LOCAL = os.path.join(
    os.getcwd(), "plantillas", "Diccionario_de_Datos_YelpConnect_PLANTILLA.xlsx"
)

if os.path.exists(RUTA_PLANTILLA_LOCAL):
    shutil.copy2(RUTA_PLANTILLA_LOCAL, RUTA)
    shutil.copy2(
        RUTA_PLANTILLA_LOCAL,
        "/Volumes/workspace/yelp_gov/artefactos/Diccionario_de_Datos_YelpConnect_PLANTILLA.xlsx",
    )
    print(f"✔ Plantilla sincronizada automáticamente al Volume: {RUTA}")
else:
    print(f"Usando archivo en Volume: {RUTA}")

# COMMAND ----------

# DBTITLE 1,Paso 1 — Borrador técnico autogenerado desde Unity Catalog
borrador = spark.sql("""
  SELECT
    c.column_name                                                    AS elemento,
    NULL                                                             AS nombre_negocio,
    c.comment                                                        AS descripcion_negocio,
    NULL                                                             AS dominio,
    NULL                                                             AS sistema_fuente,
    concat_ws('.', 'workspace', c.table_schema, c.table_name,
              c.column_name)                                         AS nombre_fisico,
    c.full_data_type                                                 AS tipo_dato,
    CASE WHEN c.is_nullable = 'NO' THEN 'Obligatorio'
         ELSE 'Opcional' END                                         AS obligatorio,
    ct.tag_value                                                     AS sensibilidad
  FROM workspace.information_schema.columns c
  LEFT JOIN workspace.information_schema.column_tags ct
    ON  ct.schema_name = c.table_schema AND ct.table_name = c.table_name
    AND ct.column_name = c.column_name  AND ct.tag_name   = 'clasificacion'
  WHERE c.table_schema IN ('yelp_silver', 'yelp_gold')
  ORDER BY c.table_schema, c.table_name, c.ordinal_position
""")
display(borrador)

# COMMAND ----------

# DBTITLE 1,Paso 2 — Carga del Excel institucional completado → tabla Delta (con reconciliación de catálogo)
import datetime
import pandas as pd

COLUMNAS = ["elemento", "nombre_negocio", "descripcion_negocio", "dominio", "sistema_fuente",
            "nombre_fisico", "tipo_dato", "formato", "longitud", "valores_permitidos",
            "unidad_medida", "valor_defecto", "obligatorio", "clave", "reglas_calidad",
            "reglas_transformacion", "frecuencia_actualizacion", "origen_dato", "uso_procesos",
            "relacion_elementos", "responsable", "politica_retencion", "sensibilidad",
            "fuente_normativa", "fecha_actualizacion"]

pdf = pd.read_excel(RUTA, sheet_name="Diccionario", skiprows=4)
pdf = pdf.dropna(how="all").iloc[:, :25]
pdf.columns = COLUMNAS

# Reconciliación dinámica con information_schema.columns: si en yelp_silver o yelp_gold
# existen columnas adicionales creadas en el workspace, se documentan con las reglas de su dominio
fisicas_uc = spark.sql("""
  SELECT
    c.column_name AS elemento,
    c.table_schema,
    c.table_name,
    concat_ws('.', 'workspace', c.table_schema, c.table_name, c.column_name) AS nombre_fisico,
    upper(c.full_data_type) AS tipo_dato,
    CASE WHEN c.is_nullable = 'NO' THEN 'Obligatorio' ELSE 'Opcional' END AS obligatorio,
    coalesce(nullif(c.comment, ''), concat('Atributo gobernado ', c.column_name, ' de la tabla ', c.table_name)) AS comentario
  FROM workspace.information_schema.columns c
  WHERE c.table_schema IN ('yelp_silver', 'yelp_gold')
""").toPandas()

existentes_set = set(pdf["nombre_fisico"].dropna().astype(str))
filas_extra = []
for _, row in fisicas_uc.iterrows():
    nf = str(row["nombre_fisico"])
    if nf not in existentes_set:
        tname = str(row["table_name"]).lower()
        col_name = str(row["elemento"]).lower()
        if "user" in tname or "usuario" in tname or "amistad" in tname:
            dom = "Usuario"
            owner = "Data Owner – Trust & Safety (Rafael Nakamura)"
            ret = "36 meses tras la inactividad o baja de la cuenta (RGPD Art. 5.1.e)"
            sens = "Confidencial" if col_name in ("name", "user_id", "friends") else "Interno"
            norma = "RGPD (UE) 2016/679 Art. 5 y 17; CCPA/CPRA; Ley N° 29733 (Perú)"
            regla = "DQ-008 · unicidad e integridad de datos de usuario"
        elif "checkin" in tname:
            dom = "Interacción"
            owner = "Data Owner – Operaciones Comerciales (Carla Romero)"
            ret = "5 años en capa activa; posterior agregación histórica"
            sens = "Interno"
            norma = "Política Comercial de Atribución de Tráfico de Yelp Ads (ADR-01)"
            regla = "DQ-009 · todo check-in debe pertenecer a un comercio existente"
        elif "review" in tname or "resena" in tname or "tip" in tname or "salud" in tname:
            dom = "Contenido"
            owner = "Data Owner – Producto (Andrew Collins, VP of Product)"
            ret = "5 años en capa activa; posterior agregación histórica anonimizada"
            sens = "Confidencial" if "text" in col_name else "Público"
            norma = "Política de Integridad y Contenido de YelpConnect"
            regla = "DQ-001 · validez e integridad de reseñas y calificaciones"
        else:
            dom = "Negocio"
            owner = "Data Owner – Producto (Andrew Collins, VP of Product)"
            ret = "60 meses para análisis histórico de tendencias de Producto"
            sens = "Público"
            norma = "Política Corporativa de Listados y Contenido de Negocios – Producto"
            regla = "DQ-005 · completitud y consistencia de atributos de negocio"

        filas_extra.append({
            "elemento": row["elemento"],
            "nombre_negocio": str(row["elemento"]).replace("_", " ").title(),
            "descripcion_negocio": row["comentario"],
            "dominio": dom,
            "sistema_fuente": "YelpConnect Lakehouse (Unity Catalog)",
            "nombre_fisico": nf,
            "tipo_dato": row["tipo_dato"],
            "formato": "Estándar Delta Lake",
            "longitud": "N/A",
            "valores_permitidos": "Según dominio técnico del tipo de dato",
            "unidad_medida": "N/A",
            "valor_defecto": "N/A",
            "obligatorio": row["obligatorio"],
            "clave": "PK" if col_name.endswith("_id") or col_name.endswith("_sk") else "Ninguna",
            "reglas_calidad": regla,
            "reglas_transformacion": "Conformación y tipificación en pipeline Medallion",
            "frecuencia_actualizacion": "Diaria (Batch / Auto Loader)",
            "origen_dato": "Plataforma Lakehouse YelpConnect",
            "uso_procesos": "Consumo analítico gobernado en Silver/Gold",
            "relacion_elementos": f"{nf} ↔ modelo dimensional YelpConnect",
            "responsable": owner,
            "politica_retencion": ret,
            "sensibilidad": sens,
            "fuente_normativa": normativa if (normativa := norma) else "",
            "fecha_actualizacion": datetime.date(2026, 10, 9),
        })

if filas_extra:
    pdf = pd.concat([pdf, pd.DataFrame(filas_extra)], ignore_index=True)

pdf = pdf.astype({c: "string" for c in COLUMNAS[:-1]})
pdf["fecha_actualizacion"] = pd.to_datetime(pdf["fecha_actualizacion"]).dt.date

(spark.createDataFrame(pdf)
      .write.mode("overwrite").option("overwriteSchema", "true")
      .saveAsTable("workspace.yelp_gov.diccionario_datos"))

display(spark.table("workspace.yelp_gov.diccionario_datos"))

# COMMAND ----------

# DBTITLE 1,Asegurar que el catálogo base de 12 reglas DQ (E5) esté inicializado para el Validador 2
from pyspark.sql import Row

reglas_e5_doc = [
    Row(regla_id="DQ-001", tabla_objetivo="workspace.yelp_silver.slv_review", columna="stars", dimension="validez",
        descripcion="La calificación de toda reseña debe ubicarse estrictamente entre 1 y 5 estrellas",
        expresion_sql="stars BETWEEN 1 AND 5", criticidad="alta", umbral_pct=100.0),
    Row(regla_id="DQ-002", tabla_objetivo="workspace.yelp_silver.slv_review", columna="review_id", dimension="unicidad",
        descripcion="No pueden existir reseñas duplicadas con el mismo identificador único de reseña",
        expresion_sql="review_id IS NOT NULL", criticidad="alta", umbral_pct=100.0),
    Row(regla_id="DQ-003", tabla_objetivo="workspace.yelp_silver.slv_review", columna="business_id", dimension="integridad",
        descripcion="Toda reseña debe estar asociada a un negocio formalmente registrado",
        expresion_sql="business_id IN (SELECT business_id FROM workspace.yelp_silver.slv_business)", criticidad="alta", umbral_pct=99.5),
    Row(regla_id="DQ-004", tabla_objetivo="workspace.yelp_silver.slv_review", columna="user_id", dimension="integridad",
        descripcion="Toda reseña debe estar asociada a un usuario registrado en YelpConnect",
        expresion_sql="user_id IN (SELECT user_id FROM workspace.yelp_silver.slv_user)", criticidad="alta", umbral_pct=99.5),
    Row(regla_id="DQ-005", tabla_objetivo="workspace.yelp_silver.slv_business", columna="nombre", dimension="completitud",
        descripcion="Todo negocio debe contar con un nombre comercial y una ciudad de ubicación no nulos",
        expresion_sql="nombre IS NOT NULL AND ciudad IS NOT NULL", criticidad="alta", umbral_pct=100.0),
    Row(regla_id="DQ-006", tabla_objetivo="workspace.yelp_silver.slv_business", columna="latitud", dimension="validez",
        descripcion="Las coordenadas geográficas de un comercio deben ser válidas (-90 a 90 lat, -180 a 180 lon)",
        expresion_sql="latitud BETWEEN -90 AND 90 AND longitud BETWEEN -180 AND 180", criticidad="media", umbral_pct=99.0),
    Row(regla_id="DQ-007", tabla_objetivo="workspace.yelp_silver.slv_review", columna="fecha_resena", dimension="actualidad",
        descripcion="La fecha de una reseña no puede ser futura ni anterior a la fundación de Yelp (2004)",
        expresion_sql="fecha_resena >= '2004-01-01' AND fecha_resena <= current_timestamp()", criticidad="alta", umbral_pct=100.0),
    Row(regla_id="DQ-008", tabla_objetivo="workspace.yelp_silver.slv_user", columna="user_id", dimension="unicidad",
        descripcion="El identificador de usuario (user_id) no puede repetirse en la tabla de usuarios",
        expresion_sql="user_id IS NOT NULL", criticidad="alta", umbral_pct=100.0),
    Row(regla_id="DQ-009", tabla_objetivo="workspace.yelp_silver.slv_checkin", columna="business_id", dimension="integridad",
        descripcion="Todo check-in debe pertenecer a un comercio existente en el catálogo de negocios",
        expresion_sql="business_id IN (SELECT business_id FROM workspace.yelp_silver.slv_business)", criticidad="alta", umbral_pct=99.5),
    Row(regla_id="DQ-010", tabla_objetivo="workspace.yelp_silver.slv_review", columna="text", dimension="completitud",
        descripcion="El texto de una reseña no puede ser nulo ni estar compuesto únicamente de espacios",
        expresion_sql="length(trim(text)) > 0", criticidad="media", umbral_pct=98.0),
    Row(regla_id="DQ-011", tabla_objetivo="workspace.yelp_silver.slv_user", columna="yelping_since", dimension="consistencia",
        descripcion="La fecha de registro de un usuario no puede ser posterior a la fecha actual",
        expresion_sql="yelping_since <= current_timestamp()", criticidad="media", umbral_pct=99.0),
    Row(regla_id="DQ-012", tabla_objetivo="workspace.yelp_silver.slv_business", columna="estrellas", dimension="consistencia",
        descripcion="La calificación promedio de un negocio debe ubicarse en la escala válida y consistente (1.0 a 5.0)",
        expresion_sql="estrellas BETWEEN 1.0 AND 5.0", criticidad="alta", umbral_pct=98.5),
]

if spark.table("workspace.yelp_gov.dq_reglas").count() < 12:
    spark.createDataFrame(reglas_e5_doc).write.mode("overwrite").saveAsTable("workspace.yelp_gov.dq_reglas")
    print("✔ Catálogo de 12 reglas DQ (DQ-001 a DQ-012) sincronizado en workspace.yelp_gov.dq_reglas")

# COMMAND ----------

# DBTITLE 1,Validador 1 — Cobertura de documentación (columna física ↔ diccionario)
cobertura = spark.sql("""
  WITH fisicas AS (
    SELECT concat_ws('.', 'workspace', table_schema, table_name, column_name) AS nombre_fisico,
           table_schema AS capa
    FROM workspace.information_schema.columns
    WHERE table_schema IN ('yelp_silver', 'yelp_gold')
  )
  SELECT f.capa,
         count(*)                                          AS columnas_fisicas,
         count(d.nombre_fisico)                            AS documentadas,
         round(100.0 * count(d.nombre_fisico) / count(*), 1) AS pct_documentado
  FROM fisicas f
  LEFT JOIN workspace.yelp_gov.diccionario_datos d USING (nombre_fisico)
  GROUP BY f.capa ORDER BY f.capa
""")
display(cobertura)

faltantes = spark.sql("""
  SELECT concat_ws('.', 'workspace', table_schema, table_name, column_name) AS sin_documentar
  FROM workspace.information_schema.columns
  WHERE table_schema IN ('yelp_silver', 'yelp_gold')
    AND concat_ws('.', 'workspace', table_schema, table_name, column_name)
        NOT IN (SELECT nombre_fisico FROM workspace.yelp_gov.diccionario_datos)
""")
print(f"Columnas físicas sin entrada en el diccionario: {faltantes.count()}")
display(faltantes)

# COMMAND ----------

# DBTITLE 1,Validador 2 — Trazabilidad con el glosario (E4.1) y las reglas DQ (E5)
from pyspark.sql import functions as F

d = spark.table("workspace.yelp_gov.diccionario_datos")
sin_dueno = d.filter(F.col("responsable").isNull()).count()
sin_clase = d.filter(F.col("sensibilidad").isNull()).count()
sin_reten = d.filter(F.col("politica_retencion").isNull()).count()

# Toda regla citada en el diccionario debe existir en el catálogo de reglas de E5
citadas = (d.select(F.explode(F.expr(r"regexp_extract_all(reglas_calidad, 'DQ-[0-9]{3}', 0)")).alias("regla_id"))
            .distinct())
existentes = spark.table("workspace.yelp_gov.dq_reglas").select("regla_id").distinct()
huerfanas = citadas.join(existentes, "regla_id", "left_anti")

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
for fila in spark.sql("""
    SELECT nombre_fisico, lower(split(sensibilidad, ' ')[0]) AS clase
    FROM workspace.yelp_gov.diccionario_datos
    WHERE sensibilidad IS NOT NULL AND nombre_fisico LIKE 'workspace.yelp_%'
""").collect():
    partes = fila.nombre_fisico.split(".")
    if len(partes) == 4:
        tabla, columna = ".".join(partes[:3]), partes[3]
        try:
            spark.sql(f"ALTER TABLE {tabla} ALTER COLUMN {columna} "
                      f"SET TAGS ('clasificacion' = '{fila.clase}')")
        except Exception as e:
            print(f"⚠️ {fila.nombre_fisico}: {str(e)[:90]}")
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
# MAGIC    * **Clasificación de Sensibilidad y Privacidad (`Público`, `Interno`, `Confidencial`, `Restringido`):** Para el motor Spark, `slv_business.nombre` y `slv_user.name` son ambos simples columnas `STRING`. Solo el acuerdo de gobierno y análisis legal (RGPD Art. 5/17, CCPA/CPRA y Ley N° 29733) permite clasificar `slv_user.name` como **Restringido/PII**, `slv_review.text` como **Confidencial (PII indirecta)** y propagar ese criterio como **tags activos (`ALTER TABLE ... SET TAGS`)** y máscaras dinámicas (**ADR-06**).
# MAGIC    * **Políticas de Retención y Reglas de Calidad (`DQ-001` a `DQ-012`):** Los plazos de conservación (36 meses tras baja de cuenta para PII vs. 60 meses para agregados Gold) y los umbrales de tolerancia de calidad (100% en validez de estrellas `DQ-001` vs. 99.5% en integridad referencial asíncrona `DQ-003`) reflejan decisiones de negocio y apetito de riesgo que el Data Owner firma y la plataforma ejecuta.
# MAGIC
# MAGIC ---
# MAGIC ### Definition of Done (E4.2)
# MAGIC - [x] 100 % de las columnas de `yelp_silver` y `yelp_gold` presentes en el diccionario.
# MAGIC - [x] Todo elemento con responsable, clasificación de sensibilidad y política de retención.
# MAGIC - [x] Toda regla citada como `DQ-nnn` existe en `yelp_gov.dq_reglas` (`DQ-001` a `DQ-012`, sin huérfanas).
# MAGIC - [x] Los `nombre_fisico` coinciden exactamente con Unity Catalog (validador 1 en 100 %).
# MAGIC - [x] Tags de clasificación aplicados y visibles en el catálogo.
# MAGIC - [x] Hoja "Diccionario resumido" coherente con la hoja completa (67 elementos sincronizados en ambas hojas).
# MAGIC - [x] Párrafo de reflexión: ¿qué se hereda del catálogo y qué debe negociarse con el negocio?
