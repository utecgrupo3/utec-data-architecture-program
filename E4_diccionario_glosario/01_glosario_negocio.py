# Databricks notebook source
# MAGIC %md
# MAGIC # E4.1 · Glosario de Negocio — Grupo 03 (Data Evolution)
# MAGIC **Cierra con:** M3 MDM y Metadata · **Formato oficial:** plantilla institucional
# MAGIC `Glosario_de_terminos_de_negocio_YelpConnect_PLANTILLA.xlsx` del bloque de Seguridad, Gobernanza y Compliance.
# MAGIC
# MAGIC El glosario resuelve el problema central del caso: distintas áreas llaman igual a cosas
# MAGIC distintas ("check-in", "reseña válida", "negocio activo"). Cada término tiene UNA
# MAGIC definición acordada y UN propietario.
# MAGIC
# MAGIC **Flujo del entregable:**
# MAGIC 1. El grupo negocia y redacta los términos en el **Excel institucional** (interfaz de negocio).
# MAGIC 2. Sube (o sincroniza desde `plantillas/`) el Excel al Volume `/Volumes/workspace/yelp_gov/artefactos/`.
# MAGIC 3. Este notebook lo carga a `yelp_gov.glosario_negocio` (fuente consultable).
# MAGIC 4. Los validadores comprueban el Definition of Done.
# MAGIC
# MAGIC > La tabla Delta tiene **exactamente las 12 columnas de la plantilla**: el Excel no se
# MAGIC > "traduce", se carga. Ese es el principio de un glosario vivo.
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la sección «Anexo A · Glosario de Términos de Negocio» y las Secciones 4.1, 5.1 y 7.2 del
# MAGIC > **Documento Formal de Arquitectura de Datos (v1.2 · Equipo Data Evolution)**. El Excel institucional se anexa al documento; esta tabla Delta es la versión consultable desde Genie y el dashboard de gobierno.

# COMMAND ----------

# DBTITLE 1: Asegurar dependencia openpyxl para lectura de plantillas Excel en Serverless
# MAGIC %pip install -q openpyxl

# COMMAND ----------

# DBTITLE 1: Volume donde el grupo sube y sincroniza los artefactos de gobierno
import os
import shutil

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.yelp_gov")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.yelp_gov.artefactos")

RUTA = "/Volumes/workspace/yelp_gov/artefactos/Glosario_de_terminos_de_negocio_YelpConnect.xlsx"
RUTA_PLANTILLA_LOCAL = os.path.join(
    os.getcwd(), "plantillas", "Glosario_de_terminos_de_negocio_YelpConnect_PLANTILLA.xlsx"
)

if os.path.exists(RUTA_PLANTILLA_LOCAL):
    shutil.copy2(RUTA_PLANTILLA_LOCAL, RUTA)
    shutil.copy2(
        RUTA_PLANTILLA_LOCAL,
        "/Volumes/workspace/yelp_gov/artefactos/Glosario_de_terminos_de_negocio_YelpConnect_PLANTILLA.xlsx",
    )
    print(f"✔ Plantilla sincronizada automáticamente al Volume: {RUTA}")
else:
    print(f"Usando archivo en Volume: {RUTA}")

# COMMAND ----------

# DBTITLE 1: Carga del Excel institucional → tabla Delta
import pandas as pd

COLUMNAS = ["termino", "sinonimos", "definicion_negocio", "definicion_tecnica",
            "contexto_uso", "dominio_datos", "categoria", "ejemplo_uso",
            "relaciones", "propietario", "fuente_normativa", "fecha_actualizacion"]

# La plantilla trae 3 filas de cabecera institucional + 1 de identificación del caso;
# los encabezados de tabla están en la fila 5 y los datos comienzan en la fila 6.
pdf = pd.read_excel(RUTA, sheet_name="Glosario", skiprows=4)
pdf = pdf.dropna(how="all").iloc[:, :12]
pdf.columns = COLUMNAS
pdf["fecha_actualizacion"] = pd.to_datetime(pdf["fecha_actualizacion"]).dt.date

(spark.createDataFrame(pdf)
      .write.mode("overwrite").option("overwriteSchema", "true")
      .saveAsTable("workspace.yelp_gov.glosario_negocio"))

display(spark.table("workspace.yelp_gov.glosario_negocio"))

# COMMAND ----------

# DBTITLE 1: Validadores del Definition of Done (no modificar)
from pyspark.sql import functions as F

g = spark.table("workspace.yelp_gov.glosario_negocio")
n = g.count()
sin_owner = g.filter(F.col("propietario").isNull() | F.col("propietario").contains("✏️")).count()
sin_dominio = g.select("dominio_datos").distinct().count()
con_relaciones = g.filter(F.col("relaciones").isNotNull()).count()
duplicados = n - g.select("termino").distinct().count()

assert n >= 20, f"❌ Solo hay {n} términos; el DoD exige ≥20"
assert sin_owner == 0, "❌ Hay términos sin propietario asignado"
assert duplicados == 0, f"❌ Hay {duplicados} términos repetidos"
assert sin_dominio >= 4, "❌ El glosario debe cubrir al menos 4 dominios de datos"
assert con_relaciones / n >= 0.8, "❌ Menos del 80 % de los términos declara relaciones"
print(f"✔ Glosario válido: {n} términos · {sin_dominio} dominios · "
      f"{round(100*con_relaciones/n)} % con relaciones declaradas")

# COMMAND ----------

# DBTITLE 1: Conflictos semánticos del caso — verificación explícita
# MAGIC %sql
# MAGIC -- Los tres conflictos del enunciado deben estar resueltos con un término propio
# MAGIC SELECT termino, dominio_datos, propietario
# MAGIC FROM workspace.yelp_gov.glosario_negocio
# MAGIC WHERE lower(termino) LIKE '%verificada%'      -- Marketing ↔ Trust & Safety
# MAGIC    OR lower(termino) LIKE '%check-in válido%' -- Operaciones Comerciales
# MAGIC    OR lower(termino) LIKE '%duplicada%'       -- Marketing
# MAGIC ORDER BY termino;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Acta de Negociación Semántica del Equipo (Grupo 03 · Data Evolution)
# MAGIC
# MAGIC Conforme al **Anexo D (Actas de Decisión del Equipo)** y la **Sección 5 (Estrategia de Gobierno y Matriz RACI)** del *Documento Formal de Arquitectura de Datos (v1.2)*, los 23 términos del glosario fueron acordados mediante cuatro sesiones de conciliación entre los representantes de cada dominio:
# MAGIC
# MAGIC | Acta | Fecha | Conflicto Semántico entre Áreas | Acuerdo Institucional Aprobado | Data Owners Firmantes |
# MAGIC | :--- | :--- | :--- | :--- | :--- |
# MAGIC | **Acta 01** | 19/08/2026 | **Conteo de Check-ins (`Check-in` vs `Check-in Válido`):** Operaciones Comerciales (Yelp Ads) contabilizaba cada marca temporal cruda para facturación, mientras Producto exigía máximo un check-in diario para medir retención real (brecha del 28%). | Se formalizó el término **`Check-in Válido`** deduplicado por ventana móvil de **60 minutos** por establecimiento (**ADR-01**). Elimina rebotes técnicos sin perder visitas reales en distintos momentos del día, alimentando el **Índice de Afluencia Homologada (HAI)**. | **Carla Romero** (Op. Comerciales) y **Andrew Collins** (VP of Product) |
# MAGIC | **Acta 02** | 02/09/2026 | **Validez y Duplicidad de Reseñas (`Reseña Verificada`, `Reseña Duplicada`, `Reseña en Cuarentena`):** Marketing & Growth incluía reseñas repetidas de campañas masivas; Trust & Safety solicitaba eliminar físicamente cualquier registro sospechoso. | Se separaron tres conceptos: **`Reseña Verificada`** (aprueba el 100% de reglas DQ críticas y alimenta el rating público), **`Reseña Duplicada`** (reiteración del mismo usuario/comercio en <24h o `review_id` repetido) y **`Reseña en Cuarentena`** (aislada preventivamente para auditoría forense sin distorsionar Gold). | **Rafael Nakamura** (Trust & Safety) y **Andrew Collins** (VP of Product) |
# MAGIC | **Acta 03** | 16/09/2026 | **Taxonomía de `Categoría Comercial` y `Rating Promedio Ponderado Bayesiano (BWR)`:** Comercios con una sola reseña de 5 estrellas superaban en el buscador a negocios consolidados con cientos de opiniones. | Se aprobó el término **`Rating Promedio Ponderado Bayesiano (BWR)`** con umbral de credibilidad $m=25$ reseñas y media global $C$ de la ciudad (**ADR-02**), junto con la representación de **`Categoría Comercial`** como arreglo nativo (**ADR-03**). | **Andrew Collins** (VP of Product) y **Laura Hernández** (CDO) |
# MAGIC | **Acta 04** | 30/09/2026 | **Métricas de Comunidad (`Cohorte de Usuario`, `Usuario Activo`, `Tasa de Retención de Cohortes (CRR)`):** Ausencia de criterio uniforme para medir la permanencia de usuarios tras el alta (`yelping_since`). | Se estandarizó **`Cohorte de Usuario`** por mes calendario de alta y **`Usuario Activo`** como aquel con al menos una interacción validada en el periodo, habilitando el seguimiento de **CRR** y **Stickiness (DAU/MAU)** (**ADR-05**). | **Andrew Collins** (VP of Product) y **Rafael Nakamura** (Trust & Safety) |
# MAGIC
# MAGIC ---
# MAGIC ### Definition of Done (E4.1)
# MAGIC - [x] ≥20 términos en el formato institucional (23 términos aprobados), sin duplicados y cubriendo ≥4 dominios (6 dominios cubiertos).
# MAGIC - [x] Todo término con propietario correspondiente a un área real del caso (Andrew Collins, Rafael Nakamura, Carla Romero, Laura Hernández).
# MAGIC - [x] Los 3 conflictos semánticos del caso resueltos con término propio (`Reseña Verificada`, `Reseña Duplicada`, `Check-in Válido`).
# MAGIC - [x] La definición de negocio no nombra tablas ni columnas (eso va exclusivamente en la definición técnica).
# MAGIC - [x] Acta breve (markdown aquí) de **cómo se negociaron** las definiciones en el grupo.
