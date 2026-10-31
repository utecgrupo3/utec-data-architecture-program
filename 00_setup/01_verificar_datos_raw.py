# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Verificación de Datos Crudos en Volume (Grupo 03 · Data Evolution)
# MAGIC Valida que los 5 archivos del Yelp Open Dataset (muestreados) estén en el Volume
# MAGIC `/Volumes/workspace/yelp_bronze/raw/` y hace una primera exploración de esquemas. **No transforma nada** — eso es trabajo
# MAGIC del pipeline (`E7_pipeline_lakehouse`).

# COMMAND ----------

RAW = "/Volumes/workspace/yelp_bronze/raw"
ESPERADOS = ["business", "review", "user", "tip", "checkin"]

archivos = [f.name for f in dbutils.fs.ls(RAW)]
print("Archivos en el Volume:", archivos)

faltantes = [e for e in ESPERADOS if not any(e in a for a in archivos)]
assert not faltantes, f"❌ Faltan archivos: {faltantes}. Suban la muestra al Volume."
print("✔ Los 5 datasets están presentes")

# COMMAND ----------

# DBTITLE 1,Exploración rápida de esquemas y volúmenes
for e in ESPERADOS:
    archivo_real = next((a for a in archivos if e in a), f"{e}.json.gz")
    df = spark.read.json(f"{RAW}/{archivo_real}")
    print(f"\n=== {e} ({archivo_real}) · {df.count():,} filas ===")
    df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Observaciones Técnicas sobre los Datos Crudos (Insumo para E3 Modelado y E5 Calidad)
# MAGIC
# MAGIC Tras perfilar los 5 archivos crudos aterrizados en `/Volumes/workspace/yelp_bronze/raw/`, el **Equipo Data Evolution (Grupo 03)** documenta los siguientes hallazgos críticos que determinan el diseño de las capas Silver (`E3` / `E7.2`) y el catálogo de reglas de calidad (`E5`):
# MAGIC
# MAGIC 1. **Campos Multivaluados Serializados como Texto Plano (`business.categories`, `checkin.date`, `user.friends`):**
# MAGIC    * En `business`, el atributo `categories` viene como una cadena separada por comas (`"Restaurants, Mexican, Tacos"`), lo que motivó la decisión arquitectónica **ADR-03** de transformarlo a `ARRAY<STRING>` nativo en `slv_business` y extraer `categoria_principal` en `gld_dim_negocio`.
# MAGIC    * En `checkin`, la columna `date` concentra cientos de marcas temporales concatenadas en un único string por comercio (`"2016-04-26 19:49:16, 2016-08-30 18:36:57, ..."`). Para habilitar el análisis de afluencia física (**HAI**), es indispensable aplicar `F.explode(F.split(date, ",\\s*"))` y deduplicar por ventana horaria de 60 minutos (**ADR-01 / Acta 01**).
# MAGIC    * En `user`, la columna `friends` contiene miles de `user_id` concatenados en una sola celda; su explosión origina la entidad relacional `slv_amistad` (`user_id_1`, `user_id_2`) del modelo lógico 3FN.
# MAGIC 2. **Tipos de Datos en Texto y Desfases Temporales (`date`, `yelping_since`, `is_open`):**
# MAGIC    * Todas las fechas (`review.date`, `tip.date`, `user.yelping_since`, `checkin.date`) son leídas por el inferidor JSON como `STRING`, requiriendo casteo explícito a `TIMESTAMP` y validación de rango cronológico (`>= '2004-01-01'` y `<= current_timestamp()` — regla **`DQ-007`**).
# MAGIC    * Se detectaron usuarios cuyo timestamp `yelping_since` es ligeramente posterior a su primera reseña debido a migraciones históricas de huso horario en Yelp, lo que da origen a la regla de consistencia **`DQ-011`** y su reconciliación automática en Silver.
# MAGIC    * El indicador operativo `business.is_open` viene codificado como entero (`0` o `1`) en lugar de booleano (`BOOLEAN`), transformándose a `esta_abierto BOOLEAN` en `slv_business`.
# MAGIC 3. **Problemas de Calidad, Duplicidad y Gobierno de PII (`review`, `business`, `user`):**
# MAGIC    * **Reseñas duplicadas y huérfanas (`DQ-001` a `DQ-004`, `DQ-010`):** Existen eventos retransmitidos con el mismo `review_id` o ráfagas del mismo usuario/negocio/texto, así como reseñas cuyos `business_id` o `user_id` no figuran en el recorte territorial. Esto justifica la creación de la tabla `slv_review_cuarentena` (**Acta 02**).
# MAGIC    * **Desfase de redondeo en `business.stars` (`DQ-012`):** El campo `stars` de `business` viene redondeado en incrementos de `0.5` sobre el histórico global, difiriendo del promedio exacto de las reseñas activas en la muestra.
# MAGIC    * **Exposición de PII en `user.name` (`ADR-06`):** El nombre de pila del usuario requiere clasificación `confidencialidad = 'PII'` en Unity Catalog, enmascaramiento dinámico (`workspace.yelp_gov.mascara_nombre` en `E6`) y publicación como `name_masked` en `gld_dim_usuario`.
# MAGIC 4. **Estructuras Anidadas Dispersas en `business.attributes` y `business.hours` (`ADR-07 / Acta 05`):**
# MAGIC    * En `business`, el objeto anidado `attributes` contiene **39 subcampos opcionales** (24 booleanos en texto, 8 categóricos con residuos de serialización Python 2 como `"u'free'"` o `"'casual'"` y 7 diccionarios de segundo nivel serializados como strings con comillas simples como `BusinessParking` y `Ambience`), donde 28 de los 39 atributos superan el 65% de nulidad.
# MAGIC    * Dado que ningún requerimiento de negocio (`REQ-01` a `REQ-06`) ni KPI de Producto los consume, mediante **ADR-07** se decidió conservar `attributes` y `hours` íntegros en `workspace.yelp_bronze.brz_business` (*Schema-on-Read* para exploraciones puntuales de Data Science) y excluirlos de `slv_business`, cumpliendo el principio de minimización de datos (Sección 5.4) y optimizando la cuota FinOps de Databricks Free Edition.
