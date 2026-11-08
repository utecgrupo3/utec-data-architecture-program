# Databricks notebook source
# MAGIC %md
# MAGIC # Capstone Project — Plataforma de Datos YelpConnect
# MAGIC ### Programa de Arquitectura de Datos · UTEC
# MAGIC
# MAGIC Este repositorio es el **esqueleto base** que cada grupo usará para desplegar los
# MAGIC entregables **ejecutables** del Capstone en **Databricks Free Edition**.
# MAGIC
# MAGIC > **Importante — dónde vive cada entregable.**
# MAGIC > El Capstone tiene **dos soportes complementarios**:
# MAGIC >
# MAGIC > | Soporte | Qué contiene | Naturaleza |
# MAGIC > |---|---|---|
# MAGIC > | **Documento Formal de Arquitectura de Datos** | Visión y SoAW, arquitectura de referencia (ABB/SBB), modelos conceptual y lógico, estrategia de gobierno y matriz RACI, definición de reglas en lenguaje de negocio, requerimientos de información | Diseño, definiciones y conceptual |
# MAGIC > | **Este repositorio (Databricks)** | Modelo físico, glosario y diccionario operativos, motor de calidad, controles técnicos, pipeline, dashboard | Implementación y evidencia ejecutable |
# MAGIC >
# MAGIC > Regla de oro: **el documento define QUÉ y POR QUÉ; la plataforma demuestra QUE SE CUMPLE.**
# MAGIC > Ningún notebook rehace el diseño: cada uno declara en su cabecera qué sección del
# MAGIC > documento formal implementa (bloque **Trazabilidad**).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Setup inicial del grupo (Semana 0 / Onboarding)
# MAGIC
# MAGIC 1. **Un integrante crea la cuenta del grupo** en [Databricks Free Edition](https://www.databricks.com/learn/free-edition)
# MAGIC    (correo del grupo, no personal). Free Edition permite **un solo workspace y un
# MAGIC    metastore por cuenta**, así que la cuenta es del equipo, no de una persona.
# MAGIC 2. Invitar a los 5 integrantes como usuarios del workspace
# MAGIC    (`Settings → Identity and access → Users`).
# MAGIC 3. Crear un repositorio **fork/clon de este esqueleto** en GitHub
# MAGIC    (nombre sugerido: `capstone-grupoNN-yelpconnect`).
# MAGIC 4. En Databricks: `Workspace → Create → Git folder` y conectar el repo del grupo.
# MAGIC 5. Ejecutar `00_setup/00_configurar_lakehouse` — crea schemas, volumes y tablas de control.
# MAGIC 6. Preparar la muestra de datos con `datos/preparar_muestra_yelp.py` (se corre
# MAGIC    **en tu laptop**, no en Databricks) y subir los archivos resultantes al Volume.
# MAGIC 7. Ejecutar `00_setup/01_verificar_datos_raw` para validar que todo está en su lugar.
# MAGIC
# MAGIC ## 2. Convenciones del Lakehouse (obligatorias — se evalúan)
# MAGIC
# MAGIC | Elemento | Convención |
# MAGIC |---|---|
# MAGIC | Catálogo | `workspace` (el default de Free Edition) |
# MAGIC | Schemas | `yelp_bronze`, `yelp_silver`, `yelp_gold`, `yelp_gov` |
# MAGIC | Volume de datos crudos | `/Volumes/workspace/yelp_bronze/raw/` |
# MAGIC | Volume de artefactos de gobierno | `/Volumes/workspace/yelp_gov/artifacts/` |
# MAGIC | Tablas bronze | `brz_<entidad>` (ej. `brz_business`) |
# MAGIC | Tablas silver | `slv_<entidad>` |
# MAGIC | Tablas gold | `gld_dim_<entidad>`, `gld_fact_<proceso>`, `gld_bridge_<relación>`, `gld_agg_<tema>_<grano>` (ej. `gld_agg_business_month`) |
# MAGIC | Vistas | `vw_<nombre>` |
# MAGIC | Reglas de calidad | `DQ-nnn`, correlativas y únicas |
# MAGIC | Nombres de tablas y columnas | Inglés en todas las capas (incluido `yelp_gov` y las columnas de auditoría `_source_file`, `_ingested_at`) |
# MAGIC | Idioma de comentarios/metadata | Español |
# MAGIC | Ramas Git | `main` protegida; trabajar en `feature/E<k>-<tema>` y merge por PR |
# MAGIC
# MAGIC ## 3. Mapa entregable ↔ carpeta ↔ módulo
# MAGIC
# MAGIC Los entregables **E1, E2 y las definiciones conceptuales de E3 y E6** no viven aquí:
# MAGIC son secciones del Documento Formal. Lo que sigue es lo que se despliega en la plataforma.
# MAGIC
# MAGIC | Entregable ejecutable | Carpeta | Módulo que lo cierra | Sección del Documento Formal que implementa |
# MAGIC |---|---|---|---|
# MAGIC | E3. Modelo físico (DDL sobre Delta / UC) | `E3_modelo_fisico` | M4 Modelado Avanzado | Modelos conceptual y lógico |
# MAGIC | E4. Glosario y Diccionario operativos | `E4_diccionario_glosario` | M3 MDM y Metadata | Anexos de glosario y diccionario |
# MAGIC | E5. Motor de reglas de calidad | `E5_calidad_datos` | M2/M3 (definición) · M6 (ejecución) | Reglas de calidad en lenguaje de negocio |
# MAGIC | E6. Controles técnicos de gobierno | `E6_controles_gobierno` | M2 (diseño) · M5 (implementación) | Estrategia de gobierno, roles y RACI |
# MAGIC | E7. Pipeline de ingesta y habilitación | `E7_pipeline_lakehouse` | M6 Big Data + M7 Integración | Arquitectura de referencia (ABB/SBB) |
# MAGIC | E8. Dashboard BI + Genie | `E8_dashboard_bi` | M9 BI y Visualización | Requerimientos de información y KPIs |
# MAGIC | EX. Bonus: IA sobre reseñas | `EX_bonus_ia` | M8 IA y No Estructurados | Casos de uso analíticos avanzados |
# MAGIC | Operación (job, monitoreo, FinOps) | `E7.../04_job_orquestacion` | M10 DataOps/MLOps/FinOps | Modelo operativo de la plataforma |
# MAGIC
# MAGIC ## 3.1 Artefactos institucionales de gobierno (E4)
# MAGIC
# MAGIC El glosario y el diccionario **no se redactan en formato libre**: se completan en las
# MAGIC plantillas oficiales del bloque de Seguridad, Gobernanza y Compliance, disponibles en
# MAGIC `E4_diccionario_glosario/plantillas/`:
# MAGIC
# MAGIC | Plantilla | Hojas | Se carga en |
# MAGIC |---|---|---|
# MAGIC | `Glosario_de_terminos_de_negocio_YelpConnect_PLANTILLA.xlsx` | Definiciones · Glosario · Guía Capstone | `workspace.yelp_gov.business_glossary` |
# MAGIC | `Diccionario_de_Datos_YelpConnect_PLANTILLA.xlsx` | Definición · Diccionario · Diccionario resumido · Guía Capstone | `workspace.yelp_gov.data_dictionary` |
# MAGIC
# MAGIC Las tablas Delta tienen **las mismas columnas que las plantillas, en el mismo orden** (12 y 25
# MAGIC respectivamente, con nombres en inglés), así que el Excel no se reescribe: se carga por posición. Flujo:
# MAGIC
# MAGIC 1. El grupo completa el Excel (interfaz de negocio, se discute con las "áreas" del caso).
# MAGIC 2. Sube el archivo a `/Volumes/workspace/yelp_gov/artifacts/` sin cambiarle el nombre (los notebooks también lo copian desde `plantillas/`).
# MAGIC 3. Ejecuta los notebooks `E4.../01_glosario_negocio` y `E4.../02_diccionario_datos`.
# MAGIC 4. Los validadores comprueban cobertura, ownership, clasificación y que toda regla citada
# MAGIC    como `DQ-nnn` exista realmente en `yelp_gov.dq_rules`.
# MAGIC
# MAGIC Los dos Excel completados se **anexan además al Documento Formal**: son el mismo archivo,
# MAGIC no dos versiones distintas. El diccionario cierra el círculo cuando su columna de
# MAGIC clasificación se aplica como **tag real** en Unity Catalog y alimenta los permisos de E6.
# MAGIC
# MAGIC ## 4. Reglas de juego en Free Edition
# MAGIC
# MAGIC - Solo hay **cómputo serverless** con **cuota diaria**. Si la agotan, el cómputo se
# MAGIC   apaga hasta el día siguiente (los datos NO se pierden). Moraleja FinOps: trabajen
# MAGIC   con la **muestra**, no con el dataset completo, y eviten `display()` de millones de filas.
# MAGIC - El dataset oficial es el **Yelp Open Dataset** muestreado a UNA ciudad
# MAGIC   (el script de muestreo lo hace por ustedes). Objetivo: < 300 MB en crudo.
# MAGIC - Todo entregable debe ser **reproducible**: si el profesor clona el repo y ejecuta
# MAGIC   los notebooks en orden, obtiene el mismo resultado.
# MAGIC
# MAGIC ## 5. Definition of Done global
# MAGIC
# MAGIC Un entregable de plataforma está terminado cuando:
# MAGIC
# MAGIC 1. El notebook corre de inicio a fin sin errores y sus validadores pasan.
# MAGIC 2. Las tablas y objetos existen en el schema correcto, con comentarios y tags.
# MAGIC 3. La cabecera declara qué sección del Documento Formal implementa, y **no contradice** esa
# MAGIC    sección (si la implementación obligó a cambiar el diseño, se actualiza el documento y se
# MAGIC    deja constancia de la decisión).
# MAGIC 4. El markdown del notebook documenta decisiones y supuestos propios de la implementación.
# MAGIC 5. Está en `main` vía Pull Request revisado por al menos otro integrante.
# MAGIC
# MAGIC ## 6. Orden de ejecución paso a paso (Secuencia obligatoria)
# MAGIC
# MAGIC Para que un entorno limpio se despliegue de principio a fin sin errores de dependencias, los scripts y notebooks deben ejecutarse estrictamente en el siguiente **orden secuencial (1 → 14)**:
# MAGIC
# MAGIC | Paso | Fase | Archivo / Notebook | Dónde se ejecuta | Qué hace / Dependencia que resuelve |
# MAGIC | :---: | :--- | :--- | :---: | :--- |
# MAGIC | **1** | **0. Setup e Insumos** | `00_setup/00_configurar_lakehouse.py` | Databricks | Crea los schemas (`yelp_bronze`, `yelp_silver`, `yelp_gold`, `yelp_gov`), el Volume `/Volumes/workspace/yelp_bronze/raw/` y las tablas base de gobierno. |
# MAGIC | **2** | **0. Setup e Insumos** | `datos/preparar_muestra_yelp.py` | **Tu Laptop (Local)** | Filtra el Yelp Open Dataset para `Philadelphia`, genera los 5 archivos `.json.gz` en `datos/muestra/` y se suben a `/Volumes/workspace/yelp_bronze/raw/`. |
# MAGIC | **3** | **0. Setup e Insumos** | `00_setup/01_verificar_datos_raw.py` | Databricks | Verifica que los 5 archivos `.json.gz` estén en el Volume y perfila sus esquemas antes de ingestar. |
# MAGIC | **4** | **1. Bronze y DDL** | `E7_pipeline_lakehouse/01_bronze_ingesta.py` | Databricks | Ingesta batch (`brz_business`, `brz_user`, `brz_tip`, `brz_checkin`) e incremental con Auto Loader (`brz_review`) hacia `workspace.yelp_bronze`, con columnas de auditoría `_source_file` e `_ingested_at`. Registra cada carga en `yelp_gov.pipeline_run_log`. |
# MAGIC | **5** | **1. Bronze y DDL** | `E3_modelo_fisico/01_modelo_fisico_ddl.py` | Databricks | Crea el DDL físico de las 10 tablas Silver y las 16 tablas Gold según `yelp_silver.txt` / `yelp_gold.txt`, con comentarios, PK/FK, constraints `CHECK` y Change Data Feed. Se ejecuta con `%run` desde el paso 6. |
# MAGIC | **6** | **2. Silver, DQ y Metadata** | `E7_pipeline_lakehouse/02_silver_limpieza.py` | Databricks | Limpia desde Bronze y puebla las 10 tablas de `workspace.yelp_silver`: golden record de negocios, categorías normalizadas, amistades, check-ins a grano evento y cuarentena de reseñas en `slv_review_quarantine`. Termina con el Quality Gate de E5. |
# MAGIC | **7** | **2. Silver, DQ y Metadata** | `E5_calidad_datos/01_reglas_calidad.py` | Databricks | Registra las 15 reglas (`DQ-001` a `DQ-015`) en `yelp_gov.dq_rules` y guarda cada evaluación en `yelp_gov.dq_results`. Se ejecuta con `%run` desde el paso 6; las reglas `critical` detienen el pipeline. |
# MAGIC | **8** | **2. Silver, DQ y Metadata** | `E4_diccionario_glosario/01_glosario_negocio.py` | Databricks | Sincroniza la plantilla Excel de Glosario al Volume `/Volumes/workspace/yelp_gov/artifacts/` y puebla `yelp_gov.business_glossary`. |
# MAGIC | **9** | **2. Silver, DQ y Metadata** | `E4_diccionario_glosario/02_diccionario_datos.py` | Databricks | Sincroniza la plantilla Excel de Diccionario, cruza contra `information_schema` (E3) y `dq_rules` (E5), puebla `yelp_gov.data_dictionary` y aplica el tag `classification` por columna. |
# MAGIC | **10** | **3. Gold y Gobierno UC** | `E7_pipeline_lakehouse/03_gold_productos_datos.py` | Databricks | Puebla el modelo dimensional de `workspace.yelp_gold`: 4 dimensiones (`gld_dim_*`), el puente `gld_bridge_business_category`, 3 hechos (`gld_fact_review`, `gld_fact_checkin`, `gld_fact_tip`) y 8 agregados (`gld_agg_*`). |
# MAGIC | **11** | **3. Gold y Gobierno UC** | `E6_controles_gobierno/01_controles_unity_catalog.py` | Databricks | Registra `yelp_gov.raci_matrix`, aplica `SET TAGS`, permisos `GRANT`, máscara PII (`mask_name` en `slv_user.name` y `gld_dim_user.name`) y vista `vw_user_secure`. |
# MAGIC | **12** | **4. BI, DataOps e IA** | `E8_dashboard_bi/01_consultas_base_dashboard.py` | Databricks | Ejecuta y valida las consultas analíticas y de gobierno sobre `yelp_gold` y `yelp_gov` que alimentan el Dashboard BI y Genie. |
# MAGIC | **13** | **4. BI, DataOps e IA** | `E7_pipeline_lakehouse/04_job_orquestacion.py` | Databricks | Crea/actualiza el Lakeflow Job diario encadenado y genera el monitoreo de observabilidad y FinOps desde `yelp_gov.pipeline_run_log`. |
# MAGIC | **14** | **4. BI, DataOps e IA** | `EX_bonus_ia/01_analisis_ia_resenas.py` | Databricks *(Bonus)* | Enriquece con IA una muestra de 500 reseñas válidas (sentimiento, tópico y consistencia con las estrellas) en `gld_review_ai_enriched`. |
# MAGIC
# MAGIC ### Resumen visual del flujo lineal
# MAGIC ```text
# MAGIC [Paso 1]  00_setup/00_configurar_lakehouse.py          (Crea schemas, volumes y tablas de control)
# MAGIC    ↓
# MAGIC [Paso 2]  datos/preparar_muestra_yelp.py               (Local: genera *.json.gz y sube a /Volumes/.../raw/)
# MAGIC    ↓
# MAGIC [Paso 3]  00_setup/01_verificar_datos_raw.py           (Valida los 5 archivos .json.gz en el Volume)
# MAGIC    ↓
# MAGIC [Paso 4]  E7_pipeline_lakehouse/01_bronze_ingesta.py   (Carga brz_business, brz_user, brz_tip, brz_checkin, brz_review)
# MAGIC    ↓
# MAGIC [Paso 5]  E3_modelo_fisico/01_modelo_fisico_ddl.py     (DDL de 10 tablas Silver y 16 Gold con comments/PK/FK)
# MAGIC    ↓
# MAGIC [Paso 6]  E7_pipeline_lakehouse/02_silver_limpieza.py  (Puebla las 10 tablas slv_*, incluida la cuarentena)
# MAGIC    ↓
# MAGIC [Paso 7]  E5_calidad_datos/01_reglas_calidad.py        (Registra DQ-001..DQ-015 en dq_rules y evalúa en dq_results)
# MAGIC    ↓
# MAGIC [Paso 8]  E4_diccionario_glosario/01_glosario_negocio.py   (Carga Excel de Glosario → yelp_gov.business_glossary)
# MAGIC    ↓
# MAGIC [Paso 9]  E4_diccionario_glosario/02_diccionario_datos.py  (Carga Excel de Diccionario → yelp_gov.data_dictionary)
# MAGIC    ↓
# MAGIC [Paso 10] E7_pipeline_lakehouse/03_gold_productos_datos.py (Puebla dimensiones, hechos y KPIs en workspace.yelp_gold)
# MAGIC    ↓
# MAGIC [Paso 11] E6_controles_gobierno/01_controles_unity_catalog.py (Matriz RACI, Tags UC, RBAC, Column Mask PII y vista segura)
# MAGIC    ↓
# MAGIC [Paso 12] E8_dashboard_bi/01_consultas_base_dashboard.py   (Consultas base de KPIs para Dashboard Lakeview y Genie)
# MAGIC    ↓
# MAGIC [Paso 13] E7_pipeline_lakehouse/04_job_orquestacion.py     (Despliega Lakeflow Job automatizado y reporte FinOps)
# MAGIC    ↓
# MAGIC [Paso 14] EX_bonus_ia/01_analisis_ia_resenas.py            (Bonus: Análisis de sentimiento y extracción con IA)
# MAGIC ```
# MAGIC
# MAGIC ## 7. Job automatizado y estado actual
# MAGIC
# MAGIC El job **`Pipeline_Medallon_YelpConnect_Grupo03`** (Lakeflow Jobs, diario a las 01:00 UTC) ejecuta cuatro tareas en cadena:
# MAGIC
# MAGIC | Tarea | Notebook | Incluye |
# MAGIC | :--- | :--- | :--- |
# MAGIC | `01_bronze_ingesta` | `E7_pipeline_lakehouse/01_bronze_ingesta` | Bronze |
# MAGIC | `02_silver_limpieza` | `E7_pipeline_lakehouse/02_silver_limpieza` | E3 (`%run`), Silver y Quality Gate E5 (`%run`) |
# MAGIC | `03_gold_productos_datos` | `E7_pipeline_lakehouse/03_gold_productos_datos` | Gold |
# MAGIC | `04_controles_gobierno` | `E6_controles_gobierno/01_controles_unity_catalog` | RACI, tags, permisos, máscaras PII y vista segura |
# MAGIC
# MAGIC E4 (glosario y diccionario), E8 (consultas del dashboard) y EX (IA) se ejecutan aparte, después del job.
# MAGIC
# MAGIC El detalle de todas las tablas y columnas, generado desde Unity Catalog, está en
# MAGIC [`documentos/03_modelo_logico_y_dimensional_gold.md`](documentos/03_modelo_logico_y_dimensional_gold.md).
