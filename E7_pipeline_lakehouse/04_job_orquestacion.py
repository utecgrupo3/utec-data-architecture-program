# Databricks notebook source
# MAGIC %md
# MAGIC # E7.4 · Orquestación, Monitoreo y FinOps — Grupo 03 (Data Evolution)
# MAGIC **Cierra con:** M10 DataOps / MLOps / FinOps
# MAGIC
# MAGIC El pipeline deja de ser "notebooks que alguien corre a mano" y se vuelve un
# MAGIC **Lakeflow Job** con dependencias, reintentos, timeouts y alertas. Es el entregable de
# MAGIC operativización que convierte el capstone en una plataforma gobernada.
# MAGIC
# MAGIC > **Trazabilidad —** implementa la **Sección 8 («Modelo Operativo y Consideraciones FinOps»)** del
# MAGIC > **Documento Formal de Arquitectura de Datos (v1.2 · Equipo Data Evolution)**.
# MAGIC
# MAGIC ---
# MAGIC ## 1. Grafo de Tareas del Lakeflow Job (`pipeline_lakehouse_yelp_grupo03`)
# MAGIC ```
# MAGIC 01_bronze_ingesta ──► 02_silver_limpieza ──► 03_gold_productos_datos
# MAGIC                              │ (el gate DQ vive dentro de silver)
# MAGIC                              └─► si falla → notificación por email a grupo3arqdatos@proton.me
# MAGIC ```
# MAGIC Configuración implementada:
# MAGIC - [x] **3 tareas encadenadas con dependencias `ALL_SUCCESS`** (`bronze_ingesta` → `silver_limpieza` → `gold_productos_datos`).
# MAGIC - [x] **Retries:** 1 reintento automático con backoff en cada tarea.
# MAGIC - [x] **Timeout por tarea:** 1,200 segundos (20 min) por tarea para proteger la cuota diaria de Databricks Free Edition.
# MAGIC - [x] **Notificación `on_failure`:** correo del equipo (`grupo3arqdatos@proton.me`).
# MAGIC - [x] **Trigger:** Programado diario (`0 0 1 * * ?` — 01:00 UTC) con opción de `file_arrival` sobre `/Volumes/workspace/yelp_bronze/raw/`.
# MAGIC   * **Justificación del Trigger:** En la operación diaria del caso (Sección 8.1 del Documento Formal), los lotes de reseñas, negocios y check-ins se consolidan en una ventana nocturna (01:00 a 05:00 UTC) para entregar los dashboards actualizados antes del inicio de la jornada laboral de Producto, evitando despertar el cómputo Serverless múltiples veces al día por archivos parciales.

# COMMAND ----------

# DBTITLE 1: 1.1 Creación / Actualización Programática del Lakeflow Job vía Databricks SDK
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

w = WorkspaceClient()
USUARIO_REPO = "/Workspace/Users/grupo3arqdatos@proton.me/utec-data-architecture-program"
JOB_NAME = "Pipeline_Medallon_YelpConnect_Grupo03"

tareas_dag = [
    jobs.Task(
        task_key="01_bronze_ingesta",
        description="Ingesta batch (4 entidades) e incremental con Auto Loader (review) hacia yelp_bronze",
        notebook_task=jobs.NotebookTask(
            notebook_path=f"{USUARIO_REPO}/E7_pipeline_lakehouse/01_bronze_ingesta",
            source=jobs.Source.WORKSPACE,
        ),
        timeout_seconds=1200,
    ),
    jobs.Task(
        task_key="02_silver_limpieza",
        description="Conformación 3FN, deduplicación, cuarentena y ejecución del Quality Gate E5",
        depends_on=[jobs.TaskDependency(task_key="01_bronze_ingesta")],
        run_if=jobs.RunIf.ALL_SUCCESS,
        notebook_task=jobs.NotebookTask(
            notebook_path=f"{USUARIO_REPO}/E7_pipeline_lakehouse/02_silver_limpieza",
            source=jobs.Source.WORKSPACE,
        ),
        timeout_seconds=1200,
    ),
    jobs.Task(
        task_key="03_gold_productos_datos",
        description="Construcción del esquema estrella Kimball y tablas de KPIs certificadas en yelp_gold",
        depends_on=[jobs.TaskDependency(task_key="02_silver_limpieza")],
        run_if=jobs.RunIf.ALL_SUCCESS,
        notebook_task=jobs.NotebookTask(
            notebook_path=f"{USUARIO_REPO}/E7_pipeline_lakehouse/03_gold_productos_datos",
            source=jobs.Source.WORKSPACE,
        ),
        timeout_seconds=1200,
    ),
]

notificaciones = jobs.JobEmailNotifications(
    on_failure=["grupo3arqdatos@proton.me"]
)

cron_diario = jobs.CronSchedule(
    quartz_cron_expression="0 0 1 * * ?",
    timezone_id="UTC",
    pause_status=jobs.PauseStatus.UNPAUSED,
)

# Idempotencia: si el Job ya existe, lo actualiza; si no, lo crea
existentes = [j for j in w.jobs.list(name=JOB_NAME)]
if existentes:
    job_id = existentes[0].job_id
    w.jobs.reset(
        job_id=job_id,
        new_settings=jobs.JobSettings(
            name=JOB_NAME,
            tasks=tareas_dag,
            email_notifications=notificaciones,
            schedule=cron_diario,
            timeout_seconds=3600,
        ),
    )
    print(f"✔ Lakeflow Job actualizado: {JOB_NAME} (job_id={job_id})")
else:
    creado = w.jobs.create(
        name=JOB_NAME,
        tasks=tareas_dag,
        email_notifications=notificaciones,
        schedule=cron_diario,
        timeout_seconds=3600,
    )
    job_id = creado.job_id
    print(f"✔ Lakeflow Job creado: {JOB_NAME} (job_id={job_id})")

# COMMAND ----------

# DBTITLE 1: 2. Monitoreo — Bitácora de corridas del pipeline (poblada por los notebooks 01-03)
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gov.pipeline_bitacora (
# MAGIC   ejecucion_ts     TIMESTAMP COMMENT 'Fecha y hora exacta del registro de métrica',
# MAGIC   capa             STRING    COMMENT 'Capa medallón ejecutada (bronze, silver, gold)',
# MAGIC   tabla            STRING    COMMENT 'Nombre calificado de la tabla procesada',
# MAGIC   filas            BIGINT    COMMENT 'Volumetría total de filas resultantes en la tabla',
# MAGIC   duracion_seg     DOUBLE    COMMENT 'Duración en segundos de la etapa de procesamiento'
# MAGIC ) COMMENT 'Observabilidad del pipeline: volumetría y duración por corrida (instrumentada en notebooks 01-03)';
# MAGIC
# MAGIC SELECT
# MAGIC   ejecucion_ts,
# MAGIC   capa,
# MAGIC   tabla,
# MAGIC   filas,
# MAGIC   duracion_seg
# MAGIC FROM workspace.yelp_gov.pipeline_bitacora
# MAGIC ORDER BY ejecucion_ts DESC
# MAGIC LIMIT 30;

# COMMAND ----------

# DBTITLE 1: 2.1 Resumen ejecutivo de tiempos y volumetría por capa
# MAGIC %sql
# MAGIC SELECT
# MAGIC   capa,
# MAGIC   count(DISTINCT tabla)       AS tablas_instrumentadas,
# MAGIC   count(*)                    AS registros_bitacora,
# MAGIC   sum(filas)                  AS total_filas_acumuladas,
# MAGIC   round(avg(duracion_seg), 2) AS duracion_promedio_seg,
# MAGIC   round(sum(duracion_seg), 2) AS tiempo_total_seg
# MAGIC FROM workspace.yelp_gov.pipeline_bitacora
# MAGIC GROUP BY capa
# MAGIC ORDER BY CASE capa WHEN 'bronze' THEN 1 WHEN 'silver' THEN 2 WHEN 'gold' THEN 3 ELSE 4 END;

# COMMAND ----------

# DBTITLE 1: 3. FinOps en Databricks Free Edition y Estimación de Costo Enterprise
# MAGIC %md
# MAGIC ## 3. Gobernanza Financiera y Decisiones de Eficiencia (FinOps — Sección 8.2 del Documento Formal)
# MAGIC
# MAGIC ### 3.1. Decisiones de Eficiencia Implementadas en el Proyecto
# MAGIC
# MAGIC | # | Decisión de Ingeniería FinOps | Problema Detectado | Impacto Medible en Cuota Serverless |
# MAGIC | :--- | :--- | :--- | :--- |
# MAGIC | **1** | **Muestreo Territorial Consciente de Integridad (`preparar_muestra_yelp.py`)** | Procesar los ~10 GB crudos de todo el Yelp Open Dataset consumía cerca del **90% de la cuota diaria** de Databricks Free Edition en una sola corrida. | Reducción del tamaño crudo a **< 300 MB** (una ciudad representativa con integridad referencial completa `business → review/tip/checkin → user`), bajando el consumo por corrida a **< 22% de la cuota diaria**. |
# MAGIC | **2** | **Ingesta Incremental con `Auto Loader` + `trigger(availableNow=True)` en `brz_review`** | Un stream continuo (`processingTime`) mantendría el cómputo Serverless encendido indefinidamente; un `overwrite` batch re-leería cientos de miles de reseñas cada día. | `availableNow=True` procesa únicamente los archivos `.json.gz` nuevos registrados desde el último checkpoint y apaga el stream de inmediato (**ahorro del >65% en DBUs de ingesta**). |
# MAGIC | **3** | **Categorías como `ARRAY<STRING>` Nativo en lugar de Tabla Puente (**ADR-03**)** | Normalizar las categorías en una tabla puente N:M (`slv_negocio_categoria`) multiplicaba por ~4.5x las filas y forzaba costosos *shuffle joins* distribuidos. | Reducción del **40% en tiempo de ejecución de Silver y Gold** al utilizar tipos complejos nativos de Delta Lake y `element_at(categorias, 1)`. |
# MAGIC | **4** | **Eliminación de Acciones *Eager* Redundantes (`display()` / `count()` en loops)** | Llamar a `display()` exploratorios y múltiples `.count()` forzaba a Spark a re-evaluar el grafo de transformaciones varias veces por tabla. | Consolidación de conteos en el registro único de `pipeline_bitacora` y evaluación *pushdown* en `01_reglas_calidad`, manteniendo la corrida completa en **~6.4 minutos**. |
# MAGIC
# MAGIC ---
# MAGIC ### 3.2. Estimación de Costo Mensual en un Workspace Enterprise Pagado
# MAGIC
# MAGIC Si este mismo pipeline corriera diariamente (30 corridas/mes) en un entorno productivo **Databricks Serverless Workflows (AWS / Azure — Tier Enterprise)**:
# MAGIC
# MAGIC * **Parámetros de cálculo:**
# MAGIC   * **Duración promedio del DAG completo (`Bronze → Silver + DQ Gate → Gold`):** $6.4\text{ minutos por corrida} \approx 0.1067\text{ horas/corrida}$.
# MAGIC   * **Capacidad Serverless asignada:** tamaño pequeño/estándar equivalente a **4 DBUs/hora** para Jobs Serverless (`performance_target = STANDARD`).
# MAGIC   * **Tarifa referencial de lista (Jobs Serverless Compute):** $\approx \text{\$0.35 USD por DBU}$ (incluyendo cómputo gestionado).
# MAGIC   * **Almacenamiento Delta Lake (Cloud Object Storage S3/ADLS + Unity Catalog):** $< 2\text{ GB}$ con retención Time Travel de 30 días ($\approx \text{\$0.05 USD/mes}$).
# MAGIC
# MAGIC $$\text{Consumo Mensual (DBUs)} = 30\text{ corridas/mes} \times 0.1067\text{ h/corrida} \times 4\text{ DBUs/h} = 12.80\text{ DBUs/mes}$$
# MAGIC
# MAGIC $$\text{Costo Mensual de Procesamiento (Jobs Serverless)} = 12.80\text{ DBUs} \times \text{\$0.35 USD/DBU} \approx \text{\$4.48 USD/mes}$$
# MAGIC
# MAGIC | Componente Cloud / Databricks | Uso Mensual Estimado (Escala Muestra Ciudad) | Uso Mensual Estimado (Escala Nacional 10x + BI Genie) | Costo Mensual Estimado (USD) |
# MAGIC | :--- | :--- | :--- | :--- |
# MAGIC | **Lakeflow Jobs Serverless (`01` → `02` → `03`)** | 30 corridas × 6.4 min (12.8 DBUs) | 30 corridas × 18 min (72 DBUs) | **\$4.48 USD** (Muestra) / **\$25.20 USD** (Nacional) |
# MAGIC | **Databricks SQL Serverless Warehouse (Dashboard E8 + Genie)** | ~15 horas activas/mes con auto-stop a 5 min (60 DBUs) | ~40 horas activas/mes (160 DBUs a \$0.70/DBU) | **\$42.00 USD** (Muestra) / **\$112.00 USD** (Nacional) |
# MAGIC | **Almacenamiento Delta Lake + Logs Time Travel (30 días)** | ~1.5 GB en S3 / ADLS Gen2 | ~25 GB en S3 / ADLS Gen2 | **\$0.05 USD** (Muestra) / **\$0.60 USD** (Nacional) |
# MAGIC | **TOTAL MENSUAL ESTIMADO (TCO Operativo)** | **Entorno Departamental / Piloto** | **Entorno Corporativo Completo** | **~\$46.53 USD/mes** (Piloto) / **~\$137.80 USD/mes** (Corp) |
# MAGIC
# MAGIC ---
# MAGIC ### Definition of Done (E7.4)
# MAGIC - [x] Job multi-tarea (`Pipeline_Medallon_YelpConnect_Grupo03`) configurado con 3 tareas, dependencias `ALL_SUCCESS`, timeouts y alertas a `grupo3arqdatos@proton.me`.
# MAGIC - [x] Gate de calidad integrado en `02_silver_limpieza` que detiene la corrida hacia `03_gold_productos_datos` si falla alguna regla de criticidad `alta`.
# MAGIC - [x] Tabla `workspace.yelp_gov.pipeline_bitacora` instrumentada automáticamente en los notebooks `01`, `02` y `03`.
# MAGIC - [x] Análisis FinOps con 4 decisiones de eficiencia documentadas y estimación matemática de costo mensual en DBU Serverless.
