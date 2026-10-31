# Capstone Project — Plataforma de Datos YelpConnect
### Programa de Arquitectura de Datos · UTEC

Este repositorio es el **esqueleto base** que cada grupo usará para desplegar los
entregables **ejecutables** del Capstone en **Databricks Free Edition**.

> **Importante — dónde vive cada entregable.**
> El Capstone tiene **dos soportes complementarios**:
>
> | Soporte | Qué contiene | Naturaleza |
> |---|---|---|
> | **Documento Formal de Arquitectura de Datos** | Visión y SoAW, arquitectura de referencia (ABB/SBB), modelos conceptual y lógico, estrategia de gobierno y matriz RACI, definición de reglas en lenguaje de negocio, requerimientos de información | Diseño, definiciones y conceptual |
> | **Este repositorio (Databricks)** | Modelo físico, glosario y diccionario operativos, motor de calidad, controles técnicos, pipeline, dashboard | Implementación y evidencia ejecutable |
>
> Regla de oro: **el documento define QUÉ y POR QUÉ; la plataforma demuestra QUE SE CUMPLE.**
> Ningún notebook rehace el diseño: cada uno declara en su cabecera qué sección del
> documento formal implementa (bloque **Trazabilidad**).

---

## 1. Setup inicial del grupo (Semana 0 / Onboarding)

1. **Un integrante crea la cuenta del grupo** en [Databricks Free Edition](https://www.databricks.com/learn/free-edition)
   (correo del grupo, no personal). Free Edition permite **un solo workspace y un
   metastore por cuenta**, así que la cuenta es del equipo, no de una persona.
2. Invitar a los 5 integrantes como usuarios del workspace
   (`Settings → Identity and access → Users`).
3. Crear un repositorio **fork/clon de este esqueleto** en GitHub
   (nombre sugerido: `capstone-grupoNN-yelpconnect`).
4. En Databricks: `Workspace → Create → Git folder` y conectar el repo del grupo.
5. Ejecutar `00_setup/00_configurar_lakehouse` — crea schemas, volumes y tablas de control.
6. Preparar la muestra de datos con `datos/preparar_muestra_yelp.py` (se corre
   **en tu laptop**, no en Databricks) y subir los archivos resultantes al Volume.
7. Ejecutar `00_setup/01_verificar_datos_raw` para validar que todo está en su lugar.

## 2. Convenciones del Lakehouse (obligatorias — se evalúan)

| Elemento | Convención |
|---|---|
| Catálogo | `workspace` (el default de Free Edition) |
| Schemas | `yelp_bronze`, `yelp_silver`, `yelp_gold`, `yelp_gov` |
| Volume de datos crudos | `/Volumes/workspace/yelp_bronze/raw/` |
| Volume de artefactos de gobierno | `/Volumes/workspace/yelp_gov/artefactos/` |
| Tablas bronze | `brz_<entidad>` (ej. `brz_business`) |
| Tablas silver | `slv_<entidad>` |
| Tablas gold | `gld_<producto_de_datos>` (ej. `gld_kpi_resenas_negocio`) |
| Vistas | `vw_<nombre>` |
| Reglas de calidad | `DQ-nnn`, correlativas y únicas |
| Idioma de comentarios/metadata | Español (o inglés, pero consistente) |
| Ramas Git | `main` protegida; trabajar en `feature/E<k>-<tema>` y merge por PR |

## 3. Mapa entregable ↔ carpeta ↔ módulo

Los entregables **E1, E2 y las definiciones conceptuales de E3 y E6** no viven aquí:
son secciones del Documento Formal. Lo que sigue es lo que se despliega en la plataforma.

| Entregable ejecutable | Carpeta | Módulo que lo cierra | Sección del Documento Formal que implementa |
|---|---|---|---|
| E3. Modelo físico (DDL sobre Delta / UC) | `E3_modelo_fisico` | M4 Modelado Avanzado | Modelos conceptual y lógico |
| E4. Glosario y Diccionario operativos | `E4_diccionario_glosario` | M3 MDM y Metadata | Anexos de glosario y diccionario |
| E5. Motor de reglas de calidad | `E5_calidad_datos` | M2/M3 (definición) · M6 (ejecución) | Reglas de calidad en lenguaje de negocio |
| E6. Controles técnicos de gobierno | `E6_controles_gobierno` | M2 (diseño) · M5 (implementación) | Estrategia de gobierno, roles y RACI |
| E7. Pipeline de ingesta y habilitación | `E7_pipeline_lakehouse` | M6 Big Data + M7 Integración | Arquitectura de referencia (ABB/SBB) |
| E8. Dashboard BI + Genie | `E8_dashboard_bi` | M9 BI y Visualización | Requerimientos de información y KPIs |
| EX. Bonus: IA sobre reseñas | `EX_bonus_ia` | M8 IA y No Estructurados | Casos de uso analíticos avanzados |
| Operación (job, monitoreo, FinOps) | `E7.../04_job_orquestacion` | M10 DataOps/MLOps/FinOps | Modelo operativo de la plataforma |

## 3.1 Artefactos institucionales de gobierno (E4)

El glosario y el diccionario **no se redactan en formato libre**: se completan en las
plantillas oficiales del bloque de Seguridad, Gobernanza y Compliance, disponibles en
`E4_diccionario_glosario/plantillas/`:

| Plantilla | Hojas | Se carga en |
|---|---|---|
| `Glosario_de_terminos_de_negocio_YelpConnect_PLANTILLA.xlsx` | Definiciones · Glosario · Guía Capstone | `workspace.yelp_gov.glosario_negocio` |
| `Diccionario_de_Datos_YelpConnect_PLANTILLA.xlsx` | Definición · Diccionario · Diccionario resumido · Guía Capstone | `workspace.yelp_gov.diccionario_datos` |

Las tablas Delta tienen **exactamente las mismas columnas que las plantillas** (12 y 25
respectivamente), así que el Excel no se traduce: se carga. Flujo:

1. El grupo completa el Excel (interfaz de negocio, se discute con las "áreas" del caso).
2. Sube el archivo a `/Volumes/workspace/yelp_gov/artefactos/` sin cambiarle el nombre.
3. Ejecuta los notebooks `E4.../01_glosario_negocio` y `E4.../02_diccionario_datos`.
4. Los validadores comprueban cobertura, ownership, clasificación y que toda regla citada
   como `DQ-nnn` exista realmente en `yelp_gov.dq_reglas`.

Los dos Excel completados se **anexan además al Documento Formal**: son el mismo archivo,
no dos versiones distintas. El diccionario cierra el círculo cuando su columna de
clasificación se aplica como **tag real** en Unity Catalog y alimenta los permisos de E6.

## 4. Reglas de juego en Free Edition

- Solo hay **cómputo serverless** con **cuota diaria**. Si la agotan, el cómputo se
  apaga hasta el día siguiente (los datos NO se pierden). Moraleja FinOps: trabajen
  con la **muestra**, no con el dataset completo, y eviten `display()` de millones de filas.
- El dataset oficial es el **Yelp Open Dataset** muestreado a UNA ciudad
  (el script de muestreo lo hace por ustedes). Objetivo: < 300 MB en crudo.
- Todo entregable debe ser **reproducible**: si el profesor clona el repo y ejecuta
  los notebooks en orden, obtiene el mismo resultado.

## 5. Definition of Done global

Un entregable de plataforma está terminado cuando:

1. El notebook corre de inicio a fin sin errores y sus validadores pasan.
2. Las tablas y objetos existen en el schema correcto, con comentarios y tags.
3. La cabecera declara qué sección del Documento Formal implementa, y **no contradice** esa
   sección (si la implementación obligó a cambiar el diseño, se actualiza el documento y se
   deja constancia de la decisión).
4. El markdown del notebook documenta decisiones y supuestos propios de la implementación.
5. Está en `main` vía Pull Request revisado por al menos otro integrante.

## 6. Orden de ejecución paso a paso (Secuencia obligatoria)

Para que un entorno limpio se despliegue de principio a fin sin errores de dependencias, los scripts y notebooks deben ejecutarse estrictamente en el siguiente **orden secuencial (1 → 14)**:

| Paso | Fase | Archivo / Notebook | Dónde se ejecuta | Qué hace / Dependencia que resuelve |
| :---: | :--- | :--- | :---: | :--- |
| **1** | **0. Setup e Insumos** | `00_setup/00_configurar_lakehouse.py` | Databricks | Crea los schemas (`yelp_bronze`, `yelp_silver`, `yelp_gold`, `yelp_gov`), el Volume `/Volumes/workspace/yelp_bronze/raw/` y las tablas base de gobierno. |
| **2** | **0. Setup e Insumos** | `datos/preparar_muestra_yelp.py` | **Tu Laptop (Local)** | Filtra el Yelp Open Dataset para `Philadelphia`, genera los 5 archivos `.json.gz` en `datos/muestra/` y se suben a `/Volumes/workspace/yelp_bronze/raw/`. |
| **3** | **0. Setup e Insumos** | `00_setup/01_verificar_datos_raw.py` | Databricks | Verifica que los 5 archivos `.json.gz` estén en el Volume y perfila sus esquemas antes de ingestar. |
| **4** | **1. Bronze y DDL** | `E7_pipeline_lakehouse/01_bronze_ingesta.py` | Databricks | Ingesta batch (`brz_business`, `brz_user`, `brz_tip`, `brz_checkin`) e incremental con Auto Loader (`brz_review`) hacia `workspace.yelp_bronze`. |
| **5** | **1. Bronze y DDL** | `E3_modelo_fisico/01_modelo_fisico_ddl.py` | Databricks | Crea el DDL físico de las 7 tablas Silver (`yelp_silver`) y las 8 tablas Gold (`yelp_gold`) con comentarios, PK/FK, constraints `CHECK` y Change Data Feed. |
| **6** | **2. Silver, DQ y Metadata** | `E7_pipeline_lakehouse/02_silver_limpieza.py` | Databricks | Transforma y limpia desde Bronze, separa anomalías en `slv_review_cuarentena` y puebla las 7 tablas de `workspace.yelp_silver`. |
| **7** | **2. Silver, DQ y Metadata** | `E5_calidad_datos/01_reglas_calidad.py` | Databricks | Registra las 15 reglas (`DQ-001` a `DQ-015`) en `yelp_gov.dq_reglas` y evalúa su cumplimiento sobre las tablas Silver en `yelp_gov.dq_resultados`. |
| **8** | **2. Silver, DQ y Metadata** | `E4_diccionario_glosario/01_glosario_negocio.py` | Databricks | Sincroniza la plantilla Excel de Glosario al Volume `/Volumes/workspace/yelp_gov/artefactos/` y puebla `yelp_gov.glosario_negocio`. |
| **9** | **2. Silver, DQ y Metadata** | `E4_diccionario_glosario/02_diccionario_datos.py` | Databricks | Sincroniza la plantilla Excel de Diccionario, cruza contra `information_schema` (E3) y `dq_reglas` (E5), puebla `yelp_gov.diccionario_datos` y aplica tags. |
| **10** | **3. Gold y Gobierno UC** | `E7_pipeline_lakehouse/03_gold_productos_datos.py` | Databricks | Construye y puebla el esquema estrella Kimball (`gld_dim_*`, `gld_fact_resenas`) y las tablas agregadas de KPIs en `workspace.yelp_gold`. |
| **11** | **3. Gold y Gobierno UC** | `E6_controles_gobierno/01_controles_unity_catalog.py` | Databricks | Registra `yelp_gov.matriz_raci`, aplica `SET TAGS`, permisos `GRANT`/`REVOKE`, máscara PII (`mascara_nombre` en `slv_user.name`) y vista `vw_usuarios_seguro`. |
| **12** | **4. BI, DataOps e IA** | `E8_dashboard_bi/01_consultas_base_dashboard.py` | Databricks | Ejecuta y valida las consultas analíticas y de gobierno sobre `yelp_gold` y `yelp_gov` que alimentan el Dashboard BI y Genie. |
| **13** | **4. BI, DataOps e IA** | `E7_pipeline_lakehouse/04_job_orquestacion.py` | Databricks | Crea/actualiza el Lakeflow Job diario encadenado y genera el monitoreo de observabilidad y FinOps desde `yelp_gov.pipeline_bitacora`. |
| **14** | **4. BI, DataOps e IA** | `EX_bonus_ia/01_analisis_ia_resenas.py` | Databricks *(Bonus)* | Ejecuta el enriquecimiento analítico de sentimiento y tópicos con IA sobre las reseñas certificadas. |

### Resumen visual del flujo lineal
```text
[Paso 1]  00_setup/00_configurar_lakehouse.py          (Crea schemas, volumes y tablas de control)
   ↓
[Paso 2]  datos/preparar_muestra_yelp.py               (Local: genera *.json.gz y sube a /Volumes/.../raw/)
   ↓
[Paso 3]  00_setup/01_verificar_datos_raw.py           (Valida los 5 archivos .json.gz en el Volume)
   ↓
[Paso 4]  E7_pipeline_lakehouse/01_bronze_ingesta.py   (Carga brz_business, brz_user, brz_tip, brz_checkin, brz_review)
   ↓
[Paso 5]  E3_modelo_fisico/01_modelo_fisico_ddl.py     (Crea DDL de tablas Silver 3FN y Gold Kimball con comments/PK/FK)
   ↓
[Paso 6]  E7_pipeline_lakehouse/02_silver_limpieza.py  (Limpia, deduplica y puebla las 7 tablas slv_* + cuarentena)
   ↓
[Paso 7]  E5_calidad_datos/01_reglas_calidad.py        (Registra DQ-001..DQ-015 en dq_reglas y evalúa dq_resultados)
   ↓
[Paso 8]  E4_diccionario_glosario/01_glosario_negocio.py   (Carga Excel de Glosario → yelp_gov.glosario_negocio)
   ↓
[Paso 9]  E4_diccionario_glosario/02_diccionario_datos.py  (Carga Excel de Diccionario + valida contra DDL E3 y reglas E5)
   ↓
[Paso 10] E7_pipeline_lakehouse/03_gold_productos_datos.py (Puebla dimensiones, hechos y KPIs en workspace.yelp_gold)
   ↓
[Paso 11] E6_controles_gobierno/01_controles_unity_catalog.py (Matriz RACI, Tags UC, RBAC, Column Mask PII y vista segura)
   ↓
[Paso 12] E8_dashboard_bi/01_consultas_base_dashboard.py   (Consultas base de KPIs para Dashboard Lakeview y Genie)
   ↓
[Paso 13] E7_pipeline_lakehouse/04_job_orquestacion.py     (Despliega Lakeflow Job automatizado y reporte FinOps)
   ↓
[Paso 14] EX_bonus_ia/01_analisis_ia_resenas.py            (Bonus: Análisis de sentimiento y extracción con IA)
```
