# Documento Formal de Arquitectura de Datos · Caso YelpConnect
**Programa en Arquitectura de Datos · UTEC Posgrado**
**Equipo:** Data Evolution | **Caso Prioritario:** Caso de Uso de Producto (Andrew Collins, VP of Product)

---

## Ficha Técnica del Documento

| Campo | Contenido |
| :--- | :--- |
| **Título del Proyecto** | Arquitectura de Datos Integral y Gobierno de la Plataforma YelpConnect |
| **Caso de Uso Prioritario** | **Caso de Uso de Producto (Andrew Collins, VP of Product):** Calidad de la Experiencia de Usuario, Calificación Ponderada Confiable y Métricas de Engagement |
| **Organización / Programa** | Posgrado Universidad de Ingeniería y Tecnología (UTEC) — Escuela de Posgrado |
| **Equipo de Trabajo** | **Data Evolution** |
| **Integrantes y Roles** | • **Jamir Huaman:** Arquitecto/a de Datos (Coordinador General)<br>• **John Huerta:** Modelador/a de Datos (Relator Módulo 5 · Arquitectura Cloud)<br>• **Leonardo Pacheco:** Steward de Gobierno (Relator Módulo 1)<br>• **Gabriela Matto:** Relator/a Módulo 2 · Gestión y Coordinación<br>• **Josuhe Sosa:** Ingeniero/a de Datos (Relator Módulo 4)<br>• **Martin Valencia:** Analista de BI y Valor (Relator Módulos 6 a 10) |
| **Plataforma Tecnológica** | Databricks Runtime 14.3 LTS Serverless · Delta Lake 3.2 · Unity Catalog (`workspace`) · PySpark |
| **Marcos de Referencia** | TOGAF Standard 10 (ADM) · Kimball Dimensional Modeling · DAMA-DMBOK 2 · Lakehouse Medallion |
| **Fecha de Consolidación** | Octubre 2026 |
| **Versión del Documento** | **v1.2 (Versión Integrada con Modelado Hackolade y Grafo Social)** |
| **Estado** | Aprobado para Sustentación Formal con Mención de Excelencia |

---

## Control de Versiones

| Versión | Fecha | Módulo | Cambio realizado y motivo | Autor |
| :--- | :--- | :--- | :--- | :--- |
| **v0.1** | 11/08/2026 | M1 · Fundamentos y Estrategia | Creación del documento base, definición del SoAW, identificación de dolores de Andrew Collins y delimitación del alcance. | Jamir Huaman |
| **v0.2** | 18/08/2026 | M2 · Seguridad y Gobierno | Incorporación de la matriz RACI, modelo de roles (Andrew Collins como Owner de Producto y Negocio), clasificación de datos PII y dimensiones de calidad. | Leonardo Pacheco |
| **v0.3** | 25/08/2026 | M3 · MDM y Metadata | Inclusión del catálogo de 23 términos del Glosario de Negocio, Diccionario de Datos técnico y formalización de las primeras 12 reglas DQ en lenguaje de negocio. | Leonardo Pacheco |
| **v0.4** | 08/09/2026 | M4 · Modelado Avanzado | Diseño del Modelo Conceptual, Modelo Lógico 3FN en Silver y Modelo Dimensional en Gold (decisión de preservar categorías como ARRAY y no explotar tabla puente). | John Huerta |
| **v0.5** | 22/09/2026 | M5 · Arquitectura Cloud | Especificación de la Arquitectura de Referencia ABB → SBB, justificación de alternativas descartadas y registro de ADR-01 (conteo de check-ins) y ADR-02 (rating bayesiano). | John Huerta |
| **v1.0** | 06/10/2026 | M6 a M10 · Consolidación Final | Integración de trazabilidad con notebooks ejecutables (E3 a E8), bitácora FinOps de cómputo serverless y consultas SQL para Databricks AI/BI Dashboards. | Todo el equipo |
| **v1.1** | 09/10/2026 | Evaluación Docente / Optimización | Incorporación de lineamientos de la Sesión 19 (grano explícito en Gold, segregación estricta de hechos atómicos y agregados, tabla de cohortes de retención), formalización de zona de cuarentena explícita en Silver (`slv_review_quarantine`), incorporación de ADR-05 (Star Schema vs OBT) y ADR-06 (Dynamic Column Masking en Unity Catalog), y blindaje matemático de KPIs de Producto. | Profesor / Equipo Data Evolution |
| **v1.2** | 09/10/2026 | Integración de Modelado Físico (Hackolade) | Integración del modelo de datos de Josuhe (Hackolade/Draw.io): incorporación de la entidad `Amistad` (`slv_friendship`) para analítica del Social Graph y engagement de usuarios, atributos de votos enviados de usuario (`useful_enviados`, etc.), localización detallada (`direccion`, `codigo_postal`) y campo `categoria_principal` en `gld_dim_negocio`. | Josuhe Sosa / Equipo Data Evolution |
| **v1.3** | 08/11/2026 | Alineación con el modelo lógico en inglés | El modelo físico se alinea con `yelp_modelo_logico.drawio`, `yelp_silver.txt` y `yelp_gold.txt`: nombres de tablas y columnas en inglés en todas las capas (incluidos `yelp_gov` y las columnas de auditoría de Bronze), categorías normalizadas con tabla puente, golden record de negocios (`master_business_id`), Gold con claves naturales y sin SCD tipo 2, y catálogo de 20 reglas DQ (`DQ-001` a `DQ-020`; DQ-016 a DQ-020 provienen del diccionario revisado por el grupo). | Equipo Data Evolution |
| **DQ-016** | Toda reseña corresponde a un usuario existente en la muestra. | `user_id IN (SELECT user_id FROM slv_user)` | Reseña | Consistencia | **Critical** | 100.0% | Cuarentena con motivo DQ-016 + Quality Gate. Se separó de DQ-003 para distinguir un dato faltante de una referencia rota. |
| **DQ-017** | Todo negocio tiene al menos una categoría. | `business_id IN (SELECT business_id FROM slv_business_category)` | Negocio | Completitud | High | 98.0% | Se acepta hasta el umbral; los negocios sin categoría quedan fuera del análisis por categoría. |
| **DQ-018** | Cada relación negocio-categoría apunta a un negocio y una categoría existentes. | `business_id IN (slv_business) AND category_id IN (slv_category)` | Categoría | Consistencia | **Critical** | 100.0% | Quality Gate: bloquea la actualización de `gld_agg_category_month` hasta corregir Silver. |
| **DQ-019** | Cada categoría aparece una sola vez en el catálogo. | `name` único en `slv_category` | Categoría | Unicidad | High | 100.0% | Consolidar variantes de escritura en el catálogo antes de publicar Gold. |
| **DQ-020** | Todo tip tiene autor y negocio existentes y fecha válida. | `user_id IN (slv_user) AND business_id IN (slv_business) AND tip_date BETWEEN 2004 y hoy` | Tip | Consistencia | High | 100.0% | Excluir del conteo de actividad y revisar `02_silver_limpieza`. |

---

## 1. Resumen Ejecutivo

### 1.1. Contexto y Diagnóstico del Problema
YelpConnect opera como el canal principal de interacción entre millones de comensales y negocios locales en más de 30 países. A pesar del éxito comercial de la plataforma, el crecimiento desarticulado de nuevas funciones (Yelp Ads, Waitlist y Reservations) provocó una profunda fractura informacional interna. 

Desde la perspectiva del **VP de Producto, Andrew Collins**, la organización enfrentaba una crisis de confianza que amenazaba la experiencia de usuario y la tracción de la app: las métricas clave de salud del producto diferían sistemáticamente según el área que las consultara. Mientras el equipo de Producto calculaba una calificación promedio y retención con base en eventos sin depurar, Marketing reportaba aumentos de reseñas impulsados por duplicados de campañas, y Operaciones Comerciales inflaba la afluencia contabilizando cada marca temporal repetida de los check-ins para fines publicitarios (una brecha del 28% respecto a visitas reales). Además, la presencia de negocios con calificaciones extremas basadas en una sola reseña distorsionaba el motor de búsqueda, no existían *Data Owners* con responsabilidad legal y de negocio sobre las entidades maestras, los datasets carecían de linaje auditable, y el motor de machine learning de recomendaciones consumía fuentes sin versionamiento ni trazabilidad.

### 1.2. Solución de Arquitectura Diseñada
La Oficina de Arquitectura de Datos diseñó e implementó una plataforma moderna basada en el paradigma **Lakehouse Medallion** desplegada sobre **Databricks**, gobernada de extremo a extremo mediante **Unity Catalog**:
1. **Capa Bronze (`workspace.yelp_bronze`):** Ingesta cruda, inmutable y orientada a eventos mediante **Databricks Auto Loader (`cloudFiles`)**, asegurando trazabilidad forense sin transformar el payload original y añadiendo metadatos de auditoría (`_source_file`, `_ingested_at`).
2. **Capa Silver (`workspace.yelp_silver`):** Conformación en Tercera Forma Normal (3FN), tipificación de datos estricta, validación de esquemas y deduplicación determinística mediante sentencias `MERGE INTO` de Delta Lake. Se implementa una **zona de cuarentena explícita (`slv_review_quarantine`)** para aislar registros no conformes sin frenar la continuidad operacional.
3. **Capa Gold (`workspace.yelp_gold`):** Modelo dimensional de consumo analítico bajo metodología Kimball, con grano declarado en cada tabla: dimensiones `gld_dim_business`, `gld_dim_user`, `gld_dim_category` y `gld_dim_date`; puente `gld_bridge_business_category`; hechos atómicos `gld_fact_review`, `gld_fact_checkin` y `gld_fact_tip`; y ocho agregados `gld_agg_*` por consumidor (por ejemplo, `gld_agg_business_bayesian_month` para el rating bayesiano y `gld_agg_user_cohort_month` para la retención de cohortes).
4. **Gobierno Transversal (`workspace.yelp_gov`):** Marco normativo unificado donde residen las tablas de control institucional (`dq_rules`, `dq_results`, `business_glossary`, `data_dictionary`, `raci_matrix`, `pipeline_run_log`), etiquetas de clasificación PII reales (`ALTER TABLE ... SET TAGS`) y políticas de control de acceso granular con enmascaramiento dinámico (*Dynamic Column Masking*).

### 1.3. Resultados Construidos y Verificables
* **Resolución de la Disputa de Check-ins:** Se adoptó un criterio de deduplicación horaria por usuario y negocio (ADR-01), erradicando al 100% la discrepancia entre la facturación de Yelp Ads y las métricas de visitas reales de Producto (discrepancia = 0.0%).
* **Rating Promedio Ponderado Bayesiano:** Se implementó una fórmula estadística en Gold que neutraliza la distorsión de negocios con una sola reseña atípica mediante amortiguación bayesiana ($m=25$, $C=	ext{media global}$), entregando a Producto una métrica de satisfacción matemáticamente blindada.
* **Calidad Observable Automatizada:** 20 reglas de negocio (`DQ-001` a `DQ-020`) cubren seis dimensiones de calidad; las de criticidad `critical` operan como *Quality Gate* y detienen el pipeline si fallan. Cada corrida queda registrada en `yelp_gov.dq_results`.
* **Cumplimiento y Privacidad por Diseño:** Anonimización y enmascaramiento dinámico de datos de usuarios (`name`, PII indirecta en textos) garantizando conformidad con RGPD, CPRA y la Ley N° 29733 (Perú), sin romper la capacidad analítica agregada.
* **Eficiencia FinOps Demostrable:** Reducción del 70% en escaneo de archivos mediante particionado inteligente (`Z-ORDER BY business_id, date`) y confinamiento de la ejecución diaria a menos de 7 minutos (<25% de la cuota serverless).

### 1.4. Recomendaciones Estratégicas Siguientes
1. **Formalización de Contratos de Datos (Data Contracts):** Establecer acuerdos vinculantes entre los desarrolladores del backend/móvil de YelpConnect y la plataforma Lakehouse para prevenir rupturas silenciosas de esquemas operacionales.
2. **Escalamiento a Nuevos Dominios de Negocio:** Extender la arquitectura hacia las fuentes transaccionales de *Yelp Reservations* y *Yelp Waitlist*, homologando el identificador maestro de usuario y negocio.
3. **Analítica Avanzada de NLP sobre Texto Libre:** Operacionalizar el módulo `EX_bonus_ia` utilizando modelos de lenguaje integrados de Databricks (`ai_analyze_sentiment`, `ai_classify`) para categorizar automáticamente las razones cualitativas de insatisfacción en el producto (servicio, tiempos de espera, higiene).

---

## 2. Visión de Arquitectura y SoAW (Entregable E1)

### 2.1. Sponsor y Contexto del Encargo
* **Sponsor Ejecutivo:** **Laura Hernández (Chief Data Officer)**, con el co-patrocinio funcional prioritario de **Andrew Collins (VP of Product)**.
* **Contexto de Urgencia:** Durante el cierre del segundo trimestre, la dirección de Producto planificaba el rediseño del algoritmo de recomendación y las insignias de fidelidad. Sin embargo, se constató que el 14% de las reseñas de negocios altamente calificados provenían de cuentas duplicadas o no verificadas, y que el conteo de afluencia difería en un 28% respecto a los datos reportados a los anunciantes. El comité ejecutivo suspendió el despliegue de producto hasta contar con una arquitectura de datos certificada.
* **Consecuencia de la Inacción:** De no implementarse esta arquitectura, Yelp continuará sufriendo fuga de usuarios descontentos por recomendaciones inexactas, litigios comerciales con anunciantes por métricas infladas y multas regulatorias por manejo indebido de datos de privacidad.

### 2.2. Stakeholders del Caso

| Área | Stakeholder | Dolor actual | Qué espera obtener | Tensión cruzada con | Prioridad |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Producto (Prioritario)** | **Andrew Collins**, VP of Product | Métricas de satisfacción y rating poco confiables; imposibilidad de medir retención real de usuarios en YelpConnect. | Única versión de la verdad sobre interacción de usuarios, ratings ponderados certificados y vistas de engagement fidedignas. | Operaciones Comerciales / Marketing | **Alta** |
| **Marketing & Growth** | Jessica Martínez, Director of Growth | Inconsistencia de métricas por duplicidad de registros generados en campañas masivas. | Atribución real de adquisición, detección de usuarios únicos y volumen neto de reseñas efectivas. | Trust & Safety | Media |
| **Data Science & Analytics** | Mei Lin, Director of Data Science | Datasets sin linaje, sin metadatos y modelos de machine learning entrenados sobre fuentes no versionadas. | Capa conformada (Silver/Gold) con versionamiento histórico (*Time Travel*) y linaje observable para entrenamiento de modelos. | Infraestructura y Producto | Alta |
| **Trust & Safety** | Rafael Nakamura, Head of Safety Operations | Imposibilidad de auditar el origen de reseñas fraudulentas; falta de trazabilidad en las reglas de mitigación de fraude. | Linaje de datos auditable desde la ingesta cruda y aislamiento de reseñas en cuarentena antes de impactar el rating público. | Marketing & Growth | Alta |
| **Operaciones Comerciales (Ads)** | Carla Romero, Head of Ads Strategy | Conteo inconsistente de check-ins que distorsiona la facturación de campañas publicitarias. | Conteo uniforme y homologado de check-ins con granularidad temporal clara para el cobro a anunciantes. | Producto | Alta |
| **Infraestructura y Datos (Platform)** | Laura Hernández, CDO | Fragmentación tecnológica en silos y ausencia absoluta de responsabilidad (*accountability*) sobre los datos. | Plataforma Lakehouse centralizada y gobernada con matriz RACI formalizada y políticas de seguridad Unity Catalog. | Todas las áreas operacionales | Alta |

### 2.3. Motivadores del Negocio

| ID | Motivador | Qué se pierde si no se atiende | Stakeholder que lo sostiene |
| :--- | :--- | :--- | :--- |
| **MOT-01** | **Consolidación de Métricas de Producto y Satisfacción:** Establecer una verdad única para la calificación de negocios y la actividad de usuarios. | Pérdida de usuarios activos por desconfianza en las recomendaciones; degradación del NPS corporativo y caída de cuota de mercado frente a competidores. | Andrew Collins (VP Product) |
| **MOT-02** | **Accountability y Ownership Formal:** Asignar formalmente la propiedad de las entidades maestras de datos a líderes de negocio. | Continuación del caos informacional; nadie responde por la calidad, definiciones huérfanas y disputas operativas interminables. | Laura Hernández (CDO) |
| **MOT-03** | **Depuración de Reseñas y Check-ins Duplicados:** Erradicar la duplicidad transaccional y el spam publicitario. | Pérdida de credibilidad de la plataforma ante los comercios afiliados y riesgo de demandas por cobros publicitarios inflados. | Carla Romero (Ads) / Andrew Collins |
| **MOT-04** | **Trazabilidad y Linaje Auditable de Extremo a Extremo:** Demostrar el origen, transformaciones y reglas aplicadas a cada dato. | Incapacidad de defender cifras ante auditorías externas y degradación de modelos predictivos de Data Science. | Rafael Nakamura (Trust & Safety) / Mei Lin |
| **MOT-05** | **Gobernanza Financiera y Eficiencia de Cómputo (FinOps):** Garantizar la viabilidad técnica y económica en entornos serverless. | Agotamiento prematuro de las cuotas diarias de cómputo en Databricks y sobrecostos presupuestarios desmedidos. | Laura Hernández (CDO) |

### 2.4. Alcance del Proyecto

| Dentro del alcance | Fuera del alcance | Por qué queda fuera |
| :--- | :--- | :--- |
| • Ingesta de las 5 fuentes transaccionales principales: `business`, `user`, `review`, `checkin`, `tip` (muestra acotada a una ciudad). | • Rediseño del código transaccional de la app móvil o el backend web de YelpConnect. | La Oficina de Arquitectura de Datos habilita y gobierna el consumo analítico; no altera el core operacional transaccional. |
| • Modelado conceptual, lógico y físico Lakehouse en Unity Catalog (capas Bronze, Silver, Gold y Gov). | • Gestión y ejecución operativa de campañas publicitarias en Yelp Ads. | Corresponde al dominio operativo de Marketing; la plataforma únicamente entrega los productos analíticos de consumo. |
| • Catálogo institucional de 12 reglas de calidad de datos (6D) y motor automatizado en PySpark/SQL operando como Quality Gates. | • Migración masiva de históricos superiores a 10 años en múltiples regiones cloud. | Restricción de cuota de Databricks Free Edition; el modelo se valida sobre una muestra territorial representativa (<300 MB). |
| • Asignación formal de roles RACI, Glosario de Negocio (23 términos) y Diccionario de Datos (30 elementos técnicos). | • Entrenamiento y puesta en producción de modelos de deep learning de generación de texto. | Excede el objetivo fundacional del proyecto integrador; se habilita un prototipo NLP en el módulo bonus (`EX_bonus_ia`). |
| • Definición de interfaces de Contratos de Datos (Data Contracts) y zona de cuarentena explícita en Silver. | • Conexión directa JDBC a bases de datos operacionales de producción sin pasar por Lakehouse. | Práctica de antipatrón que genera acoplamiento estrecho, saturación de transacciones y evasión del gobierno de datos. |
| • Dashboard de BI para Producto y Gobierno en Databricks AI/BI y espacio semántico Genie. | • Despliegue de infraestructura IaaS permanente (clusters dedicados 24/7). | Contrario a las directrices FinOps de cómputo serverless bajo demanda. |

### 2.5. Criterios de Éxito

| ID | Criterio de éxito | Cómo se verifica | Motivador que atiende |
| :--- | :--- | :--- | :--- |
| **CE-01** | **Métricas Unificadas de Producto:** El 100% de los reportes de Producto y Operaciones consultan las mismas tablas certificadas de Gold (`gld_agg_business_month`, `gld_agg_business_bayesian_month`). | Verificación en el linaje de Unity Catalog: cero consultas a capas intermedias no autorizadas; discrepancia de métricas = 0.0%. | MOT-01, MOT-03 |
| **CE-02** | **Ownership y RACI Formalizado:** El 100% de las entidades críticas y columnas gobernadas tienen un Data Owner y Steward asignado. | Inspección de la tabla `workspace.yelp_gov.data_dictionary` y `raci_matrix`: cero atributos con campos de owner nulos. | MOT-02 |
| **CE-03** | **Calidad Observable Automatizada:** Ejecución sistemática de las 20 reglas DQ en cada corrida; ninguna regla `critical` puede fallar. | Consulta SQL sobre la tabla `workspace.yelp_gov.dq_results` tras la ejecución del pipeline programado. | MOT-03, MOT-04 |
| **CE-04** | **Protección de Datos Personales (PII):** Cumplimiento total de enmascaramiento dinámico para atributos sensibles de usuarios. | Consulta con perfil de analista estándar sobre `slv_user`: los campos `name` y correos aparecen enmascarados vía tags de Unity Catalog. | MOT-04 |
| **CE-05** | **Sostenibilidad Operativa FinOps:** El pipeline multi-tarea completo se ejecuta dentro de la cuota diaria de Databricks Free Edition sin cortes. | Auditoría de la tabla `workspace.yelp_gov.pipeline_run_log`: duración y volumetría por tabla en cada corrida. | MOT-05 |
| **CE-06** | **Disponibilidad y Reconciliación de Producto (SLA):** Tablas Gold actualizadas diariamente con reconciliación matemática estricta ($delta = 0$). | Reconciliación en E6: filas de `brz_review` = `slv_review` + `slv_review_quarantine`, y `slv_review` = `gld_fact_review`. | MOT-01, MOT-05 |

### 2.6. Restricciones y Supuestos

| Tipo | Enunciado | Impacto en el diseño |
| :--- | :--- | :--- |
| **Restricción** | **Cuota Limitada de Cómputo Serverless:** El entorno de Databricks Free Edition cuenta con cuota diaria de DBU y un solo metastore por cuenta. | Obliga a muestrear geográficamente el dataset a una ciudad (< 300 MB), evitar operaciones de `display()` redundantes y diseñar pipelines estrictamente idempotentes. |
| **Restricción** | **Inmutabilidad de Fuentes Transaccionales:** Los esquemas de origen JSON no pueden modificarse a nivel de producción. | El Lakehouse debe manejar esquemas semi-estructurados en Bronze y realizar el desempaquetado y tipificación en Silver mediante código. |
| **Restricción** | **Restricciones de Claves no Enforzadas:** Unity Catalog declara `PRIMARY KEY` y `FOREIGN KEY` de forma informativa (no forzadas por el motor Delta). | La integridad referencial y unicidad física deben garantizarse programáticamente mediante reglas DQ específicas (`DQ-001`, `DQ-003`, `DQ-006`, `DQ-012`) y Quality Gates. |
| **Supuesto** | **Adopción Organizacional:** Los líderes de área respetarán la gobernanza y consumirán exclusivamente desde la capa Gold. | El equipo de CDO implementará restricciones de permisos (`GRANT SELECT`) bloqueando el acceso de usuarios finales a Silver y Bronze. |
| **Supuesto** | **Estabilidad Semántica:** El Glosario de Términos aprobado representa el acuerdo institucional definitivo sobre las métricas. | Cualquier cambio futuro en fórmulas o definiciones requerirá una solicitud formal de excepción y modificación del control de versiones. |

---

## 3. Arquitectura de Referencia (Entregable E2)

### 3.1. Capacidades de Negocio

| ID | Capacidad de negocio | Motivador que habilita | Área beneficiada |
| :--- | :--- | :--- | :--- |
| **CAP-01** | **Consolidación de Métricas de Experiencia de Usuario:** Publicar indicadores mensuales de rating, distribución de estrellas y volumen de opiniones con trazabilidad auditable. | MOT-01 | Producto, Marketing |
| **CAP-02** | **Deduplicación y Depuración Transaccional:** Detectar y filtrar reseñas repetidas y check-ins simultáneos antes de propagar datos a analítica. | MOT-03 | Producto, Operaciones Comerciales |
| **CAP-03** | **Evaluación Continua de Calidad de Datos:** Monitorear el apego a reglas de integridad, validez y completitud con alertas y Quality Gates. | MOT-02, MOT-04 | Trust & Safety, CDO |
| **CAP-04** | **Enmascaramiento y Seguridad de Datos Personales:** Proteger la identidad de usuarios en reportes y paneles sin degradar el análisis agregado. | MOT-04 | Trust & Safety, Legal |
| **CAP-05** | **Historiografía y Versionamiento Analítico:** Conservar la evolución temporal del comportamiento de negocios y estados operativos mediante Time Travel. | MOT-01, MOT-04 | Data Science, Producto |
| **CAP-06** | **Autoservicio Semántico para Consumo de Producto:** Permitir consultas en lenguaje natural y dashboards ejecutivos basados en datos certificados. | MOT-01, MOT-05 | Producto, CDO |
| **CAP-07** | **Observabilidad de Datos y Linaje de Métricas:** Demostrar el camino completo desde el evento JSON hasta el cálculo del KPI directivo. | MOT-02, MOT-04 | Data Platform, Auditoría |

### 3.2. Bloques Arquitectónicos (ABB → SBB)

| ABB (Bloque Abstracto) | Capacidad que habilita | SBB (Componente Concreto) | Alternativas descartadas y por qué |
| :--- | :--- | :--- | :--- |
| **Almacenamiento Analítico Columnar Versionado** | CAP-01, CAP-05 | **Tablas Delta Lake sobre Unity Catalog** (esquemas `yelp_bronze`, `yelp_silver`, `yelp_gold`) | • **Base de Datos Relacional (PostgreSQL):** Descartada por incapacidad de escalar a volúmenes masivos de datos no estructurados, altos costos de almacenamiento y falta de versionamiento histórico *Time Travel* nativo.<br>• **Data Lake Parquet tradicional (sin capa Delta):** Descartado por carecer de transacciones ACID, lo que provocaría lecturas corruptas ante escrituras concurrentes y la imposibilidad de ejecutar sentencias `MERGE INTO` de deduplicación. |
| **Ingesta Idempotente y Desacoplada** | CAP-02 | **Databricks Auto Loader (`cloudFiles`)** sobre volúmenes de almacenamiento (`/Volumes/.../raw/`) | • **Cargas Batch JDBC directas:** Descartadas por generar sobrecarga y contención de bloqueos en la base transaccional de producción.<br>• **Scripts de copia manual (boto3/az copy):** Descartados por carecer de detección automática de nuevos archivos, evolución de esquemas y tolerancia a fallos. |
| **Motor de Conformación y Procesamiento Distribuido** | CAP-01, CAP-02 | **Apache Spark (PySpark) Serverless** en Databricks Runtime | • **Procesamiento en memoria con Pandas:** Descartado por desbordamiento de memoria RAM ante crecimientos exponenciales de datos y falta de paralelismo distribuido.<br>• **Procedimientos Almacenados en SQL tradicional:** Descartados por carecer de control de versiones de software, trazabilidad CI/CD y capacidades de testing unitario. |
| **Catálogo de Metadatos y Gobierno Centralizado** | CAP-03, CAP-04, CAP-07 | **Databricks Unity Catalog** (esquema tripartito: `catalogo.esquema.tabla`) | • **Hive Metastore clásico:** Descartado porque no soporta linaje automático a nivel de columna, ni tags de gobierno ni permisos RBAC con control granular centralizado.<br>• **Catálogo independiente desacoplado (DataHub / Apache Atlas):** Descartado por implicar costos adicionales de infraestructura y servidores dedicados incompatibles con las restricciones FinOps. |
| **Motor de Calidad de Datos (Rules-as-Data)** | CAP-03 | **Framework Propietario `dq_rules` + `dq_results`** ejecutado en PySpark/SQL como Quality Gate | • **Great Expectations independiente:** Descartado por requerir contenedores dedicados y configuración compleja en entornos universitarios serverless.<br>• **Aserciones ad-hoc dispersas en notebooks:** Descartadas por ser invisibles para auditoría corporativa y no permitir cálculo de métricas históricas de salud. |
| **Orquestación de Pipelines Multi-tarea** | CAP-01, CAP-03 | **Databricks Workflows (Jobs DAG)** | • **Apache Airflow dedicado:** Descartado por requerir aprovisionamiento de infraestructura virtual (IaaS), administración de red y consumo permanente de recursos sin justificación de volumen. |
| **Capa Semántica y Autoservicio de BI** | CAP-06 | **Databricks AI/BI Dashboards + Genie Space** | • **Servidor On-Premise de Power BI / Tableau:** Descartado por requerir pasarelas de enlace de datos (gateways), exportación masiva fuera de la nube y pago de licencias de usuario adicionales. |

### 3.3. Vista de la Arquitectura

```
========================================================================================================================
                          VISTA INTEGRAL DE LA ARQUITECTURA LAKEHOUSE (YELPCONNECT)
========================================================================================================================

  FUENTES OPERACIONALES                   CAPAS DEL LAKEHOUSE (DATABRICKS SERVERLESS)                     CONSUMO Y VALOR
┌───────────────────────┐            ┌──────────────────────────────────────────────────┐            ┌─────────────────┐
│ • YelpConnect Mobile  │            │  BRONZE (Ingesta Cruda - Append Only)            │            │                 │
│ • Yelp Web Platform   │  Auto      │  • /Volumes/workspace/yelp_bronze/raw/           │            │  PRODUCTO       │
│ • Yelp Ads Engine     │ ─────────► │  • brz_business, brz_review, brz_user, ...       │            │  • AI/BI Dash-  │
│ • APIs de Partners    │   Loader   │    (Payload original + _metadata auditoría)      │            │    board        │
└───────────────────────┘            └─────────────────────────┬────────────────────────┘            │  • Genie Space  │
                                                               │                                     │  (Autoservicio) │
                                                               ▼                                     └─────────────────┘
                                     ┌──────────────────────────────────────────────────┐                     ▲
                                     │  SILVER (Conformada, 3FN, Deduplicada)           │                     │
                                     │  • slv_business + slv_category (puente N:M)      │                     │
                                     │  • slv_review     (Deduplicada, Stars 1-5)       │                     │
                                     │  • slv_user       (Antigüedad, Métricas)         │                     │
                                     │  • slv_checkin    (Grano evento, is_duplicate)   │                     │
                                     │  • slv_tip, slv_friendship, slv_photo            │                     │
                                     │  ----------------------------------------------  │                     │
                                     │  • slv_review_quarantine (Datos No Conformes)    │                     │
                                     └─────────────────────────┬────────────────────────┘                     │
                                                               │                                              │
                                                               ▼                                              │
                                     ┌──────────────────────────────────────────────────┐                     │
                                     │  GOLD (Kimball: Dimensional Estrella & KPIs)     │                     │
                                     │  • gld_dim_business, _user, _category, _date     │ ────────────────────┘
                                     │  • gld_fact_review, _checkin, _tip (atómicos)    │
                                     │  • gld_bridge_business_category                  │
                                     │  • gld_agg_* (8 agregados de KPIs por consumidor)│
                                     └──────────────────────────────────────────────────┘
                                                               ▲
                                                               │
┌──────────────────────────────────────────────────────────────┴───────────────────────────────────────────────────────┐
│                          FRANJA VERTICAL DE GOBIERNO TRANSVERSAL (UNITY CATALOG)                                     │
│  • Glosario de Negocio (`yelp_gov.business_glossary`)       • Diccionario de Datos (`yelp_gov.data_dictionary`)       │
│  • Catálogo de Reglas DQ (`yelp_gov.dq_rules`)              • Resultados de Calidad (`yelp_gov.dq_results`)           │
│  • Matriz RACI y Roles (`yelp_gov.raci_matrix`)             • Dynamic Column Masking (`yelp_gov.mask_name`)           │
│  • Bitácora FinOps (`yelp_gov.pipeline_run_log`)            • Linaje Automático de Extremo a Extremo                 │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
========================================================================================================================
```

**Descripción Arquitectónica:**
El diseño implementa tres capas horizontales secuenciales sobre Delta Lake y una franja vertical de gobierno que las intersecta en su totalidad. En el plano horizontal, la capa **Bronze** almacena fielmente los eventos serializados en formato JSON sin ninguna alteración destructiva, agregando únicamente marcas temporales y fuentes de ingesta para trazabilidad forense; la capa **Silver** estandariza tipos de datos, filtra registros inválidos y resuelve duplicados mediante sentencias `MERGE INTO`, materializando entidades limpias bajo la Tercera Forma Normal. Crucialmente, Silver incorpora la tabla de **cuarentena (`slv_review_quarantine`)** para aislar registros sospechosos o con fallas referenciales sin romper el pipeline. Finalmente, la capa **Gold** organiza los datos en modelos dimensionales de estrella y tablas de agregación optimizadas para el consumo de Producto.

La franja vertical de **Gobierno y Unity Catalog** no constituye una etapa posterior del pipeline, sino un conjunto de políticas y metadatos activos aplicados en cada estrato del flujo. Desde esta franja se controlan los accesos por perfiles de usuario, se aplican máscaras dinámicas de datos confidenciales sobre atributos de usuarios, se rastrea el linaje gráfico a nivel de columna y se ejecutan los Quality Gates contra las tablas maestras de gobierno. De este modo, Andrew Collins y su equipo interactúan exclusivamente con productos de datos gobernados y certificados.

### 3.4. Decisiones de Arquitectura (ADR)

| ID | Decisión | Contexto y opciones evaluadas | Motivo de la elección | Estado |
| :--- | :--- | :--- | :--- | :--- |
| **ADR-01** | **Deduplicación de Check-ins por Ventana Horaria** | Origen transaccional entrega marcas concatenadas. Se evaluó deduplicar por día (propuesta de Producto original) o contabilizar cada timestamp (propuesta de Ads). | La deduplicación por hora (ventana de 60 min por usuario y comercio) elimina el ruido de rebotes técnicos sin destruir la señal de visitas reales múltiples, satisfaciendo tanto el cobro justo de Ads como el cálculo de retención de Producto. | **Vigente** |
| **ADR-02** | **Cálculo de Rating Ponderado Bayesiano en Gold** | Producto sufría distorsión por comercios con 1 sola reseña de 5.0 estrellas que superaban a negocios consagrados con miles de calificaciones. Se evaluó promedio aritmético simple vs. media bayesiana. | Se adoptó la media bayesiana ponderada ($m=25$ reseñas mínimas, $C=$ media global de la ciudad) en `gld_agg_business_bayesian_month`. Garantiza estabilidad estadística y credibilidad al algoritmo de recomendaciones. | **Vigente** |
| **ADR-03** | **Categorías normalizadas con tabla puente en Silver y Gold** | Las categorías llegan como cadenas delimitadas por comas. En v0.4 se guardaron como `ARRAY<STRING>`; el modelo lógico vigente (`yelp_modelo_logico.drawio`) exige catálogo propio y relación N:M. | Se adoptó `slv_category` + `slv_business_category` en Silver y `gld_dim_category` + `gld_bridge_business_category` (con `is_primary`) en Gold. Permite métricas por categoría sin explotar arreglos en cada consulta y mantiene la 3FN. Reemplaza la decisión de v0.4. | **Vigente (v1.3)** |
| **ADR-04** | **Adopción de Delta Lake Time Travel para Auditoría** | Modificaciones y eliminaciones de reseñas por usuarios y moderadores borraban el rastro histórico. Se evaluó bitácora manual en base relacional vs. historial nativo de Delta. | Se habilitó la propiedad `delta.enableChangeDataFeed = true` y retención de 30 días de log Delta. Permite auditar el estado exacto de cualquier dato en cualquier punto del tiempo para respaldar decisiones de Trust & Safety y Producto. | **Vigente** |
| **ADR-05** | **Esquema Dimensional Kimball en Gold vs Tablón OBT** | Se evaluó implementar un único tablón desnormalizado (One Big Table - OBT) frente a un esquema dimensional Kimball con hechos atómicos y dimensiones conformes. | Se adoptó el esquema estrella Kimball. Permite análisis flexible de retención (slicing & dicing por usuario, negocio y tiempo) sin incurrir en duplicidad masiva de almacenamiento, complementado por tablas de hechos agregados específicos. | **Vigente** |
| **ADR-06** | **Enmascaramiento Dinámico de PII en Unity Catalog** | Se evaluó anonimizar destructivamente los nombres de usuarios en Silver vs enmascaramiento dinámico en tiempo de consulta. | Se adoptó *Dynamic Column Masking* mediante funciones de Unity Catalog (`workspace.yelp_gov.mask_name`). Preserva el dato original para roles privilegiados (Trust & Safety) y enmascara para analistas de Producto. | **Vigente** |
| **ADR-07** | **Retención de `business.attributes` y `hours` en Bronze (*Schema-on-Read*)** | En `brz_business`, el campo `attributes` es un `STRUCT` altamente disperso de 39 subcampos (28 de ellos con >65% de nulos, literales `u'...'` de Python 2 y diccionarios de 2° nivel serializados como texto). Se evaluó aplanarlo o crear una tabla EAV en Silver vs. mantenerlo en Bronze. | Se decidió conservar `attributes` y `hours` íntegros en `brz_business` (*Schema-on-Read* para exploraciones puntuales de Data Science) y excluirlos de `slv_business`. Ningún requerimiento (`REQ-01` a `REQ-06`) ni KPI de Producto los consume, cumpliendo el principio de minimización de datos (Sección 5.4) y ahorrando cómputo Serverless en Databricks Free Edition. | **Vigente** |

---

## 4. Modelo de Datos Conceptual y Lógico (Entregable E3)

### 4.1. Modelo Conceptual

```
┌────────────────────────┐                    ┌────────────────────────┐
│        USUARIO         │                    │        NEGOCIO         │
├────────────────────────┤                    ├────────────────────────┤
│ • user_id (PK)         │◄───(es amigo de)──►│ • business_id (PK)     │
│ • name, review_count   │    [Amistad N:M]   │ • nombre, direccion    │
│ • yelping_since, fans  │                    │ • ciudad, cod_postal   │
│ • votos_enviados       │                    │ • estrellas, abierto   │
└───────────┬────────────┘                    └───────────┬────────────┘
            │                                             │
            │ 1                                           │ 1
            │                                             │
            │              ┌────────────────┐             │
            ├────────────► │    RESEÑA      │ ◄───────────┤
            │ N            ├────────────────┤           N │
            │              │ • review_id    │             │
            │              │ • stars, text  │             │
            │              │ • date, useful │             │
            │              └────────────────┘             │
            │                                             │
            │              ┌────────────────┐             │
            ├────────────► │      TIP       │ ◄───────────┤
            │ N            ├────────────────┤           N │
            │              │ • tip_id, text │             │
            │              │ • date         │             │
            │              └────────────────┘             │
            │                                             │
            │              ┌────────────────┐             │
            ├────────────► │    CHECK-IN    │ ◄───────────┘
            │ (visitas)    ├────────────────┤           N
            │              │ • checkin_id   │
            │              │ • date         │
            │              └────────────────┘
            │                                             │
            │              ┌────────────────┐             │
            └────────────► │      FOTO      │ ◄───────────┘
              (multimedia) ├────────────────┤           N
                           │ • photo_id     │
                           │ • caption      │
                           └────────────────┘
```

| Entidad | Definición de negocio | Dominio | Relaciones principales |
| :--- | :--- | :--- | :--- |
| **Negocio** | Establecimiento comercial registrado en YelpConnect receptor de interacciones y opiniones. | Negocio | Tiene N Reseñas, recibe N Tips y acumula N Check-ins. |
| **Usuario** | Individuo registrado en YelpConnect facultado para opinar, visitar y evaluar establecimientos. | Usuario | Publica N Reseñas, registra N Tips y genera N Check-ins. |
| **Reseña** | Acto formal de valoración que combina una calificación numérica (1-5) y un texto libre argumentativo. | Contenido | Pertenece a 1 Usuario y evalúa a 1 Negocio. |
| **Check-in** | Marca de afluencia física o presencia confirmada de un usuario en las instalaciones del negocio. | Interacción | Vinculado a 1 Negocio en una marca temporal dada. |
| **Tip** | Recomendación o consejo breve publicado por un usuario sobre un aspecto puntual del negocio. | Contenido | Emitido por 1 Usuario hacia 1 Negocio. |
| **Foto** | Elemento multimedia cargado por usuarios para ilustrar productos o instalaciones del negocio. | Contenido | Asociado a 1 Negocio y capturado por 1 Usuario. |
| **Amistad (Red Social)** | Vínculo bidireccional entre dos usuarios de la plataforma que habilita el Social Graph y la confianza en recomendaciones. | Interacción / Red | Relaciona a 1 Usuario con N Usuarios amigos (`user_id_1` ↔ `user_id_2`). |

### 4.2. Modelo Lógico y Decisiones de Normalización
En la capa **Silver**, el modelo se mantiene estrictamente en **Tercera Forma Normal (3FN)**:
1. **Normalización Atómica:** Toda columna no dependiente de la clave primaria fue relocalizada en su entidad de origen. Las 10 tablas Silver (`slv_business`, `slv_category`, `slv_business_category`, `slv_user`, `slv_friendship`, `slv_review`, `slv_review_quarantine`, `slv_tip`, `slv_checkin`, `slv_photo`) tienen grano único declarado en su comentario.
2. **Claves Primarias y Foráneas:** Se declaran restricciones informativas en Unity Catalog (`CONSTRAINT pk_review PRIMARY KEY (review_id)`, `CONSTRAINT fk_review_business FOREIGN KEY (business_id) REFERENCES slv_business(business_id)`). Aunque Delta Lake no aborta transacciones por violación de claves foráneas, estas restricciones habilitan al optimizador Catalyst para simplificar planes de ejecución y construir el linaje automático en el catálogo.
3. **Restricciones Enforzadas:** Se configuran cláusulas `CHECK (stars BETWEEN 1 AND 5)` y `NOT NULL` sobre identificadores y calificaciones, garantizando que registros anómalos sean rechazados en el motor.
4. **Categorías Normalizadas (ADR-03):** Las categorías se modelan con catálogo propio `slv_category` y tabla puente N:M `slv_business_category`, como define el modelo lógico. En Gold se publican como `gld_dim_category` y `gld_bridge_business_category`, donde `is_primary` marca una categoría principal por negocio.
5. **Red Social y Golden Record:** `slv_friendship` (`user_id_1`, `user_id_2`, con `user_id_1 < user_id_2`) guarda el grafo de amistades una sola vez por par. `slv_business.master_business_id` agrupa los registros del mismo negocio físico (nombre, dirección y código postal normalizados) y Gold publica solo el negocio maestro (DQ-012).
6. **Zona de Cuarentena:** `slv_review_quarantine` guarda las reseñas que incumplen DQ-001, DQ-003, DQ-004, DQ-005 o DQ-006, con `rejection_reason` y `rejected_at`, sin detener la operación diaria. Las duplicadas lógicas (DQ-002) no se rechazan: quedan en Silver y Gold las marca con `is_valid = false`.
7. **Exclusión Deliberada de Estructuras Anidadas Dispersas (`business.attributes` y `hours` — ADR-07):** El perfilado sobre los 14,577 negocios de la muestra demostró que `attributes` contiene 39 subcampos opcionales dependientes del giro comercial (24 booleanos en texto, 8 categóricos con residuos de serialización Python 2 como `"u'free'"` y 7 diccionarios anidados serializados como cadenas con comillas simples como `BusinessParking` y `Ambience`), donde 28 de los 39 atributos superan el 65% de nulidad. Dado que ningún requerimiento de información (`REQ-01` a `REQ-06`) ni KPI de negocio los utiliza, ambos objetos (`attributes` y `hours`) se preservan sin pérdida en `workspace.yelp_bronze.brz_business` bajo enfoque *Schema-on-Read* y no se promueven a `slv_business`, evitando tablas EAV o columnas dispersas que penalizarían la cuota diaria de Databricks Free Edition.

### 4.3. Estrategia Dimensional para la Capa Gold (Kimball)
Cumpliendo rigurosamente los 12 lineamientos de la Sesión 19 ("Lineamientos Técnicos Gold"):

| Tabla Gold | Tipo | Declaración Explícita del Grano | Dimensiones asociadas | Tratamiento de historia |
| :--- | :--- | :--- | :--- | :--- |
| **`gld_fact_review`** | Hecho Transaccional Atómico | **Una fila por reseña publicada.** `is_valid` indica si cuenta en métricas oficiales. | `gld_dim_business`, `gld_dim_user`, `gld_dim_date` | Se recarga completa en cada corrida; el historial queda en el Change Data Feed de Delta. |
| **`gld_fact_checkin`** | Hecho Transaccional Atómico | **Una fila por check-in.** No identifica al usuario. | `gld_dim_business`, `gld_dim_date` | Igual que `gld_fact_review`. |
| **`gld_fact_tip`** | Hecho Transaccional Atómico | **Una fila por tip.** | `gld_dim_business`, `gld_dim_user`, `gld_dim_date` | Igual que `gld_fact_review`. |
| **`gld_agg_business_month`** / **`_week`** | Hecho Agregado Periódico | **Una fila por negocio y mes / semana.** | `gld_dim_business` | Reseñas válidas, rating válido, check-ins y tips del periodo. |
| **`gld_agg_business_bayesian_month`** | Hecho Agregado Periódico | **Una fila por negocio y mes.** | `gld_dim_business` | Rating bayesiano ($m=25$, $C$ = promedio de la ciudad) para el ranking de Producto (ADR-02). |
| **`gld_agg_business_checkin_day`** | Hecho Agregado Periódico | **Una fila por negocio y día con check-ins.** | `gld_dim_business`, `gld_dim_date` | Check-ins oficiales (sin duplicados, DQ-010) para Operaciones Comerciales. |
| **`gld_agg_category_month`** | Hecho Agregado Periódico | **Una fila por categoría principal y mes.** | `gld_dim_category` | Indicadores por categoría (REQ-01). |
| **`gld_agg_user_month`** / **`gld_agg_user_cohort_month`** | Hecho Agregado Periódico | **Una fila por usuario y mes / por cohorte y mes de actividad.** | `gld_dim_user` | Actividad mensual y retención de cohortes ($CRR$, REQ-04). |
| **`gld_agg_review_health_month`** | Hecho Agregado Periódico | **Una fila por mes.** | — | Reseñas válidas, duplicadas y en cuarentena para Trust & Safety (REQ-03). |
| **`gld_dim_business`** | Dimensión Conforme | **Una fila por negocio maestro (golden record).** | N/A | Estado vigente (sin SCD tipo 2). Incluye `primary_category`. |
| **`gld_dim_user`** | Dimensión Conforme | **Una fila por usuario.** | N/A | Estado vigente (sin SCD tipo 2). `name` con máscara dinámica (ADR-06). |
| **`gld_dim_category`** + **`gld_bridge_business_category`** | Dimensión + Puente | **Una fila por categoría / por par negocio-categoría.** | N/A | `is_primary` marca la categoría principal (ADR-03). |
| **`gld_dim_date`** | Dimensión Conforme | **Una fila por día (2004–2030).** | N/A | Estática; generada por secuencia de fechas. `date_key` = YYYYMMDD. |

### 4.4. Trazabilidad Conceptual → Lógico → Físico

| Entidad conceptual | Entidad lógica (Silver 3FN) | Tabla física en Unity Catalog | Claves de Enlace |
| :--- | :--- | :--- | :--- |
| **Negocio** | `BUSINESS (business_id, master_business_id, name, address, city, state, postal_code, latitude, longitude, stars, review_count, is_open)` | `workspace.yelp_silver.slv_business` | PK: `business_id`; FK: `master_business_id` |
| **Categoría** | `CATEGORY (category_id, name)` y `BUSINESS_CATEGORY (business_id, category_id)` | `workspace.yelp_silver.slv_category`, `slv_business_category` | PK: `category_id`; PK compuesta en el puente |
| **Usuario** | `USER (user_id, name, review_count, yelping_since, average_stars, fans, elite_year_count, ...)` | `workspace.yelp_silver.slv_user` | PK: `user_id` |
| **Amistad (Red Social)** | `FRIENDSHIP (user_id_1, user_id_2)` | `workspace.yelp_silver.slv_friendship` | PK compuesta: `(user_id_1, user_id_2)` |
| **Reseña** | `REVIEW (review_id, user_id, business_id, stars, review_date, text, useful, funny, cool)` | `workspace.yelp_silver.slv_review` | PK: `review_id` |
| **Reseña rechazada** | `REVIEW_QUARANTINE (review_id, ..., rejection_reason, rejected_at)` | `workspace.yelp_silver.slv_review_quarantine` | Sin PK (auditoría) |
| **Check-in** | `CHECKIN (checkin_id, business_id, checkin_at, is_duplicate)` | `workspace.yelp_silver.slv_checkin` | PK: `checkin_id` |
| **Tip** | `TIP (tip_id, user_id, business_id, text, tip_date, compliment_count)` | `workspace.yelp_silver.slv_tip` | PK: `tip_id` |
| **Foto** | `PHOTO (photo_id, business_id, caption, label)` | `workspace.yelp_silver.slv_photo` | PK: `photo_id` |
| **Dimensión Negocio** | `DIM_BUSINESS (business_key, name, city, state, primary_category, is_open, declared_stars)` | `workspace.yelp_gold.gld_dim_business` | Clave natural: `business_key` |
| **Dimensión Usuario** | `DIM_USER (user_key, name, yelping_since, fans, avg_stars, is_elite)` | `workspace.yelp_gold.gld_dim_user` | Clave natural: `user_key` |
| **Dimensión Fecha** | `DIM_DATE (date_key, full_date, year, quarter, month, month_start, ...)` | `workspace.yelp_gold.gld_dim_date` | `date_key` (YYYYMMDD) |
| **Hechos Reseñas** | `FACT_REVIEW (review_id, business_key, user_key, date_key, stars, is_valid, ...)` | `workspace.yelp_gold.gld_fact_review` | FKs a dimensiones |
| **KPIs Producto** | `AGG_BUSINESS_BAYESIAN_MONTH (business_key, period_month, valid_rating, bayesian_rating, ...)` | `workspace.yelp_gold.gld_agg_business_bayesian_month` | PK: `(business_key, period_month)` |
| **Cohortes Engagement** | `AGG_USER_COHORT_MONTH (cohort_month, activity_month, cohort_size, active_user_count, retention_rate)` | `workspace.yelp_gold.gld_agg_user_cohort_month` | PK: `(cohort_month, activity_month)` |

> El detalle completo de columnas, tipos y claves de todas las tablas está en `documentos/03_modelo_logico_y_dimensional_gold.md`, generado desde Unity Catalog.

---

## 5. Estrategia de Gobierno y Matriz RACI (Entregable E6)

### 5.1. Modelo de Roles de Gobierno
Siguiendo las directrices del marco DAMA-DMBOK 2:
* **Data Owner (Dueño de Negocio):** Responsable máximo ante el comité ejecutivo del significado, clasificación, calidad y uso permitido de los datos de su dominio. Aprueba definiciones del glosario y asume riesgos residuales. En YelpConnect:
  - **Andrew Collins (VP Product):** Owner de los Dominios **Negocio** y **Contenido** (incluye Reseñas, Tips y Calificaciones).
  - **Rafael Nakamura (Trust & Safety):** Owner del Dominio **Datos Personales y Seguridad**.
  - **Carla Romero (Ads):** Owner del Dominio **Interacción y Publicidad** (Check-ins y Facturación).
* **Data Steward (Custodio Funcional):** Líder operativo que define reglas de calidad de datos, documenta diccionarios, verifica linaje y atiende solicitudes de acceso. Se nombran Stewards de Producto, T&S y Privacidad.
* **Data Custodian (Custodio Técnico):** Equipo de Data Platform responsable de la infraestructura cloud, ejecución de pipelines, despliegue de esquemas y disponibilidad del Lakehouse.
* **Consumidor de Datos:** Analistas de Producto, Científicos de Datos y Líderes Ejecutivos autorizados a consultar tablas certificadas en Gold.

### 5.2. Matriz RACI por Entidad

| Entidad | Definir significado | Aprobar accesos | Operar calidad | Sostener infraestructura |
| :--- | :--- | :--- | :--- | :--- |
| **Negocio** | **A:** Andrew Collins (Producto)<br>**R:** Steward de Producto | **A:** Andrew Collins<br>**C:** Operaciones Comerciales | **R:** Steward de Producto<br>**I:** Laura Hernández (CDO) | **R:** Data Platform<br>**A:** Data Custodian |
| **Reseña** | **A:** Andrew Collins (Producto)<br>**R:** Steward de Producto / T&S | **A:** Andrew Collins<br>**C:** Rafael Nakamura (T&S) | **R:** Steward de T&S y Producto<br>**I:** Laura Hernández | **R:** Data Platform<br>**A:** Data Custodian |
| **Usuario** | **A:** Rafael Nakamura (T&S)<br>**R:** Steward de Privacidad | **A:** Rafael Nakamura<br>**C:** Legal / Andrew Collins | **R:** Steward de Privacidad<br>**I:** Andrew Collins | **R:** Data Platform<br>**A:** Data Custodian |
| **Check-in** | **A:** Carla Romero (Ads)<br>**R:** Steward de Interacción | **A:** Carla Romero<br>**C:** Andrew Collins (Producto) | **R:** Steward de Interacción<br>**I:** Andrew Collins | **R:** Data Platform<br>**A:** Data Custodian |
| **Tip** | **A:** Andrew Collins (Producto)<br>**R:** Steward de Producto | **A:** Andrew Collins<br>**C:** Trust & Safety | **R:** Steward de Producto<br>**I:** CDO | **R:** Data Platform<br>**A:** Data Custodian |
| **Foto** | **A:** Andrew Collins (Producto)<br>**R:** Steward de Producto / T&S | **A:** Andrew Collins<br>**C:** Trust & Safety | **R:** Steward de T&S<br>**I:** Data Platform | **R:** Data Platform<br>**A:** Data Custodian |

### 5.3. Política de Clasificación de Datos

| Nivel | Definición en el caso | Controles exigidos | Ejemplo de dato |
| :--- | :--- | :--- | :--- |
| **Público** | Dato abierto visible en la ficha pública de YelpConnect. Sin impacto de confidencialidad. | Sin restricciones de lectura; auditado en cambios y versionado. | Nombre comercial del negocio, ciudad, estrellas promedio publicadas. |
| **Interno** | Datos operativos para uso exclusivo de colaboradores de Yelp. | Acceso mediante grupos de Unity Catalog (`grp_producto`, `grp_marketing`). Prohibida exportación no autorizada. | IDs técnicos, flags de procesamiento (`fecha_carga`), conteo de fans de usuarios. |
| **Confidencial** | Información sensible cuyo compromiso impacta decisiones comerciales o modelos analíticos. | Acceso restringido por rol estricto; cifrado en reposo y tránsito; auditoría de consultas. | Métricas brutas de facturación publicitaria, algoritmos de detección de spam, reglas internas de moderación. |
| **Restringido / PII** | Información de Identificación Personal protegida por leyes internacionales de privacidad. | **Enmascaramiento dinámico**, tags `confidencialidad = 'PII'` en Unity Catalog; acceso exclusivo para Trust & Safety. | Nombre de pila del usuario (`name`), correo electrónico, coordenadas exactas no agregadas. |

### 5.4. Marco Normativo Aplicable

| Norma | Qué exige sobre los datos del caso | Dónde se cumple en la plataforma |
| :--- | :--- | :--- |
| **RGPD (UE) 2016/679** | • **Minimización de Datos (Art. 5):** No almacenar datos sin finalidad analítica explícita.<br>• **Derecho de Supresión (Art. 17):** Habilitar borrado de usuarios mediante Time Travel e instrucciones `DELETE`/`MERGE`. | Retención de 36 meses configurada en el Diccionario de Datos; seudonimización de `user_id`; enmascaramiento dinámico de nombres. |
| **CCPA / CPRA (California)** | • Prohibición de venta de datos personales sin consentimiento (*Do Not Sell My Info*).<br>• Transparencia y derecho a auditar datos retenidos. | Gobernanza de permisos en Unity Catalog; bloqueo absoluto de extracción de datos personales en bruto a partners comerciales. |
| **Ley N° 29733 (Perú)** | • Principio de consentimiento y seguridad en el tratamiento de bancos de datos personales. | Almacenamiento seguro en esquemas gobernados con control de auditoría en `yelp_gov.pipeline_run_log`. |
| **Políticas Internas de Yelp** | • Prohibición de uso de reseñas en cuarentena para fines comerciales o publicidad dirigida. | Aislamiento preventivo de registros que incumplen reglas DQ hacia tablas de cuarentena en Silver antes de publicar en Gold. |

### 5.5. Proceso de Gestión de Excepciones y SLAs de Gobernanza
1. **Solicitud Formal:** El colaborador que requiera una excepción de acceso temporal o modificación de regla registra un ticket ante el Data Steward del dominio correspondiente, justificando el propósito analítico y la duración.
2. **Evaluación de Alternativas (SLA: 24 horas):** El Data Steward verifica si existe una vista o producto certificado en la capa Gold que satisfaga la necesidad sin exponer datos en crudo. Si existe, la solicitud se canaliza a través de dicha tabla y se cierra.
3. **Aprobación del Data Owner (SLA: 48 horas):** Si no existe alternativa técnica, la solicitud es elevada a **Andrew Collins** (o al Owner respectivo), quien evalúa el riesgo y aprueba o rechaza formalmente la excepción.
4. **Registro con Caducidad Obligatoria:** Toda excepción aprobada se registra en la bitácora institucional con una fecha límite de vencimiento (máximo 30 días calendario) y es revocada de manera automática por scripts de gobernanza.

---

## 6. Reglas de Calidad en Lenguaje de Negocio (Entregable E5)

### 6.1. Dimensiones de Calidad Adoptadas

| Dimensión | Qué significa en YelpConnect | Por qué importa al negocio (Impacto en Producto) |
| :--- | :--- | :--- |
| **Validez** | Que los datos numéricos y categóricos se ubiquen dentro de los rangos válidos del dominio de negocio. | Si un rating tiene 0 estrellas o 6 estrellas, distorsiona completamente el promedio y corrompe los algoritmos de recomendación. |
| **Unicidad** | Que cada transacción o entidad se registre exactamente una sola vez sin duplicidades de clave. | Duplicados inflan artificialmente la popularidad de comercios y alteran las métricas de engagement mensual. |
| **Integridad Referencial** | Que toda relación entre entidades apunte a un registro padre existente y consistente. | Reseñas huérfanas sin negocio o sin usuario asignado representan datos basura que no pueden presentarse en la app móvil. |
| **Completitud** | Que los atributos críticos obligatorios posean valores no nulos. | Comercios sin nombre o sin ciudad no pueden ser indexados en búsquedas geográficas de los usuarios. |
| **Consistencia** | Que no existan contradicciones lógicas entre atributos dependientes de una misma entidad. | Un comercio cerrado (`is_open = false`) no debería recibir reseñas recientes de actividad activa. |
| **Actualidad** | Que las marcas temporales reflejen hechos ocurridos en periodos cronológicamente plausibles. | Marcas de tiempo en el futuro o fechas anteriores al lanzamiento de Yelp indican fallos severos de sincronización. |

### 6.2. Catálogo de Reglas de Calidad de Datos

| ID | Regla en lenguaje de negocio | Expresión Técnica SQL | Entidad | Dimensión | Criticidad | Umbral | Decisión si no se cumple (Quality Gate) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **DQ-001** | Cada reseña se registra una sola vez (`review_id` único). | `review_id IS NOT NULL` + conteo distinto | Reseña | Unicidad | **Critical** | 100.0% | El `review_id` repetido va a `slv_review_quarantine`; se conserva la versión más reciente. |
| **DQ-002** | Un usuario no publica el mismo texto dos veces sobre el mismo negocio. | `row_number() OVER (PARTITION BY user_id, business_id, md5(text)) = 1` | Reseña | Unicidad | High | 99.5% | La reseña queda en Silver y `gld_fact_review` la marca con `is_valid = false`. |
| **DQ-003** | Toda reseña tiene autor existente, negocio, estrellas y fecha. | `user_id IN (SELECT user_id FROM slv_user) AND ... IS NOT NULL` | Reseña | Completitud | **Critical** | 100.0% | Cuarentena + Quality Gate. |
| **DQ-004** | Las estrellas son un número entero de 1 a 5. | `stars BETWEEN 1 AND 5` | Reseña | Validez | **Critical** | 100.0% | Cuarentena + Quality Gate; además `CHECK chk_slv_review_stars`. |
| **DQ-005** | La fecha de la reseña no es futura ni anterior a 2004. | `review_date BETWEEN DATE'2004-01-01' AND current_date()` | Reseña | Validez | High | 100.0% | Cuarentena con `DQ-005: FECHA_INVALIDA`. |
| **DQ-006** | Toda reseña corresponde a un negocio existente en la muestra. | `business_id IN (SELECT business_id FROM slv_business)` | Reseña | Consistencia | **Critical** | 100.0% | Cuarentena + Quality Gate. |
| **DQ-007** | La calificación declarada del negocio no difiere en más de 0,5 estrellas del promedio de sus reseñas. | `abs(b.stars - avg(r.stars)) <= 0.5` | Negocio | Consistencia | Medium | 90.0% | Se reporta como `rating_gap` en Gold; no se corrige el dato de Yelp. |
| **DQ-008** | Las reseñas de la muestra no superan las reseñas declaradas del negocio. | `count(r.*) <= b.review_count` | Negocio | Consistencia | Medium | 98.0% | Se reporta; no bloquea la carga. |
| **DQ-009** | Cada check-in tiene fecha y hora válidas. | `checkin_at BETWEEN TIMESTAMP'2004-01-01' AND current_timestamp()` | Check-in | Validez | High | 99.9% | Las fechas inválidas se descartan al cargar. |
| **DQ-010** | No hay dos check-ins del mismo negocio en la misma fecha y hora. | `NOT is_duplicate` | Check-in | Unicidad | High | 99.0% | Los repetidos quedan con `is_duplicate = true` y no cuentan en Gold. |
| **DQ-011** | Todo negocio tiene nombre, dirección, ciudad y coordenadas. | `name, address, city, latitude, longitude IS NOT NULL` | Negocio | Completitud | High | 98.0% | Se acepta; los negocios incompletos se excluyen solo de los mapas. |
| **DQ-012** | Un mismo negocio físico tiene un solo identificador maestro (golden record). | un solo `master_business_id` por nombre + dirección + código postal | Negocio | Unicidad | High | 99.0% | Corrección en pipeline con `master_business_id`. |
| **DQ-013** | Las coordenadas caen dentro del área de Philadelphia. | `latitude BETWEEN 39.80 AND 40.20 AND longitude BETWEEN -75.40 AND -74.90` | Negocio | Exactitud | Medium | 99.0% | Se acepta; se excluyen de los mapas. |
| **DQ-014** | La fecha de alta del usuario no es posterior a su primera reseña. | `yelping_since <= min(review_date)` | Usuario | Consistencia | Medium | 99.0% | Corrección en pipeline: se ajusta `yelping_since`. |
| **DQ-015** | Los datos publicados provienen de la última carga de Bronze. | `loaded_at >= max(brz_review._ingested_at)` | Reseña | Oportunidad | High | 100.0% | Se reporta; `loaded_at` permite auditar la frescura. |

### 6.3. Justificación de Umbrales de Calidad
* **Umbral de 100.0% (`DQ-001`, `DQ-003` a `DQ-006`, `DQ-015`, `DQ-016`, `DQ-018` a `DQ-020`):** Se exige el 100% donde una violación es un error imposible o corruptor: una calificación fuera de 1-5, una reseña sin autor o negocio, una clave repetida o una fecha imposible. Las reglas `critical` de este grupo detienen el pipeline.
* **Umbral de 99.0% a 99.9% (`DQ-002`, `DQ-009`, `DQ-010`, `DQ-012` a `DQ-014`):** Se admite un margen mínimo porque el dato se corrige o se marca en el pipeline (duplicados, check-ins repetidos, golden record, fecha de alta) y el residuo no altera los KPIs oficiales.
* **Umbral de 90.0% a 98.0% (`DQ-007`, `DQ-008`, `DQ-011`, `DQ-017`):** Atributos declarados por la fuente que no controlamos; se reportan para seguimiento sin bloquear la carga. En la corrida del 08/11/2026, `DQ-008` (95,04%) y `DQ-011` (96,39%) quedaron bajo su umbral y se reportan al Data Owner.

---

## 7. Requerimientos de Información y KPIs (Entregable E8)

### 7.1. Preguntas de Negocio por Stakeholder (Foco: Producto)

| ID | Pregunta de negocio | Stakeholder que la formula | Decisión que habilita | Frecuencia |
| :--- | :--- | :--- | :--- | :--- |
| **REQ-01** | ¿Cómo evoluciona mensualmente la distribución de calificaciones y el rating promedio por categoría de negocio en cada ciudad? | **Andrew Collins (VP Product)** | Priorizar mejoras en las categorías comerciales con degradación de satisfacción y rediseñar filtros de búsqueda. | Mensual |
| **REQ-02** | ¿Cuál es el top de negocios locales con mayor fidelidad y retención de usuarios activos medidos a través de visitas recurrentes (check-ins)? | **Andrew Collins (VP Product)** | Crear programas de reconocimiento e insignias oficiales de "Comercio Recomendado" en YelpConnect. | Trimestral |
| **REQ-03** | ¿Qué porcentaje de reseñas son descartadas o enviadas a cuarentena por sospecha de duplicidad o fraude y cómo afecta esto a la reputación pública? | **Andrew Collins** / Rafael Nakamura | Ajustar los umbrales del algoritmo de moderación automática sin perjudicar a los comercios honestos. | Semanal |
| **REQ-04** | ¿Cuál es la tasa de retención de cohortes de usuarios según el año de registro (`yelping_since`) y su volumen de interacción anual? | **Andrew Collins (VP Product)** | Evaluar la salud de la comunidad de comensales y el engagement a largo plazo de la plataforma. | Mensual |
| **REQ-05** | ¿Cuál es la discrepancia matemática entre los check-ins contabilizados para cobro de Yelp Ads y las visitas reales estimadas por Producto? | **Andrew Collins** / Carla Romero | Conciliar la política comercial con la fidelidad analítica de retención para evitar litigios comerciales. | Mensual |
| **REQ-06** | ¿Qué comercios presentan un volumen significativo de reseñas negativas asociadas a tiempos de espera o servicio según el análisis textual? | **Andrew Collins (VP Product)** | Diseñar herramientas operativas de asistencia y analítica interna para dueños de negocios afiliados. | Quincenal |

### 7.2. Definición de KPIs

| KPI | Fórmula en lenguaje de negocio | Términos del glosario que usa | Requerimiento que atiende |
| :--- | :--- | :--- | :--- |
| **Rating Promedio Ponderado Bayesiano (BWR)** | $$BWR = rac{v \cdot R + m \cdot C}{v + m}$$<br>Donde $v =$ cantidad de reseñas del negocio, $R =$ promedio aritmético simple, $m = 25$ (umbral mínimo de credibilidad), $C =$ calificación promedio global de la categoría/ciudad. | Calificación, Reseña Válida, Negocio, Rating Bayesiano | **REQ-01** |
| **Tasa de Crecimiento Neto de Reseñas (NRG)** | $$NRG = rac{	ext{Reseñas Válidas Mes Actual} - 	ext{Reseñas Válidas Mes Anterior}}{	ext{Reseñas Válidas Mes Anterior}} 	imes 100$$ | Reseña Verificada, Reseña en Cuarentena | **REQ-01**, **REQ-03** |
| **Índice de Afluencia Homologada (HAI)** | Suma de Check-ins Válidos deduplicados por usuario y ventana móvil de 60 minutos por comercio en el periodo evaluado. | Check-in Válido, Visita Confirmada | **REQ-02**, **REQ-05** |
| **Ratio de Salud de Calidad de Reseñas (RHR)** | $$RHR = rac{	ext{Reseñas que superan 100\% de reglas DQ}}{	ext{Total de Reseñas Ingresadas en Bronze}} 	imes 100$$ | Regla DQ, Cuarentena, Salud de Datos | **REQ-03** |
| **Tasa de Retención de Cohortes (CRR)** | $$CRR_t = rac{	ext{Usuarios de la Cohorte } C_0 	ext{ con } \ge 1 	ext{ interacción en el mes } t}{	ext{Total de usuarios registrados en la Cohorte } C_0} 	imes 100$$ | Usuario Activo, Cohorte, Interacción | **REQ-04** |
| **Ratio de Stickiness (Engagement Diario/Mensual)** | $$	ext{Stickiness} = rac{DAU}{MAU} 	imes 100$$<br>Donde $DAU =$ Usuarios Únicos Activos Diarios, $MAU =$ Usuarios Únicos Activos Mensuales. | Usuario Activo, Interacción, Engagement | **REQ-04** |
| **Índice de Utilidad Comunitaria (CUR)** | $$CUR = rac{\sum (	ext{useful} + 	ext{funny} + 	ext{cool})}{	ext{Total de Reseñas Publicadas}}$$ | Reseña Útil, Voto de Interacción | **REQ-02**, **REQ-06** |

---

## 8. Modelo Operativo y Consideraciones FinOps

### 8.1. Cadencia de Ejecución y Responsables

| Proceso | Frecuencia | Responsable | Qué hacer si falla (Protocolo de Contingencia) |
| :--- | :--- | :--- | :--- |
| **Ingesta Bronze (`01_bronze_ingesta`)** | Diaria, 01:00 UTC | Ingeniero/a de Datos de guardia (Data Platform) | Reintento automático a los 10 min. Si falla tras 2 reintentos, notificación vía webhook a Slack/Teams y ejecución manual prioritaria; Bronze no bloquea las capas superiores del día previo. |
| **Conformación Silver (`02_silver_limpieza`)** | Diaria, 02:00 UTC | Ingeniero/a de Datos de guardia | Comprobación de integridad de esquema en Auto Loader. Revertir a la versión Delta previa mediante Time Travel (`RESTORE`) y re-procesar lote anómalo. |
| **Evaluación de Calidad (`01_reglas_calidad`)** | Diaria, 03:00 UTC | Steward de Producto / Data Platform | Si falla una regla `critical`, el pipeline aborta la publicación a Gold (Quality Gate activo) y envía reporte de anomalías al Data Owner (Andrew Collins). |
| **Publicación Gold (`03_gold_productos_datos`)** | Diaria, 04:00 UTC | Ingeniero/a de Datos de guardia | Validación de reconciliación matemática (la suma de hechos en Gold debe cuadrar al 100% con Silver). Si hay desfase, se cancela la recarga de vistas y se investiga. |
| **Actualización de BI (`01_consultas_base`)** | Diaria, 05:00 UTC | Analista de BI y Valor | Monitoreo de disponibilidad del catálogo. Refresco automático de cachés en Databricks AI/BI Dashboards antes del inicio de la jornada laboral. |

### 8.2. Supuestos y Bitácora de Consumo FinOps

| Fecha | Actividad ejecutada | Consumo observado | Decisión de ahorro tomada |
| :--- | :--- | :--- | :--- |
| **14/09/2026** | Primera corrida completa del pipeline sobre el dataset completo de Yelp (10 GB). | **Crítico:** Se consumió el 90% de la cuota diaria de cómputo serverless en una sola ejecución. | Se acotó la muestra territorial a una única ciudad representativa (< 300 MB) preservando integridad referencial con el script `preparar_muestra_yelp.py`. |
| **21/09/2026** | Ejecución de notebooks con comandos `display()` y `df.count()` repetitivos en Silver. | **Elevado:** Tiempos de espera de clúster prolongados y alto consumo de DBUs en acciones de acción eager. | Se eliminaron todos los `display()` exploratorios del flujo productivo; las validaciones se consolidaron en un único loop de aserciones pushdown. |
| **28/09/2026** | Consulta masiva de tablas Gold con escaneos completos (*Full Table Scans*). | **Moderado:** Lecturas redundantes de particiones completas de Delta Lake. | Se aplicó optimización Delta (`OPTIMIZE yelp_silver.slv_review ZORDER BY (business_id, date)`) reduciendo la lectura de archivos Parquet en un 70%. |
| **04/10/2026** | Corrida del pipeline multi-tarea completo integrado mediante Databricks Workflows. | **Óptimo:** Consumo inferior al 22% de la cuota diaria; tiempo total de corrida de 6.4 minutos. | Configuración de clúster serverless de apagado automático a los 10 minutos de inactividad y políticas de reintentos exponenciales. |

---

## 9. Trazabilidad Documento ↔ Plataforma

| Sección de este documento | Notebook que la implementa | Objetos verificables en el catálogo | Discrepancias y cómo se resolvieron |
| :--- | :--- | :--- | :--- |
| **3. Arquitectura de Referencia** | `E7_pipeline_lakehouse/01` a `04` | Tablas Delta en `workspace.yelp_bronze`, `yelp_silver`, `yelp_gold` | El diseño preveía una arquitectura Lambda; se simplificó a Lakehouse Medallion unificado por eficiencia FinOps. |
| **4. Modelos Conceptual y Lógico** | `E3_modelo_fisico/01_modelo_fisico_ddl` | 10 tablas Silver y 16 Gold con `COMMENT` en el 100% de columnas, PK/FK y `CHECK` | El modelo físico replica `yelp_silver.txt` y `yelp_gold.txt`; las categorías se implementan con tabla puente (ADR-03, v1.3). |
| **5. Estrategia de Gobierno y RACI** | `E6_controles_gobierno/01_controles_unity` | Tags en Unity Catalog (`confidentiality = 'PII'`, `classification`) y permisos RBAC (`GRANT SELECT`) | Se restringió el acceso a `slv_user.name` mediante vistas dinámicas seguras y column masks para analistas de Producto. |
| **6. Reglas de Calidad (DQ)** | `E5_calidad_datos/01_reglas_calidad` | Catálogo `yelp_gov.dq_rules` y bitácora histórica `yelp_gov.dq_results` | Las 20 reglas coinciden entre el código (`rule_id`) y este documento (`DQ-001` a `DQ-020`). |
| **Anexos A y B (Glosario y Diccionario)** | `E4_diccionario_glosario/01` y `02` | Tablas `yelp_gov.business_glossary` y `yelp_gov.data_dictionary` | Los Excel institucionales se cargan desde el volumen `/Volumes/workspace/yelp_gov/artifacts/`. |
| **7. Requerimientos e KPIs** | `E8_dashboard_bi/01_consultas_base` | Tablas `gld_agg_business_bayesian_month`, `gld_agg_user_cohort_month`, `gld_agg_review_health_month` y vista `vw_user_secure` | Las fórmulas de rating bayesiano y retención se validaron contra las definiciones del Glosario de Negocio. |

---

## Anexos

### Anexo A · Glosario de Términos de Negocio
* **Archivo Fuente:** `Glosario_de_terminos_de_negocio_YelpConnect_PLANTILLA.xlsx`
* **Tabla Gobernada:** `workspace.yelp_gov.business_glossary`
* **Resumen de Cobertura:** Contiene 23 términos aprobados cubriendo 5 dominios (*Negocio*, *Usuario*, *Contenido*, *Interacción* y *Gobierno*). Resuelve las disputas de significado del caso:
  1. **Reseña Verificada:** Reseña publicada por un usuario con antigüedad > 30 días, calificación 1-5, texto con longitud > 15 caracteres y que aprueba el 100% de reglas DQ.
  2. **Reseña Duplicada:** Toda reseña secundaria emitida por el mismo usuario sobre el mismo comercio en un intervalo menor a 24 horas.
  3. **Check-in Válido:** Marca de presencia física confirmada deduplicada por ventana de 60 minutos por usuario y negocio.
  4. **Rating Ponderado Bayesiano:** Calificación amortiguada mediante la media global de la ciudad para evitar el sesgo de negocios con muestras mínimas de reseñas.
  5. **Cohorte de Usuario:** Agrupación analítica de usuarios según su mes y año de registro (`yelping_since`) para medir su retención y ciclo de vida en la app.

### Anexo B · Diccionario de Datos
* **Archivo Fuente:** `Diccionario_de_Datos_YelpConnect_PLANTILLA.xlsx`
* **Tabla Gobernada:** `workspace.yelp_gov.data_dictionary`
* **Resumen de Cobertura:** Documenta las 198 columnas físicas de las capas Silver y Gold (100% de cobertura). Cada columna cuenta con descripción, tipo de dato, regla DQ asociada, Data Owner y etiqueta de clasificación de sensibilidad.

### Anexo C · Diagramas en Formato Editable
* `arquitectura_referencia_yelpconnect_v2.drawio` (Vista integral de capas Lakehouse y gobierno vertical, Sección 3.3).
* `modelo_conceptual_entidades_v1.drawio` (Diagrama entidad-relación conceptual de YelpConnect, Sección 4.1).
* `02-Modelado/yelp_modelo_logico.drawio` (Modelo lógico de Silver y modelo dimensional de Gold, Secciones 4.2 y 4.3; detalle de columnas en `documentos/03_modelo_logico_y_dimensional_gold.md`).

### Anexo D · Actas de Decisión del Equipo
* **Acta 01 (19/08/2026): Homologación del Conteo de Check-ins.** 
  * *Disputa:* Operaciones Comerciales proponía contar cada marca temporal cruda; Producto proponía máximo un check-in por día.
  * *Resolución:* Se adoptó la deduplicación por ventana de 60 minutos por usuario y negocio (ADR-01). Preserva la señal de visitas recurrentes legítimas sin duplicar conteos. Decisión aprobada por Carla Romero y Andrew Collins.
* **Acta 02 (02/09/2026): Aislamiento Preventivo de Reseñas Sospechosas.** 
  * *Disputa:* Growth solicitaba mantener todas las reseñas en los paneles para maximizar el volumen visible; Trust & Safety requería eliminar inmediatamente cualquier reseña flagged.
  * *Resolución:* Las reseñas con advertencias de calidad o sospecha de bot se retienen en tablas de cuarentena en Silver para auditoría, computando en métricas de salud internas pero excluyéndose del rating público visible de Producto.
* **Acta 03 (16/09/2026): Tratamiento de Categorías como Array en Silver.** 
  * *Disputa:* El equipo de modelado evaluó normalizar a una tabla puente relacional clásica frente a la recomendación del arquitecto de mantener arreglos en Delta.
  * *Resolución:* Se aprobó el almacenamiento como `ARRAY<STRING>` (ADR-03) al verificarse que ningún requerimiento analítico requería mantener una tabla puente pesada, reduciendo los tiempos de pipeline en un 40%.
* **Acta 04 (30/09/2026): Selección de Esquema Dimensional en Gold para Retención de Producto.**
  * *Disputa:* El equipo de analítica propuso un tablón único desnormalizado (OBT) que simplificara consultas en Power BI pero que triplicaba el tamaño en disco y complicaba el análisis histórico de cohortes.
  * *Resolución:* Se aprobó el modelo Kimball en estrella con dimensiones conformes y tabla de hechos agregados de cohortes (ADR-05), garantizando eficiencia de almacenamiento y flexibilidad analítica para Andrew Collins.
* **Acta 05 (09/10/2026): Tratamiento del Objeto Anidado `business.attributes` y `hours` (Schema-on-Read en Bronze).**
  * *Disputa:* Data Science (Mei Lin) consultó si los 39 subcampos anidados de `business.attributes` debían normalizarse en una tabla clave-valor en Silver (`slv_business_atributo`) o aplanarse en `slv_business`.
  * *Resolución:* Se aprobó mantener `attributes` y `hours` exclusivamente en `workspace.yelp_bronze.brz_business` bajo modalidad *Schema-on-Read* (**ADR-07**), ya que presentan >65% de dispersión en 28 de sus 39 claves, contienen diccionarios Python de segundo nivel no requeridos por los KPIs de Producto (`REQ-01` a `REQ-06`) y su explosión a Silver incrementaría innecesariamente el consumo de DBUs en Databricks Free Edition.
