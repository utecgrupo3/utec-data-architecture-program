# Databricks notebook source
# MAGIC %md
# MAGIC # E3 · Modelo Físico — DDL sobre Delta Lake / Unity Catalog (Grupo 03 · Data Evolution)
# MAGIC **Cierra con:** M4 Modelado Avanzado · Se implementa en `yelp_silver` (conformado 3FN) y `yelp_gold` (dimensional Kimball).
# MAGIC
# MAGIC Reglas del entregable:
# MAGIC 1. **Toda tabla y toda columna llevan `COMMENT`** — ese metadata alimenta E4 (diccionario) automáticamente.
# MAGIC 2. Declarar constraints informativas de UC: `PRIMARY KEY` / `FOREIGN KEY` (no se
# MAGIC    enforcan, pero documentan el modelo, habilitan optimizaciones Catalyst y dibujan el lineage en Catalog Explorer).
# MAGIC 3. Constraints sí enforzadas donde aplique: `NOT NULL` y `CHECK`.
# MAGIC 4. Habilitar `delta.enableChangeDataFeed = true` para auditoría histórica Time Travel (**ADR-04**).
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la **Sección 4 («Modelo de Datos Conceptual y Lógico»)** y las decisiones **ADR-01 a ADR-06** del
# MAGIC > **Documento Formal de Arquitectura de Datos (v1.2 · Equipo Data Evolution)**:
# MAGIC > - **Capa Silver (`workspace.yelp_silver`):** 7 tablas (`slv_business`, `slv_user`, `slv_amistad`, `slv_review`, `slv_review_cuarentena`, `slv_tip`, `slv_checkin`).
# MAGIC > - **Capa Gold (`workspace.yelp_gold`):** Esquema estrella Kimball (`gld_dim_negocio`, `gld_dim_usuario`, `gld_dim_tiempo`, `gld_fact_resenas`) + productos agregados (`gld_kpi_negocio_mensual`, `gld_checkins_diarios`, `gld_salud_resenas`, `gld_cohortes_retencion_mensual`).

# COMMAND ----------

# DBTITLE 1: Asegurar existencia de los schemas Silver y Gold en Unity Catalog
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.yelp_silver COMMENT 'Capa Silver: datos limpios, conformados en 3FN y con calidad validada — Grupo 03'")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.yelp_gold COMMENT 'Capa Gold: modelo dimensional Kimball y productos de datos certificados — Grupo 03'")

# COMMAND ----------

# DBTITLE 1: 1. Entidad Silver — slv_business (Negocios locales + ADR-03 ARRAY<STRING>)
# MAGIC %sql
# MAGIC -- ======================= 1.1 ENTIDAD SILVER: slv_business =======================
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_business (
# MAGIC   business_id   STRING        NOT NULL COMMENT 'Identificador único del negocio en YelpConnect (clave natural de 22 caracteres)',
# MAGIC   nombre        STRING        NOT NULL COMMENT 'Razón comercial o nombre público del establecimiento',
# MAGIC   ciudad        STRING        NOT NULL COMMENT 'Ciudad normalizada (title case, sin espacios extremos)',
# MAGIC   estado        STRING                 COMMENT 'Código de estado o provincia de dos letras en mayúsculas',
# MAGIC   latitud       DOUBLE                 COMMENT 'Coordenada de latitud geográfica (-90 a +90 grados)',
# MAGIC   longitud      DOUBLE                 COMMENT 'Coordenada de longitud geográfica (-180 a +180 grados)',
# MAGIC   estrellas     DOUBLE                 COMMENT 'Rating promedio publicado en la ficha del negocio (escala 1.0 a 5.0)',
# MAGIC   n_resenas     INT                    COMMENT 'Cantidad histórica acumulada de reseñas asociadas al negocio',
# MAGIC   esta_abierto  BOOLEAN                COMMENT 'TRUE si el negocio sigue operativo y abierto al público',
# MAGIC   categorias    ARRAY<STRING>          COMMENT 'Categorías comerciales deserializadas como arreglo nativo Delta según ADR-03',
# MAGIC   fecha_carga   TIMESTAMP     NOT NULL COMMENT 'Timestamp de procesamiento del pipeline en Silver (auditoría técnica)',
# MAGIC   CONSTRAINT pk_slv_business PRIMARY KEY (business_id)
# MAGIC )
# MAGIC COMMENT 'Negocios locales registrados en YelpConnect — capa conformada 3FN. Data Owner: Producto (Andrew Collins).'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Constraint CHECK idempotente sobre slv_business (escala válida 1.0 a 5.0)
# MAGIC ALTER TABLE workspace.yelp_silver.slv_business DROP CONSTRAINT IF EXISTS chk_slv_business_estrellas;
# MAGIC ALTER TABLE workspace.yelp_silver.slv_business ADD CONSTRAINT chk_slv_business_estrellas CHECK (estrellas IS NULL OR (estrellas >= 1.0 AND estrellas <= 5.0));

# COMMAND ----------

# DBTITLE 1: 2. Entidades Silver — slv_user (Usuarios con PII) y slv_amistad (Grafo Social v1.2)
# MAGIC %sql
# MAGIC -- ======================= 1.2 ENTIDAD SILVER: slv_user =======================
# MAGIC -- Nota de Gobierno (E6 / ADR-06): La columna 'name' contiene PII directa y se protege
# MAGIC -- con tag clasificacion='restringido' y Dynamic Column Masking para analistas.
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_user (
# MAGIC   user_id        STRING    NOT NULL COMMENT 'Identificador único seudonimizado del usuario registrado en YelpConnect',
# MAGIC   name           STRING    NOT NULL COMMENT 'Nombre de pila del usuario (Dato Personal PII sujeto a enmascaramiento dinámico ADR-06)',
# MAGIC   review_count   INT                COMMENT 'Cantidad total acumulada de reseñas escritas por el usuario',
# MAGIC   yelping_since  TIMESTAMP NOT NULL COMMENT 'Fecha y hora de alta original del usuario en la plataforma YelpConnect',
# MAGIC   useful         INT                COMMENT 'Total histórico de votos Useful asociados al perfil del usuario',
# MAGIC   funny          INT                COMMENT 'Total histórico de votos Funny asociados al perfil del usuario',
# MAGIC   cool           INT                COMMENT 'Total histórico de votos Cool asociados al perfil del usuario',
# MAGIC   fans           INT                COMMENT 'Número de seguidores activos (fans) del usuario en la comunidad',
# MAGIC   elite          STRING             COMMENT 'Años en los que el usuario fue certificado como miembro de la comunidad Élite',
# MAGIC   average_stars  DOUBLE             COMMENT 'Calificación promedio histórica otorgada por el usuario en sus reseñas (1.0 a 5.0)',
# MAGIC   fecha_carga    TIMESTAMP NOT NULL COMMENT 'Timestamp de procesamiento y carga en la capa Silver (auditoría)',
# MAGIC   CONSTRAINT pk_slv_user PRIMARY KEY (user_id)
# MAGIC )
# MAGIC COMMENT 'Usuarios registrados en YelpConnect — capa conformada 3FN. Data Owner: Trust & Safety (Rafael Nakamura).'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC -- ======================= 1.3 ENTIDAD SILVER: slv_amistad (Grafo Social) =======================
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_amistad (
# MAGIC   user_id_1    STRING    NOT NULL COMMENT 'Identificador del usuario titular dentro del vínculo de amistad en la red social',
# MAGIC   user_id_2    STRING    NOT NULL COMMENT 'Identificador del usuario amigo vinculado en el grafo social de YelpConnect',
# MAGIC   fecha_carga  TIMESTAMP NOT NULL COMMENT 'Timestamp de procesamiento del grafo social en Silver (auditoría)',
# MAGIC   CONSTRAINT pk_slv_amistad PRIMARY KEY (user_id_1, user_id_2),
# MAGIC   CONSTRAINT fk_amistad_user1 FOREIGN KEY (user_id_1) REFERENCES workspace.yelp_silver.slv_user(user_id)
# MAGIC )
# MAGIC COMMENT 'Relación N:M del grafo social de amistad entre usuarios (v1.2 Hackolade). Data Owner: Trust & Safety (Rafael Nakamura).'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# DBTITLE 1: 3. Entidades Silver — slv_review (Reseñas Verificadas) y slv_review_cuarentena (Aislamiento DQ)
# MAGIC %sql
# MAGIC -- ======================= 1.4 ENTIDAD SILVER: slv_review =======================
# MAGIC -- Tratamiento de 'text' (PII indirecta): el comentario libre puede contener nombres o datos de terceros;
# MAGIC -- se clasifica como Confidencial en E4/E6 y en Gold solo se exponen métricas agregadas o longitud (text_len).
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_review (
# MAGIC   review_id     STRING    NOT NULL COMMENT 'Identificador único global de la reseña emitida en YelpConnect',
# MAGIC   user_id       STRING    NOT NULL COMMENT 'Identificador del usuario autor de la reseña (FK hacia slv_user)',
# MAGIC   business_id   STRING    NOT NULL COMMENT 'Identificador del negocio evaluado en la reseña (FK hacia slv_business)',
# MAGIC   stars         INT       NOT NULL COMMENT 'Calificación numérica en estrellas otorgada al negocio (escala entera 1 a 5)',
# MAGIC   useful        INT                COMMENT 'Conteo de votos Useful recibidos por la reseña de parte de la comunidad',
# MAGIC   funny         INT                COMMENT 'Conteo de votos Funny recibidos por la reseña',
# MAGIC   cool          INT                COMMENT 'Conteo de votos Cool recibidos por la reseña',
# MAGIC   text          STRING    NOT NULL COMMENT 'Texto argumentativo de la reseña (PII indirecta clasificada como Confidencial)',
# MAGIC   fecha_resena  TIMESTAMP NOT NULL COMMENT 'Fecha y hora exacta de publicación de la reseña convertida a TIMESTAMP',
# MAGIC   fecha_carga   TIMESTAMP NOT NULL COMMENT 'Timestamp de procesamiento y validación en la capa Silver (auditoría)',
# MAGIC   CONSTRAINT pk_slv_review PRIMARY KEY (review_id),
# MAGIC   CONSTRAINT fk_review_business FOREIGN KEY (business_id) REFERENCES workspace.yelp_silver.slv_business(business_id),
# MAGIC   CONSTRAINT fk_review_user FOREIGN KEY (user_id) REFERENCES workspace.yelp_silver.slv_user(user_id)
# MAGIC )
# MAGIC COMMENT 'Reseñas verificadas y deduplicadas que superaron el Quality Gate en Silver. Data Owner: Producto (Andrew Collins).'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC -- ======================= 1.5 ENTIDAD SILVER: slv_review_cuarentena =======================
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_review_cuarentena (
# MAGIC   review_id       STRING             COMMENT 'Identificador de la reseña aislada preventivamente en cuarentena',
# MAGIC   user_id         STRING             COMMENT 'Identificador del usuario reportado en el registro no conforme',
# MAGIC   business_id     STRING             COMMENT 'Identificador del negocio reportado en el registro no conforme',
# MAGIC   stars           INT                COMMENT 'Calificación original del registro aislado en cuarentena',
# MAGIC   text            STRING             COMMENT 'Texto original de la reseña retenida para auditoría forense de Trust & Safety',
# MAGIC   fecha_resena    TIMESTAMP          COMMENT 'Fecha original reportada en la reseña aislada',
# MAGIC   motivo_rechazo  STRING    NOT NULL COMMENT 'Regla DQ o causa de rechazo (FK_INVALIDA, STARS_FUERA_RANGO, DUPLICADO_LOGICO, TEXTO_VACIO)',
# MAGIC   fecha_rechazo   TIMESTAMP NOT NULL COMMENT 'Timestamp exacto en que el Quality Gate aisló el registro en cuarentena'
# MAGIC )
# MAGIC COMMENT 'Zona de cuarentena explícita en Silver para reseñas que incumplen reglas DQ o integridad referencial (Acta 02). Data Owner: Trust & Safety (Rafael Nakamura).'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Constraint CHECK enforzada sobre slv_review.stars (DQ-001: estrictamente entre 1 y 5)
# MAGIC ALTER TABLE workspace.yelp_silver.slv_review DROP CONSTRAINT IF EXISTS chk_slv_review_stars;
# MAGIC ALTER TABLE workspace.yelp_silver.slv_review ADD CONSTRAINT chk_slv_review_stars CHECK (stars BETWEEN 1 AND 5);

# COMMAND ----------

# DBTITLE 1: 4. Entidades Silver — slv_tip (Consejos breves) y slv_checkin (Visitas a grano evento ADR-01)
# MAGIC %sql
# MAGIC -- ======================= 1.6 ENTIDAD SILVER: slv_tip =======================
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_tip (
# MAGIC   tip_id            STRING    NOT NULL COMMENT 'Identificador único determinístico (hash SHA-256) del consejo breve',
# MAGIC   user_id           STRING    NOT NULL COMMENT 'Identificador del usuario autor del tip (FK hacia slv_user)',
# MAGIC   business_id       STRING    NOT NULL COMMENT 'Identificador del negocio sobre el cual se publica el tip (FK hacia slv_business)',
# MAGIC   text              STRING    NOT NULL COMMENT 'Texto breve del consejo o recomendación publicada por el usuario',
# MAGIC   fecha_tip         TIMESTAMP NOT NULL COMMENT 'Marca temporal de publicación del consejo breve',
# MAGIC   compliment_count  INT                COMMENT 'Cantidad de cumplidos o agradecimientos recibidos por el consejo',
# MAGIC   fecha_carga       TIMESTAMP NOT NULL COMMENT 'Timestamp de procesamiento y carga en la capa Silver (auditoría)',
# MAGIC   CONSTRAINT pk_slv_tip PRIMARY KEY (tip_id),
# MAGIC   CONSTRAINT fk_tip_business FOREIGN KEY (business_id) REFERENCES workspace.yelp_silver.slv_business(business_id),
# MAGIC   CONSTRAINT fk_tip_user FOREIGN KEY (user_id) REFERENCES workspace.yelp_silver.slv_user(user_id)
# MAGIC )
# MAGIC COMMENT 'Consejos breves (tips) conformados en Silver. Data Owner: Producto (Andrew Collins).'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC -- ======================= 1.7 ENTIDAD SILVER: slv_checkin =======================
# MAGIC -- Decisión de diseño (ADR-01): el campo 'date' crudo (string de fechas separadas por coma)
# MAGIC -- se explota a grano evento (1 fila = 1 check-in válido deduplicado por ventana horaria).
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_checkin (
# MAGIC   checkin_id     STRING    NOT NULL COMMENT 'Identificador único determinístico (hash SHA-256) del evento de check-in válido',
# MAGIC   business_id    STRING    NOT NULL COMMENT 'Identificador del establecimiento comercial visitado (FK hacia slv_business)',
# MAGIC   fecha_checkin  TIMESTAMP NOT NULL COMMENT 'Marca temporal individual de la visita tras explotar y deduplicar por ventana horaria (ADR-01)',
# MAGIC   fecha_carga    TIMESTAMP NOT NULL COMMENT 'Timestamp de procesamiento y carga en la capa Silver (auditoría)',
# MAGIC   CONSTRAINT pk_slv_checkin PRIMARY KEY (checkin_id),
# MAGIC   CONSTRAINT fk_checkin_business FOREIGN KEY (business_id) REFERENCES workspace.yelp_silver.slv_business(business_id)
# MAGIC )
# MAGIC COMMENT 'Eventos individuales de check-in explotados y deduplicados por ventana horaria (ADR-01). Data Owner: Operaciones Comerciales (Carla Romero).'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# DBTITLE 1: 5. Capa Gold Dimensional (Esquema Estrella Kimball — ADR-05)
# MAGIC %sql
# MAGIC -- ======================= 2.1 DIMENSIÓN GOLD: gld_dim_negocio (SCD Tipo 1 y 2) =======================
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_dim_negocio (
# MAGIC   business_sk            STRING        NOT NULL COMMENT 'Clave subrogada (surrogate key) de la dimensión negocio',
# MAGIC   business_id            STRING        NOT NULL COMMENT 'Clave natural del establecimiento comercial en YelpConnect',
# MAGIC   nombre                 STRING        NOT NULL COMMENT 'Nombre comercial del establecimiento (SCD Tipo 1)',
# MAGIC   ciudad                 STRING        NOT NULL COMMENT 'Ciudad normalizada del establecimiento comercial',
# MAGIC   estado                 STRING                 COMMENT 'Código de estado o provincia de dos letras',
# MAGIC   categoria_principal    STRING                 COMMENT 'Categoría comercial primaria extraída del primer elemento del arreglo para BI directo',
# MAGIC   categorias             ARRAY<STRING>          COMMENT 'Arreglo completo de categorías comerciales del negocio (ADR-03)',
# MAGIC   esta_abierto           BOOLEAN                COMMENT 'Estado operativo del negocio (auditado con historia SCD Tipo 2)',
# MAGIC   fecha_inicio_vigencia  TIMESTAMP     NOT NULL COMMENT 'Fecha de inicio de vigencia del registro dimensional (SCD Tipo 2)',
# MAGIC   fecha_fin_vigencia     TIMESTAMP              COMMENT 'Fecha de fin de vigencia del registro dimensional (NULL si es la versión actual)',
# MAGIC   es_actual              BOOLEAN       NOT NULL COMMENT 'TRUE si el registro corresponde a la versión vigente actual del negocio',
# MAGIC   CONSTRAINT pk_gld_dim_negocio PRIMARY KEY (business_sk)
# MAGIC )
# MAGIC COMMENT 'Dimensión conforme de Negocios con soporte híbrido SCD Tipo 1 y Tipo 2. Consumidor: Producto y Data Science.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC -- ======================= 2.2 DIMENSIÓN GOLD: gld_dim_usuario (PII Enmascarada + SCD) =======================
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_dim_usuario (
# MAGIC   user_sk        STRING    NOT NULL COMMENT 'Clave subrogada (surrogate key) de la dimensión usuario',
# MAGIC   user_id        STRING    NOT NULL COMMENT 'Clave natural seudonimizada del usuario en YelpConnect',
# MAGIC   name_masked    STRING    NOT NULL COMMENT 'Nombre del usuario anonimizado/enmascarado para consumo analítico seguro (ADR-06)',
# MAGIC   yelping_since  TIMESTAMP NOT NULL COMMENT 'Fecha de registro original del comensal para análisis de antigüedad y cohortes',
# MAGIC   es_elite       BOOLEAN   NOT NULL COMMENT 'TRUE si el usuario cuenta con distinción activa en la comunidad Élite (SCD Tipo 2)',
# MAGIC   average_stars  DOUBLE             COMMENT 'Calificación promedio histórica otorgada por el comensal',
# MAGIC   es_actual      BOOLEAN   NOT NULL COMMENT 'TRUE si representa la versión vigente del perfil en la dimensión',
# MAGIC   CONSTRAINT pk_gld_dim_usuario PRIMARY KEY (user_sk)
# MAGIC )
# MAGIC COMMENT 'Dimensión conforme de Usuarios con privacidad por diseño (nombre enmascarado). Consumidor: Producto y Data Science.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC -- ======================= 2.3 DIMENSIÓN GOLD: gld_dim_tiempo =======================
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_dim_tiempo (
# MAGIC   date_sk      INT     NOT NULL COMMENT 'Clave subrogada entera de fecha en formato YYYYMMDD',
# MAGIC   fecha        DATE    NOT NULL COMMENT 'Fecha calendario en formato estándar YYYY-MM-DD',
# MAGIC   anio         INT     NOT NULL COMMENT 'Año calendario de cuatro dígitos (ej. 2026)',
# MAGIC   trimestre    INT     NOT NULL COMMENT 'Trimestre calendario del año (1 a 4)',
# MAGIC   mes          INT     NOT NULL COMMENT 'Mes calendario del año (1 a 12)',
# MAGIC   nombre_mes   STRING  NOT NULL COMMENT 'Nombre del mes en español para reportes ejecutivos',
# MAGIC   dia_semana   STRING  NOT NULL COMMENT 'Nombre del día de la semana para análisis de estacionalidad intra-semanal',
# MAGIC   CONSTRAINT pk_gld_dim_tiempo PRIMARY KEY (date_sk)
# MAGIC )
# MAGIC COMMENT 'Dimensión calendario generada programáticamente para el esquema estrella Kimball. Consumidor: Transversal.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC -- ======================= 2.4 TABLA DE HECHOS ATÓMICA: gld_fact_resenas =======================
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_fact_resenas (
# MAGIC   review_sk    STRING  NOT NULL COMMENT 'Clave subrogada del hecho atómico de reseña',
# MAGIC   review_id    STRING  NOT NULL COMMENT 'Clave degenerada con el identificador original de la reseña verificada',
# MAGIC   business_sk  STRING  NOT NULL COMMENT 'Clave foránea hacia la dimensión de negocios (gld_dim_negocio)',
# MAGIC   user_sk      STRING  NOT NULL COMMENT 'Clave foránea hacia la dimensión de usuarios (gld_dim_usuario)',
# MAGIC   date_sk      INT     NOT NULL COMMENT 'Clave foránea hacia la dimensión de tiempo (gld_dim_tiempo)',
# MAGIC   stars        INT     NOT NULL COMMENT 'Métrica aditiva/semi-aditiva de calificación en estrellas (1 a 5)',
# MAGIC   text_len     INT     NOT NULL COMMENT 'Longitud en caracteres del comentario de la reseña (sin exponer PII indirecta)',
# MAGIC   useful       INT     NOT NULL COMMENT 'Cantidad de votos de utilidad comunitaria recibidos por la reseña',
# MAGIC   CONSTRAINT pk_gld_fact_resenas PRIMARY KEY (review_sk),
# MAGIC   CONSTRAINT fk_fact_negocio FOREIGN KEY (business_sk) REFERENCES workspace.yelp_gold.gld_dim_negocio(business_sk),
# MAGIC   CONSTRAINT fk_fact_usuario FOREIGN KEY (user_sk) REFERENCES workspace.yelp_gold.gld_dim_usuario(user_sk),
# MAGIC   CONSTRAINT fk_fact_tiempo  FOREIGN KEY (date_sk) REFERENCES workspace.yelp_gold.gld_dim_tiempo(date_sk)
# MAGIC )
# MAGIC COMMENT 'Tabla de hechos transaccional atómica (grano: 1 fila por reseña verificada). Consumidor: Producto y Data Science (Mei Lin).'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# DBTITLE 1: 6. Capa Gold — Tablas de KPIs y Productos Agregados por Stakeholder (Sección 7 Doc Formal)
# MAGIC %sql
# MAGIC -- ======================= 2.5 PRODUCTO GOLD: gld_kpi_negocio_mensual =======================
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_kpi_negocio_mensual (
# MAGIC   business_id       STRING    NOT NULL COMMENT 'Identificador único del establecimiento comercial evaluado',
# MAGIC   nombre            STRING    NOT NULL COMMENT 'Nombre comercial del negocio para visualización en el dashboard E8',
# MAGIC   ciudad            STRING    NOT NULL COMMENT 'Ciudad normalizada del establecimiento comercial',
# MAGIC   mes               TIMESTAMP NOT NULL COMMENT 'Mes calendario truncado de evaluación del KPI (YYYY-MM-01)',
# MAGIC   n_resenas         BIGINT    NOT NULL COMMENT 'Volumen mensual de reseñas verificadas recibidas por el negocio',
# MAGIC   rating_promedio   DOUBLE    NOT NULL COMMENT 'Promedio aritmético simple de calificaciones del negocio en el mes',
# MAGIC   rating_bayesiano  DOUBLE             COMMENT 'Rating Promedio Ponderado Bayesiano (BWR con m=25 reseñas según ADR-02)',
# MAGIC   usuarios_unicos   BIGINT    NOT NULL COMMENT 'Cantidad de comensales únicos que reseñaron el negocio en el mes',
# MAGIC   CONSTRAINT pk_gld_kpi_negocio_mensual PRIMARY KEY (business_id, mes)
# MAGIC )
# MAGIC COMMENT 'KPIs mensuales de satisfacción y volumen por negocio. Stakeholder: Producto (Andrew Collins — REQ-01).'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC -- ======================= 2.6 PRODUCTO GOLD: gld_checkins_diarios =======================
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_checkins_diarios (
# MAGIC   business_id       STRING  NOT NULL COMMENT 'Identificador del establecimiento comercial visitado',
# MAGIC   fecha             DATE    NOT NULL COMMENT 'Fecha calendario de las visitas presenciales confirmadas',
# MAGIC   checkins_validos  BIGINT  NOT NULL COMMENT 'Conteo oficial homologado de check-ins válidos deduplicados por hora (HAI - ADR-01)',
# MAGIC   CONSTRAINT pk_gld_checkins_diarios PRIMARY KEY (business_id, fecha)
# MAGIC )
# MAGIC COMMENT 'Conteo oficial diario de check-ins válidos para liquidación publicitaria y fidelidad. Stakeholder: Operaciones Comerciales (Carla Romero — REQ-02, REQ-05).'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC -- ======================= 2.7 PRODUCTO GOLD: gld_salud_resenas =======================
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_salud_resenas (
# MAGIC   mes                  TIMESTAMP NOT NULL COMMENT 'Mes calendario de auditoría de calidad y moderación de reseñas',
# MAGIC   resenas_verificadas  BIGINT    NOT NULL COMMENT 'Volumen mensual de reseñas que aprobaron el 100% de reglas DQ críticas',
# MAGIC   resenas_cuarentena   BIGINT    NOT NULL COMMENT 'Volumen mensual de reseñas aisladas preventivamente en slv_review_cuarentena',
# MAGIC   pct_cuarentena       DOUBLE    NOT NULL COMMENT 'Porcentaje de reseñas en cuarentena sobre el total bruto del mes (complemento RHR)',
# MAGIC   CONSTRAINT pk_gld_salud_resenas PRIMARY KEY (mes)
# MAGIC )
# MAGIC COMMENT 'Métricas mensuales de salud de calidad de reseñas y tasa de cuarentena. Stakeholder: Trust & Safety (Rafael Nakamura — REQ-03).'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC -- ======================= 2.8 PRODUCTO GOLD: gld_cohortes_retencion_mensual =======================
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_cohortes_retencion_mensual (
# MAGIC   cohorte_mes       DATE    NOT NULL COMMENT 'Mes calendario de registro original del usuario (cohorte de adquisición)',
# MAGIC   mes_actividad     DATE    NOT NULL COMMENT 'Mes calendario en el que se registra la interacción recurrente de la cohorte',
# MAGIC   usuarios_activos  BIGINT  NOT NULL COMMENT 'Cantidad de usuarios únicos de la cohorte con actividad validada en el mes',
# MAGIC   retencion_pct     DOUBLE  NOT NULL COMMENT 'Tasa de Retención de Cohortes (CRR %) respecto al tamaño inicial de la camada',
# MAGIC   CONSTRAINT pk_gld_cohortes_retencion PRIMARY KEY (cohorte_mes, mes_actividad)
# MAGIC )
# MAGIC COMMENT 'Matriz mensual de retención de cohortes de usuarios (CRR). Stakeholder: Producto (Andrew Collins — REQ-04).'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# DBTITLE 1: Validación automática del entregable (correr antes del PR)
tablas = spark.sql("SHOW TABLES IN workspace.yelp_silver").collect()
assert len(tablas) >= 5, f"Deben existir al menos las 5 entidades en silver (hay {len(tablas)})"

tablas_gold = spark.sql("SHOW TABLES IN workspace.yelp_gold").collect()
assert len(tablas_gold) >= 5, f"Deben existir al menos 5 tablas en gold (hay {len(tablas_gold)})"

sin_comment = spark.sql("""
  SELECT table_name, column_name
  FROM workspace.information_schema.columns
  WHERE table_schema IN ('yelp_silver','yelp_gold')
    AND (comment IS NULL OR comment = '')
""").collect()
assert not sin_comment, f"❌ Columnas sin COMMENT (rompe E4): {[(r.table_name, r.column_name) for r in sin_comment]}"
print(f"✔ Modelo físico validado: {len(tablas)} tablas en Silver · {len(tablas_gold)} tablas en Gold · 100% de columnas documentadas con COMMENT")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Decisiones de Implementación del Modelo Físico (Trazabilidad con Sección 4 del Documento Formal)
# MAGIC
# MAGIC 1. **Categorías como `ARRAY<STRING>` nativo en Silver (`slv_business.categorias` — ADR-03):**
# MAGIC    * Se descartó materializar una tabla puente relacional `slv_negocio_categoria` en Silver para evitar costosos *shuffle joins* que consumirían la cuota diaria de cómputo Serverless en Databricks Free Edition. En `gld_dim_negocio` se expone además `categoria_principal STRING` para facilitar agrupaciones directas en BI.
# MAGIC 2. **Explotación y Deduplicación Horaria de Check-ins (`slv_checkin` — ADR-01):**
# MAGIC    * El archivo crudo `checkin.json` entrega todas las fechas concatenadas en un único `STRING` por negocio. En `slv_checkin` se modela a **grano evento individual** (`checkin_id`, `business_id`, `fecha_checkin`) deduplicado por ventana de 60 minutos, conciliando la disputa entre Operaciones Comerciales y Producto.
# MAGIC 3. **Privacidad por Diseño y Aislamiento Preventivo (`slv_user`, `slv_review`, `slv_review_cuarentena` — ADR-06 y Acta 02):**
# MAGIC    * `slv_user.name` (PII directa) y `slv_review.text` (PII indirecta) quedan gobernados en Silver; en `gld_dim_usuario` solo se expone `name_masked` y en `gld_fact_resenas` únicamente la longitud `text_len`.
# MAGIC    * Las reseñas que violan integridad referencial (`fk_review_business`, `fk_review_user`) o rango de estrellas (`chk_slv_review_stars`) no se destruyen: se derivan a `slv_review_cuarentena` con `motivo_rechazo` y `fecha_rechazo`.
# MAGIC 4. **Grafo Social en Silver (`slv_amistad` — v1.2 Hackolade):**
# MAGIC    * Se incorpora la relación N:M `slv_amistad (user_id_1, user_id_2)` con clave primaria compuesta y clave foránea hacia `slv_user`, habilitando análisis de conectividad social y detección de fraude coordinado.
# MAGIC
# MAGIC ---
# MAGIC ### Definition of Done (E3)
# MAGIC - [x] DDL completo de silver (7 tablas: 5 entidades core + `slv_amistad` + `slv_review_cuarentena`) y gold (esquema estrella Kimball de 4 tablas + 4 tablas agregadas de KPIs).
# MAGIC - [x] 100% de columnas con `COMMENT` (la celda de validación pasa con 0 columnas sin documentar).
# MAGIC - [x] `PRIMARY KEY` / `FOREIGN KEY` informativas declaradas y `CHECK` enforzadas donde el negocio lo exige (`estrellas` y `stars` entre 1 y 5).
# MAGIC - [x] `TBLPROPERTIES (delta.enableChangeDataFeed = true)` activo en todas las tablas según **ADR-04**.
