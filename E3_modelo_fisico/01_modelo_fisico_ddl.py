# Databricks notebook source
# MAGIC %md
# MAGIC # E3 · Modelo Físico — DDL sobre Delta Lake / Unity Catalog (Grupo 03 · Data Evolution)
# MAGIC **Cierra con:** M4 Modelado Avanzado · Se implementa en `yelp_silver` (modelo lógico normalizado) y `yelp_gold` (modelo dimensional).
# MAGIC
# MAGIC Este notebook es la **única fuente del DDL** de Silver y Gold. Reproduce los scripts `yelp_silver.txt` y `yelp_gold.txt` y el diagrama `yelp_modelo_logico.drawio` (carpeta `02-Modelado`).
# MAGIC Los notebooks del pipeline (`E7_pipeline_lakehouse`) lo ejecutan al inicio y luego solo cargan datos, sin redefinir tablas.
# MAGIC
# MAGIC Reglas del entregable:
# MAGIC 1. **Nomenclatura en inglés:** snake_case, prefijo de capa (`slv_`, `gld_dim_`, `gld_fact_`, `gld_bridge_`, `gld_agg_`), tablas en singular, `<entidad>_id` en Silver y `<entidad>_key` en Gold, booleanos `is_*`, contadores `*_count`.
# MAGIC 2. **Toda tabla y toda columna llevan `COMMENT`** en español (lenguaje del glosario de negocio); ese metadata alimenta E4.
# MAGIC 3. Constraints informativas de Unity Catalog: `PRIMARY KEY` / `FOREIGN KEY`. Constraints enforzadas: `NOT NULL` y `CHECK`.
# MAGIC 4. `delta.enableChangeDataFeed = true` en todas las tablas para auditoría histórica.
# MAGIC
# MAGIC **Silver (`workspace.yelp_silver`, 10 tablas):** `slv_business`, `slv_user`, `slv_review`, `slv_review_quarantine`, `slv_category`, `slv_business_category`, `slv_checkin`, `slv_tip`, `slv_photo`, `slv_friendship`.
# MAGIC
# MAGIC **Gold (`workspace.yelp_gold`, 16 tablas):** dimensiones `gld_dim_date`, `gld_dim_business`, `gld_dim_user`, `gld_dim_category`; puente `gld_bridge_business_category`; hechos `gld_fact_review`, `gld_fact_checkin`, `gld_fact_tip`; agregados `gld_agg_business_month`, `gld_agg_business_week`, `gld_agg_category_month`, `gld_agg_user_month`, `gld_agg_business_bayesian_month`, `gld_agg_business_checkin_day`, `gld_agg_review_health_month`, `gld_agg_user_cohort_month`.

# COMMAND ----------

# DBTITLE 1,Esquemas Silver y Gold
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.yelp_silver COMMENT 'Capa Silver: modelo lógico normalizado, datos limpios y con calidad validada — Grupo 03'")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.yelp_gold COMMENT 'Capa Gold: modelo dimensional y productos de datos certificados — Grupo 03'")

# COMMAND ----------

# DBTITLE 1,1. Silver — negocios, categorías y usuarios
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_business (
# MAGIC   business_id         STRING        NOT NULL COMMENT 'Identificador único del negocio asignado por Yelp',
# MAGIC   master_business_id  STRING        NOT NULL COMMENT 'Negocio maestro (golden record): business_id del registro sobreviviente entre los que representan el mismo negocio físico. Es el mismo business_id si el negocio no tiene duplicados',
# MAGIC   name                STRING        NOT NULL COMMENT 'Nombre comercial del negocio',
# MAGIC   address             STRING                 COMMENT 'Dirección postal del negocio',
# MAGIC   city                STRING                 COMMENT 'Ciudad donde se ubica el negocio',
# MAGIC   state               STRING                 COMMENT 'Estado donde se ubica el negocio (código de 2 letras)',
# MAGIC   postal_code         STRING                 COMMENT 'Código postal del negocio',
# MAGIC   latitude            DECIMAL(9,6)           COMMENT 'Latitud geográfica del negocio',
# MAGIC   longitude           DECIMAL(9,6)           COMMENT 'Longitud geográfica del negocio',
# MAGIC   stars               DECIMAL(2,1)           COMMENT 'Calificación promedio declarada por Yelp, de 1 a 5 en pasos de media estrella',
# MAGIC   review_count        INT                    COMMENT 'Cantidad total de reseñas declarada por Yelp para el negocio',
# MAGIC   is_open             BOOLEAN                COMMENT 'Indica si el negocio sigue abierto (true) o cerró (false)',
# MAGIC   loaded_at           TIMESTAMP     NOT NULL COMMENT 'Momento en que la fila se cargó desde Bronze',
# MAGIC   source_file         STRING                 COMMENT 'Archivo de Bronze del que proviene la fila',
# MAGIC   CONSTRAINT pk_slv_business PRIMARY KEY (business_id)
# MAGIC )
# MAGIC COMMENT 'Maestro de negocios de Philadelphia, tomado del Yelp Open Dataset. Una fila por negocio.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_category (
# MAGIC   category_id  BIGINT     NOT NULL COMMENT 'Identificador único de la categoría, generado en el proceso de carga',
# MAGIC   name         STRING     NOT NULL COMMENT 'Nombre de la categoría tal como aparece en Yelp',
# MAGIC   loaded_at    TIMESTAMP  NOT NULL COMMENT 'Momento en que la fila se cargó desde Bronze',
# MAGIC   source_file  STRING              COMMENT 'Archivo de Bronze del que proviene la fila',
# MAGIC   CONSTRAINT pk_slv_category PRIMARY KEY (category_id)
# MAGIC )
# MAGIC COMMENT 'Catálogo de categorías de negocio de Yelp (por ejemplo, restaurantes o salones de belleza). Una fila por categoría.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_business_category (
# MAGIC   business_id  STRING     NOT NULL COMMENT 'Negocio asociado a la categoría',
# MAGIC   category_id  BIGINT     NOT NULL COMMENT 'Categoría asignada al negocio',
# MAGIC   loaded_at    TIMESTAMP  NOT NULL COMMENT 'Momento en que la fila se cargó desde Bronze',
# MAGIC   source_file  STRING              COMMENT 'Archivo de Bronze del que proviene la fila',
# MAGIC   CONSTRAINT pk_slv_business_category PRIMARY KEY (business_id, category_id)
# MAGIC )
# MAGIC COMMENT 'Tabla puente que asocia cada negocio con sus categorías. Una fila por par negocio-categoría.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC -- slv_user.name es PII directa: se protege con tag y COLUMN MASK en E6.
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_user (
# MAGIC   user_id           STRING        NOT NULL COMMENT 'Identificador único del usuario asignado por Yelp',
# MAGIC   name              STRING        NOT NULL COMMENT 'Nombre de pila del usuario (dato personal, enmascarado en E6)',
# MAGIC   review_count      INT                    COMMENT 'Cantidad total de reseñas escritas por el usuario',
# MAGIC   yelping_since     DATE                   COMMENT 'Fecha en que el usuario se unió a Yelp',
# MAGIC   fans              INT                    COMMENT 'Cantidad de seguidores (fans) del usuario',
# MAGIC   average_stars     DECIMAL(3,2)           COMMENT 'Calificación promedio que el usuario da en sus reseñas',
# MAGIC   elite_year_count  INT                    COMMENT 'Cantidad de años en que Yelp reconoció al usuario como Elite (0 si nunca lo fue)',
# MAGIC   useful_sent       INT                    COMMENT 'Votos "útil" que el usuario ha dado a reseñas de otros',
# MAGIC   funny_sent        INT                    COMMENT 'Votos "divertido" que el usuario ha dado a reseñas de otros',
# MAGIC   cool_sent         INT                    COMMENT 'Votos "genial" que el usuario ha dado a reseñas de otros',
# MAGIC   loaded_at         TIMESTAMP     NOT NULL COMMENT 'Momento en que la fila se cargó desde Bronze',
# MAGIC   source_file       STRING                 COMMENT 'Archivo de Bronze del que proviene la fila',
# MAGIC   CONSTRAINT pk_slv_user PRIMARY KEY (user_id)
# MAGIC )
# MAGIC COMMENT 'Maestro de usuarios de Yelp que escribieron reseñas o tips en la muestra de Philadelphia. Una fila por usuario.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_friendship (
# MAGIC   user_id_1    STRING     NOT NULL COMMENT 'Usuario de la amistad con el identificador menor',
# MAGIC   user_id_2    STRING     NOT NULL COMMENT 'Usuario de la amistad con el identificador mayor',
# MAGIC   loaded_at    TIMESTAMP  NOT NULL COMMENT 'Momento en que la fila se cargó desde Bronze',
# MAGIC   source_file  STRING              COMMENT 'Archivo de Bronze del que proviene la fila',
# MAGIC   CONSTRAINT pk_slv_friendship PRIMARY KEY (user_id_1, user_id_2)
# MAGIC )
# MAGIC COMMENT 'Tabla puente que registra las amistades entre usuarios. Una fila por amistad (el par se guarda una sola vez, con user_id_1 < user_id_2).'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# DBTITLE 1,2. Silver — reseñas, cuarentena, tips, check-ins y fotos
# MAGIC %sql
# MAGIC -- slv_review.text puede contener datos de terceros (PII indirecta): en Gold no se publica el texto.
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_review (
# MAGIC   review_id    STRING     NOT NULL COMMENT 'Identificador único de la reseña asignado por Yelp',
# MAGIC   user_id      STRING     NOT NULL COMMENT 'Usuario que escribió la reseña',
# MAGIC   business_id  STRING     NOT NULL COMMENT 'Negocio sobre el que trata la reseña',
# MAGIC   stars        INT        NOT NULL COMMENT 'Calificación dada en la reseña, de 1 a 5 estrellas',
# MAGIC   review_date  DATE       NOT NULL COMMENT 'Fecha en que se publicó la reseña',
# MAGIC   text         STRING              COMMENT 'Texto completo de la reseña',
# MAGIC   useful       INT                 COMMENT 'Votos "útil" recibidos por la reseña',
# MAGIC   funny        INT                 COMMENT 'Votos "divertido" recibidos por la reseña',
# MAGIC   cool         INT                 COMMENT 'Votos "genial" recibidos por la reseña',
# MAGIC   loaded_at    TIMESTAMP  NOT NULL COMMENT 'Momento en que la fila se cargó desde Bronze',
# MAGIC   source_file  STRING              COMMENT 'Archivo de Bronze del que proviene la fila',
# MAGIC   CONSTRAINT pk_slv_review PRIMARY KEY (review_id)
# MAGIC )
# MAGIC COMMENT 'Reseñas escritas por usuarios sobre negocios de Philadelphia. Una fila por reseña.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_review_quarantine (
# MAGIC   review_id         STRING              COMMENT 'Identificador de la reseña rechazada',
# MAGIC   user_id           STRING              COMMENT 'Usuario reportado en la reseña rechazada',
# MAGIC   business_id       STRING              COMMENT 'Negocio reportado en la reseña rechazada',
# MAGIC   stars             INT                 COMMENT 'Calificación original de la reseña rechazada',
# MAGIC   review_date       DATE                COMMENT 'Fecha original de la reseña rechazada',
# MAGIC   text              STRING              COMMENT 'Texto original de la reseña, retenido para auditoría de Trust & Safety',
# MAGIC   rejection_reason  STRING     NOT NULL COMMENT 'Regla de calidad incumplida (por ejemplo, autor inexistente o estrellas fuera de rango)',
# MAGIC   rejected_at       TIMESTAMP  NOT NULL COMMENT 'Momento en que el pipeline rechazó la reseña',
# MAGIC   source_file       STRING              COMMENT 'Archivo de Bronze del que proviene la fila'
# MAGIC )
# MAGIC COMMENT 'Reseñas de Bronze que no pasan a slv_review por incumplir una regla de calidad. Una fila por reseña rechazada. Las reseñas duplicadas no se rechazan: quedan en slv_review y Gold las marca como no válidas.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_tip (
# MAGIC   tip_id            STRING     NOT NULL COMMENT 'Identificador determinístico del tip: hash de user_id, business_id, tip_date y text',
# MAGIC   user_id           STRING     NOT NULL COMMENT 'Usuario que escribió el tip',
# MAGIC   business_id       STRING     NOT NULL COMMENT 'Negocio sobre el que trata el tip',
# MAGIC   text              STRING              COMMENT 'Texto del tip',
# MAGIC   tip_date          DATE       NOT NULL COMMENT 'Fecha en que se escribió el tip',
# MAGIC   compliment_count  INT                 COMMENT 'Cantidad de cumplidos recibidos por el tip',
# MAGIC   loaded_at         TIMESTAMP  NOT NULL COMMENT 'Momento en que la fila se cargó desde Bronze',
# MAGIC   source_file       STRING              COMMENT 'Archivo de Bronze del que proviene la fila',
# MAGIC   CONSTRAINT pk_slv_tip PRIMARY KEY (tip_id)
# MAGIC )
# MAGIC COMMENT 'Tips (sugerencias cortas) que los usuarios escribieron sobre negocios de Philadelphia. Una fila por tip.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_checkin (
# MAGIC   checkin_id    STRING     NOT NULL COMMENT 'Identificador determinístico del check-in: hash de business_id, checkin_at y posición en la lista original de checkin.date',
# MAGIC   business_id   STRING     NOT NULL COMMENT 'Negocio donde se registró el check-in',
# MAGIC   checkin_at    TIMESTAMP  NOT NULL COMMENT 'Fecha y hora en que se registró el check-in',
# MAGIC   is_duplicate  BOOLEAN    NOT NULL COMMENT 'Verdadero si ya existe otro check-in del mismo negocio con la misma fecha y hora (DQ-010). Los duplicados no cuentan en las métricas',
# MAGIC   loaded_at     TIMESTAMP  NOT NULL COMMENT 'Momento en que la fila se cargó desde Bronze',
# MAGIC   source_file   STRING              COMMENT 'Archivo de Bronze del que proviene la fila',
# MAGIC   CONSTRAINT pk_slv_checkin PRIMARY KEY (checkin_id)
# MAGIC )
# MAGIC COMMENT 'Eventos de check-in en negocios de Philadelphia. Una fila por cada fecha-hora de la lista original.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_photo (
# MAGIC   photo_id     STRING     NOT NULL COMMENT 'Identificador único de la foto asignado por Yelp',
# MAGIC   business_id  STRING     NOT NULL COMMENT 'Negocio al que pertenece la foto',
# MAGIC   caption      STRING              COMMENT 'Descripción escrita por el usuario para la foto',
# MAGIC   label        STRING              COMMENT 'Tipo de foto (comida, bebida, menú, interior o exterior)',
# MAGIC   loaded_at    TIMESTAMP  NOT NULL COMMENT 'Momento en que la fila se cargó desde Bronze',
# MAGIC   source_file  STRING              COMMENT 'Archivo de Bronze del que proviene la fila',
# MAGIC   CONSTRAINT pk_slv_photo PRIMARY KEY (photo_id)
# MAGIC )
# MAGIC COMMENT 'Fotos que los usuarios subieron a negocios de Yelp. Una fila por foto. Sin datos en la muestra actual, porque el dataset descargado no incluye photo.json.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# DBTITLE 1,3. Gold — dimensiones y puente
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_dim_date (
# MAGIC   date_key     INT     NOT NULL COMMENT 'Clave de la fecha en formato AAAAMMDD',
# MAGIC   full_date    DATE    NOT NULL COMMENT 'Fecha calendario',
# MAGIC   year         INT     NOT NULL COMMENT 'Año de la fecha',
# MAGIC   month        INT     NOT NULL COMMENT 'Número de mes (1 a 12)',
# MAGIC   month_name   STRING  NOT NULL COMMENT 'Nombre del mes',
# MAGIC   quarter      INT     NOT NULL COMMENT 'Trimestre del año (1 a 4)',
# MAGIC   week_start   DATE             COMMENT 'Lunes de la semana a la que pertenece la fecha',
# MAGIC   month_start  DATE             COMMENT 'Primer día del mes al que pertenece la fecha',
# MAGIC   CONSTRAINT pk_gld_dim_date PRIMARY KEY (date_key)
# MAGIC )
# MAGIC COMMENT 'Dimensión de calendario. Una fila por día.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_dim_business (
# MAGIC   business_key      STRING        NOT NULL COMMENT 'Identificador del negocio maestro (master_business_id de slv_business)',
# MAGIC   name              STRING        NOT NULL COMMENT 'Nombre comercial del negocio',
# MAGIC   city              STRING                 COMMENT 'Ciudad del negocio',
# MAGIC   state             STRING                 COMMENT 'Estado del negocio',
# MAGIC   primary_category  STRING                 COMMENT 'Nombre de la categoría principal del negocio: la marcada con is_primary en gld_bridge_business_category',
# MAGIC   declared_stars    DECIMAL(2,1)           COMMENT 'Calificación declarada por Yelp para el negocio (stars de slv_business). Se compara con la calificación válida en rating_gap',
# MAGIC   is_open           BOOLEAN                COMMENT 'Indica si el negocio sigue abierto',
# MAGIC   CONSTRAINT pk_gld_dim_business PRIMARY KEY (business_key)
# MAGIC )
# MAGIC COMMENT 'Dimensión de negocios de Philadelphia, para análisis. Una fila por negocio maestro (golden record).'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_dim_user (
# MAGIC   user_key       STRING        NOT NULL COMMENT 'Identificador del usuario (user_id de Yelp)',
# MAGIC   name           STRING        NOT NULL COMMENT 'Nombre de pila del usuario (dato personal, enmascarado en E6)',
# MAGIC   yelping_since  DATE                   COMMENT 'Fecha en que el usuario se unió a Yelp',
# MAGIC   fans           INT                    COMMENT 'Cantidad de seguidores del usuario',
# MAGIC   avg_stars      DECIMAL(3,2)           COMMENT 'Calificación promedio que da el usuario',
# MAGIC   is_elite       BOOLEAN                COMMENT 'Verdadero si Yelp reconoció al usuario como Elite en al menos un año',
# MAGIC   CONSTRAINT pk_gld_dim_user PRIMARY KEY (user_key)
# MAGIC )
# MAGIC COMMENT 'Dimensión de usuarios de Yelp, para análisis. Una fila por usuario.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_dim_category (
# MAGIC   category_key  BIGINT  NOT NULL COMMENT 'Identificador único de la categoría de negocio (category_id de slv_category)',
# MAGIC   name          STRING  NOT NULL COMMENT 'Nombre de la categoría de negocio, único y normalizado',
# MAGIC   CONSTRAINT pk_gld_dim_category PRIMARY KEY (category_key)
# MAGIC )
# MAGIC COMMENT 'Dimensión de categorías de negocio. Una fila por categoría.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_bridge_business_category (
# MAGIC   business_key  STRING   NOT NULL COMMENT 'Negocio al que se asigna la categoría',
# MAGIC   category_key  BIGINT   NOT NULL COMMENT 'Categoría asignada al negocio',
# MAGIC   is_primary    BOOLEAN  NOT NULL COMMENT 'Indica si es la categoría principal del negocio (la de más negocios en la muestra; en empate, la primera en orden alfabético)',
# MAGIC   CONSTRAINT pk_gld_bridge_business_category PRIMARY KEY (business_key, category_key)
# MAGIC )
# MAGIC COMMENT 'Puente entre negocios y categorías. Una fila por par negocio-categoría.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# DBTITLE 1,4. Gold — hechos
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_fact_review (
# MAGIC   review_id       STRING     NOT NULL COMMENT 'Identificador único de la reseña',
# MAGIC   business_key    STRING     NOT NULL COMMENT 'Negocio maestro reseñado',
# MAGIC   user_key        STRING     NOT NULL COMMENT 'Usuario que escribió la reseña',
# MAGIC   date_key        INT        NOT NULL COMMENT 'Fecha de publicación de la reseña',
# MAGIC   stars           INT        NOT NULL COMMENT 'Calificación de 1 a 5 estrellas',
# MAGIC   useful          INT                 COMMENT 'Votos "útil" recibidos',
# MAGIC   funny           INT                 COMMENT 'Votos "divertido" recibidos',
# MAGIC   cool            INT                 COMMENT 'Votos "genial" recibidos',
# MAGIC   loaded_at       TIMESTAMP  NOT NULL COMMENT 'Momento de la carga de Silver de la que proviene la fila',
# MAGIC   is_valid        BOOLEAN             COMMENT 'Indica si la reseña es válida (no está duplicada). Solo las reseñas válidas cuentan en las métricas oficiales',
# MAGIC   invalid_reason  STRING              COMMENT 'Motivo por el que la reseña no es válida (duplicada). Vacío si la reseña es válida',
# MAGIC   CONSTRAINT pk_gld_fact_review PRIMARY KEY (review_id)
# MAGIC )
# MAGIC COMMENT 'Hechos de reseñas. Una fila por reseña publicada.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_fact_checkin (
# MAGIC   checkin_id    STRING     NOT NULL COMMENT 'Identificador determinístico del check-in (el mismo de slv_checkin)',
# MAGIC   business_key  STRING     NOT NULL COMMENT 'Negocio maestro visitado',
# MAGIC   date_key      INT        NOT NULL COMMENT 'Fecha del check-in en formato AAAAMMDD',
# MAGIC   checkin_hour  INT                 COMMENT 'Hora del check-in, de 0 a 23',
# MAGIC   is_duplicate  BOOLEAN    NOT NULL COMMENT 'Verdadero si es un check-in duplicado (DQ-010). Los duplicados no cuentan en las métricas',
# MAGIC   loaded_at     TIMESTAMP  NOT NULL COMMENT 'Momento de la carga de Silver de la que proviene la fila',
# MAGIC   CONSTRAINT pk_gld_fact_checkin PRIMARY KEY (checkin_id)
# MAGIC )
# MAGIC COMMENT 'Hechos de check-in. Una fila por check-in. No identifica al usuario.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_fact_tip (
# MAGIC   tip_id            STRING     NOT NULL COMMENT 'Identificador determinístico del tip (el mismo de slv_tip)',
# MAGIC   user_key          STRING     NOT NULL COMMENT 'Usuario que escribió el tip',
# MAGIC   business_key      STRING     NOT NULL COMMENT 'Negocio maestro que recibió el tip',
# MAGIC   date_key          INT        NOT NULL COMMENT 'Fecha de publicación del tip en formato AAAAMMDD',
# MAGIC   compliment_count  INT                 COMMENT 'Cantidad de cumplidos recibidos por el tip',
# MAGIC   loaded_at         TIMESTAMP  NOT NULL COMMENT 'Momento de la carga de Silver de la que proviene la fila',
# MAGIC   CONSTRAINT pk_gld_fact_tip PRIMARY KEY (tip_id)
# MAGIC )
# MAGIC COMMENT 'Hechos de tips. Una fila por tip.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# DBTITLE 1,5. Gold — agregados del modelo dimensional
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_agg_business_month (
# MAGIC   business_key            STRING        NOT NULL COMMENT 'Negocio al que corresponde el corte mensual',
# MAGIC   period_month            INT           NOT NULL COMMENT 'Mes del indicador en formato AAAAMM',
# MAGIC   review_count            INT                    COMMENT 'Cantidad mensual de reseñas del negocio',
# MAGIC   avg_review_stars        DECIMAL(3,2)           COMMENT 'Calificación promedio mensual de todas las reseñas publicadas',
# MAGIC   review_author_count     INT                    COMMENT 'Autores únicos de reseñas del negocio en el mes',
# MAGIC   checkin_count           INT                    COMMENT 'Cantidad mensual de check-ins',
# MAGIC   tip_count               INT                    COMMENT 'Cantidad mensual de tips',
# MAGIC   interaction_count       INT                    COMMENT 'Reseñas, check-ins y tips del mes (eventos, no usuarios)',
# MAGIC   valid_review_count      INT                    COMMENT 'Cantidad de reseñas válidas del negocio en el mes',
# MAGIC   valid_rating            DECIMAL(3,2)           COMMENT 'Promedio de estrellas de las reseñas válidas del negocio en el mes',
# MAGIC   useful_review_rate      DECIMAL(5,4)           COMMENT 'Proporción de reseñas válidas con al menos un voto útil',
# MAGIC   days_since_last_review  INT                    COMMENT 'Días entre la última reseña válida y el cierre del mes',
# MAGIC   rating_gap              DECIMAL(3,2)           COMMENT 'Diferencia entre la calificación declarada (declared_stars de gld_dim_business) y la calificación válida',
# MAGIC   negative_review_rate    DECIMAL(5,4)           COMMENT 'Proporción de reseñas válidas de 1 o 2 estrellas',
# MAGIC   invalid_content_rate    DECIMAL(5,4)           COMMENT 'Proporción de reseñas no válidas sobre el total de reseñas del mes',
# MAGIC   calculated_at           TIMESTAMP              COMMENT 'Momento en que se calculó el indicador',
# MAGIC   CONSTRAINT pk_gld_agg_business_month PRIMARY KEY (business_key, period_month)
# MAGIC )
# MAGIC COMMENT 'Indicadores mensuales por negocio. Una fila por negocio y mes.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_agg_business_week (
# MAGIC   business_key   STRING  NOT NULL COMMENT 'Negocio al que corresponde el corte semanal',
# MAGIC   week_start     DATE    NOT NULL COMMENT 'Lunes de la semana del indicador',
# MAGIC   checkin_count  INT              COMMENT 'Cantidad de check-ins únicos del negocio en la semana',
# MAGIC   tip_count      INT              COMMENT 'Cantidad de tips del negocio en la semana',
# MAGIC   CONSTRAINT pk_gld_agg_business_week PRIMARY KEY (business_key, week_start)
# MAGIC )
# MAGIC COMMENT 'Engagement semanal por negocio. Una fila por negocio y semana.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_agg_category_month (
# MAGIC   category_key           BIGINT        NOT NULL COMMENT 'Categoría principal (is_primary = true en el puente) a la que corresponde el corte',
# MAGIC   period_month           INT           NOT NULL COMMENT 'Mes del indicador en formato AAAAMM',
# MAGIC   business_count         INT                    COMMENT 'Cantidad de negocios de la categoría',
# MAGIC   review_count           INT                    COMMENT 'Cantidad de reseñas de los negocios de la categoría en el mes',
# MAGIC   checkin_count          INT                    COMMENT 'Cantidad de check-ins de los negocios de la categoría en el mes',
# MAGIC   avg_review_stars       DECIMAL(3,2)           COMMENT 'Calificación promedio de todas las reseñas de la categoría en el mes',
# MAGIC   valid_rating           DECIMAL(3,2)           COMMENT 'Promedio de estrellas de las reseñas válidas de la categoría en el mes',
# MAGIC   negative_review_count  INT                    COMMENT 'Reseñas válidas de 1 o 2 estrellas de la categoría en el mes',
# MAGIC   CONSTRAINT pk_gld_agg_category_month PRIMARY KEY (category_key, period_month)
# MAGIC )
# MAGIC COMMENT 'Indicadores mensuales por categoría principal. Una fila por categoría y mes.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_agg_user_month (
# MAGIC   user_key            STRING   NOT NULL COMMENT 'Usuario al que corresponde el corte mensual',
# MAGIC   period_month        INT      NOT NULL COMMENT 'Mes de la actividad en formato AAAAMM',
# MAGIC   review_count        INT               COMMENT 'Reseñas publicadas por el usuario en el mes',
# MAGIC   tip_count           INT               COMMENT 'Tips publicados por el usuario en el mes',
# MAGIC   contribution_count  INT               COMMENT 'Reseñas y tips del usuario en el mes',
# MAGIC   is_contributor      BOOLEAN           COMMENT 'Verdadero si el usuario publicó al menos una reseña o tip en el mes',
# MAGIC   CONSTRAINT pk_gld_agg_user_month PRIMARY KEY (user_key, period_month)
# MAGIC )
# MAGIC COMMENT 'Actividad mensual por usuario. Una fila por usuario y mes.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# DBTITLE 1,6. Gold — agregados de KPIs por stakeholder (conservados del diseño anterior)
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_agg_business_bayesian_month (
# MAGIC   business_key        STRING        NOT NULL COMMENT 'Negocio al que corresponde el corte mensual',
# MAGIC   period_month        INT           NOT NULL COMMENT 'Mes del indicador en formato AAAAMM',
# MAGIC   valid_review_count  INT                    COMMENT 'Cantidad de reseñas válidas del negocio en el mes',
# MAGIC   valid_rating        DECIMAL(3,2)           COMMENT 'Promedio simple de estrellas de las reseñas válidas del negocio en el mes',
# MAGIC   bayesian_rating     DECIMAL(3,2)           COMMENT 'Calificación bayesiana: pondera valid_rating con el promedio de la ciudad usando m = 25 reseñas, para que los negocios con pocas reseñas no queden arriba del ranking',
# MAGIC   review_author_count INT                    COMMENT 'Autores únicos de reseñas válidas del negocio en el mes',
# MAGIC   CONSTRAINT pk_gld_agg_business_bayesian_month PRIMARY KEY (business_key, period_month)
# MAGIC )
# MAGIC COMMENT 'Calificación bayesiana mensual por negocio, para el ranking de Producto (Andrew Collins). Una fila por negocio y mes.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_agg_business_checkin_day (
# MAGIC   business_key        STRING  NOT NULL COMMENT 'Negocio visitado',
# MAGIC   date_key            INT     NOT NULL COMMENT 'Fecha de las visitas en formato AAAAMMDD',
# MAGIC   checkin_count       INT              COMMENT 'Cantidad de check-ins no duplicados del negocio en el día',
# MAGIC   active_hour_count   INT              COMMENT 'Cantidad de horas distintas del día con al menos un check-in (conteo homologado de afluencia por ventana de 60 minutos)',
# MAGIC   CONSTRAINT pk_gld_agg_business_checkin_day PRIMARY KEY (business_key, date_key)
# MAGIC )
# MAGIC COMMENT 'Conteo diario oficial de check-ins por negocio, para Operaciones Comerciales (Carla Romero). Una fila por negocio y día con check-ins.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_agg_review_health_month (
# MAGIC   period_month              INT           NOT NULL COMMENT 'Mes de publicación de las reseñas en formato AAAAMM',
# MAGIC   valid_review_count        INT                    COMMENT 'Reseñas válidas del mes (en Silver y no duplicadas)',
# MAGIC   invalid_review_count      INT                    COMMENT 'Reseñas del mes marcadas como no válidas por duplicado',
# MAGIC   quarantined_review_count  INT                    COMMENT 'Reseñas del mes rechazadas en slv_review_quarantine',
# MAGIC   quarantine_rate           DECIMAL(5,4)           COMMENT 'Proporción de reseñas rechazadas sobre el total de reseñas recibidas en el mes',
# MAGIC   CONSTRAINT pk_gld_agg_review_health_month PRIMARY KEY (period_month)
# MAGIC )
# MAGIC COMMENT 'Salud mensual de las reseñas: válidas, duplicadas y rechazadas, para Trust & Safety (Rafael Nakamura). Una fila por mes.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_agg_user_cohort_month (
# MAGIC   cohort_month       INT           NOT NULL COMMENT 'Mes en que los usuarios se unieron a Yelp, en formato AAAAMM',
# MAGIC   activity_month     INT           NOT NULL COMMENT 'Mes de actividad de la cohorte, en formato AAAAMM',
# MAGIC   months_since_join  INT                    COMMENT 'Meses transcurridos entre cohort_month y activity_month',
# MAGIC   cohort_size        INT                    COMMENT 'Cantidad de usuarios de la cohorte',
# MAGIC   active_user_count  INT                    COMMENT 'Usuarios de la cohorte que publicaron al menos una reseña válida en el mes',
# MAGIC   retention_rate     DECIMAL(5,4)           COMMENT 'Proporción de usuarios de la cohorte activos en el mes',
# MAGIC   CONSTRAINT pk_gld_agg_user_cohort_month PRIMARY KEY (cohort_month, activity_month)
# MAGIC )
# MAGIC COMMENT 'Retención mensual de cohortes de usuarios según su mes de alta, para Producto (Andrew Collins). Una fila por cohorte y mes de actividad.'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# DBTITLE 1,7. Restricciones CHECK y claves foráneas (idempotentes)
restricciones = [
    # CHECK enforzadas
    ("workspace.yelp_silver.slv_business", "chk_slv_business_stars", "CHECK (stars IS NULL OR stars BETWEEN 1.0 AND 5.0)"),
    ("workspace.yelp_silver.slv_review", "chk_slv_review_stars", "CHECK (stars BETWEEN 1 AND 5)"),
    ("workspace.yelp_silver.slv_friendship", "chk_slv_friendship_order", "CHECK (user_id_1 < user_id_2)"),
    ("workspace.yelp_gold.gld_fact_review", "chk_gld_fact_review_stars", "CHECK (stars BETWEEN 1 AND 5)"),
    ("workspace.yelp_gold.gld_dim_date", "chk_gld_dim_date_month", "CHECK (month BETWEEN 1 AND 12)"),
    ("workspace.yelp_gold.gld_dim_date", "chk_gld_dim_date_quarter", "CHECK (quarter BETWEEN 1 AND 4)"),
    # Claves foráneas Silver
    ("workspace.yelp_silver.slv_business", "fk_slv_business_master", "FOREIGN KEY (master_business_id) REFERENCES workspace.yelp_silver.slv_business(business_id)"),
    ("workspace.yelp_silver.slv_review", "fk_slv_review_user", "FOREIGN KEY (user_id) REFERENCES workspace.yelp_silver.slv_user(user_id)"),
    ("workspace.yelp_silver.slv_review", "fk_slv_review_business", "FOREIGN KEY (business_id) REFERENCES workspace.yelp_silver.slv_business(business_id)"),
    ("workspace.yelp_silver.slv_business_category", "fk_slv_business_category_business", "FOREIGN KEY (business_id) REFERENCES workspace.yelp_silver.slv_business(business_id)"),
    ("workspace.yelp_silver.slv_business_category", "fk_slv_business_category_category", "FOREIGN KEY (category_id) REFERENCES workspace.yelp_silver.slv_category(category_id)"),
    ("workspace.yelp_silver.slv_checkin", "fk_slv_checkin_business", "FOREIGN KEY (business_id) REFERENCES workspace.yelp_silver.slv_business(business_id)"),
    ("workspace.yelp_silver.slv_tip", "fk_slv_tip_user", "FOREIGN KEY (user_id) REFERENCES workspace.yelp_silver.slv_user(user_id)"),
    ("workspace.yelp_silver.slv_tip", "fk_slv_tip_business", "FOREIGN KEY (business_id) REFERENCES workspace.yelp_silver.slv_business(business_id)"),
    ("workspace.yelp_silver.slv_photo", "fk_slv_photo_business", "FOREIGN KEY (business_id) REFERENCES workspace.yelp_silver.slv_business(business_id)"),
    ("workspace.yelp_silver.slv_friendship", "fk_slv_friendship_user_1", "FOREIGN KEY (user_id_1) REFERENCES workspace.yelp_silver.slv_user(user_id)"),
    ("workspace.yelp_silver.slv_friendship", "fk_slv_friendship_user_2", "FOREIGN KEY (user_id_2) REFERENCES workspace.yelp_silver.slv_user(user_id)"),
    # Claves foráneas Gold
    ("workspace.yelp_gold.gld_bridge_business_category", "fk_gld_bridge_business_category_business", "FOREIGN KEY (business_key) REFERENCES workspace.yelp_gold.gld_dim_business(business_key)"),
    ("workspace.yelp_gold.gld_bridge_business_category", "fk_gld_bridge_business_category_category", "FOREIGN KEY (category_key) REFERENCES workspace.yelp_gold.gld_dim_category(category_key)"),
    ("workspace.yelp_gold.gld_fact_review", "fk_gld_fact_review_business", "FOREIGN KEY (business_key) REFERENCES workspace.yelp_gold.gld_dim_business(business_key)"),
    ("workspace.yelp_gold.gld_fact_review", "fk_gld_fact_review_user", "FOREIGN KEY (user_key) REFERENCES workspace.yelp_gold.gld_dim_user(user_key)"),
    ("workspace.yelp_gold.gld_fact_review", "fk_gld_fact_review_date", "FOREIGN KEY (date_key) REFERENCES workspace.yelp_gold.gld_dim_date(date_key)"),
    ("workspace.yelp_gold.gld_fact_checkin", "fk_gld_fact_checkin_business", "FOREIGN KEY (business_key) REFERENCES workspace.yelp_gold.gld_dim_business(business_key)"),
    ("workspace.yelp_gold.gld_fact_checkin", "fk_gld_fact_checkin_date", "FOREIGN KEY (date_key) REFERENCES workspace.yelp_gold.gld_dim_date(date_key)"),
    ("workspace.yelp_gold.gld_fact_tip", "fk_gld_fact_tip_user", "FOREIGN KEY (user_key) REFERENCES workspace.yelp_gold.gld_dim_user(user_key)"),
    ("workspace.yelp_gold.gld_fact_tip", "fk_gld_fact_tip_business", "FOREIGN KEY (business_key) REFERENCES workspace.yelp_gold.gld_dim_business(business_key)"),
    ("workspace.yelp_gold.gld_fact_tip", "fk_gld_fact_tip_date", "FOREIGN KEY (date_key) REFERENCES workspace.yelp_gold.gld_dim_date(date_key)"),
    ("workspace.yelp_gold.gld_agg_business_month", "fk_gld_agg_business_month_business", "FOREIGN KEY (business_key) REFERENCES workspace.yelp_gold.gld_dim_business(business_key)"),
    ("workspace.yelp_gold.gld_agg_business_week", "fk_gld_agg_business_week_business", "FOREIGN KEY (business_key) REFERENCES workspace.yelp_gold.gld_dim_business(business_key)"),
    ("workspace.yelp_gold.gld_agg_category_month", "fk_gld_agg_category_month_category", "FOREIGN KEY (category_key) REFERENCES workspace.yelp_gold.gld_dim_category(category_key)"),
    ("workspace.yelp_gold.gld_agg_user_month", "fk_gld_agg_user_month_user", "FOREIGN KEY (user_key) REFERENCES workspace.yelp_gold.gld_dim_user(user_key)"),
    ("workspace.yelp_gold.gld_agg_business_bayesian_month", "fk_gld_agg_business_bayesian_month_business", "FOREIGN KEY (business_key) REFERENCES workspace.yelp_gold.gld_dim_business(business_key)"),
    ("workspace.yelp_gold.gld_agg_business_checkin_day", "fk_gld_agg_business_checkin_day_business", "FOREIGN KEY (business_key) REFERENCES workspace.yelp_gold.gld_dim_business(business_key)"),
    ("workspace.yelp_gold.gld_agg_business_checkin_day", "fk_gld_agg_business_checkin_day_date", "FOREIGN KEY (date_key) REFERENCES workspace.yelp_gold.gld_dim_date(date_key)"),
]

for tabla, nombre, definicion in restricciones:
    spark.sql(f"ALTER TABLE {tabla} DROP CONSTRAINT IF EXISTS {nombre}")
    spark.sql(f"ALTER TABLE {tabla} ADD CONSTRAINT {nombre} {definicion}")
print(f"✔ {len(restricciones)} restricciones CHECK / FOREIGN KEY declaradas")

# COMMAND ----------

# DBTITLE 1,Validación automática del entregable
silver_esperadas = {"slv_business", "slv_category", "slv_business_category", "slv_user", "slv_friendship",
                    "slv_review", "slv_review_quarantine", "slv_tip", "slv_checkin", "slv_photo"}
gold_esperadas = {"gld_dim_date", "gld_dim_business", "gld_dim_user", "gld_dim_category", "gld_bridge_business_category",
                  "gld_fact_review", "gld_fact_checkin", "gld_fact_tip",
                  "gld_agg_business_month", "gld_agg_business_week", "gld_agg_category_month", "gld_agg_user_month",
                  "gld_agg_business_bayesian_month", "gld_agg_business_checkin_day", "gld_agg_review_health_month", "gld_agg_user_cohort_month"}

silver = {r.tableName for r in spark.sql("SHOW TABLES IN workspace.yelp_silver").collect()}
gold = {r.tableName for r in spark.sql("SHOW TABLES IN workspace.yelp_gold").collect()}
assert silver_esperadas <= silver, f"Faltan tablas Silver: {silver_esperadas - silver}"
assert gold_esperadas <= gold, f"Faltan tablas Gold: {gold_esperadas - gold}"

tablas_modelo = sorted(silver_esperadas | gold_esperadas)
lista_sql = ", ".join(f"'{t}'" for t in tablas_modelo)
sin_comment = spark.sql(f"""
  SELECT table_name, column_name
  FROM workspace.information_schema.columns
  WHERE table_schema IN ('yelp_silver','yelp_gold')
    AND table_name IN ({lista_sql})
    AND (comment IS NULL OR comment = '')
""").collect()
assert not sin_comment, f"❌ Columnas sin COMMENT (rompe E4): {[(r.table_name, r.column_name) for r in sin_comment]}"
print(f"✔ Modelo físico validado: {len(silver_esperadas)} tablas Silver · {len(gold_esperadas)} tablas Gold · 100% de columnas con COMMENT")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Decisiones de implementación del modelo físico
# MAGIC
# MAGIC 1. **Categorías en tabla puente (`slv_category` + `slv_business_category`):** el texto `business.categories` se separa por comas y se normaliza en un catálogo con `category_id` generado. En Gold, `gld_bridge_business_category.is_primary` define una sola categoría principal por negocio (la de más negocios en la muestra; en empate, la primera alfabética), y `gld_dim_business.primary_category` la repite para consultas directas.
# MAGIC 2. **Golden record (`slv_business.master_business_id`, DQ-012):** los negocios con el mismo nombre, dirección y código postal normalizados comparten un negocio maestro; sobrevive el que declara más reseñas (en empate, el `business_id` menor). Gold trabaja con el negocio maestro (`business_key`).
# MAGIC 3. **Check-ins a grano evento (`slv_checkin`, DQ-010):** el texto `checkin.date` se separa en una fila por fecha-hora. `checkin_id` es un hash determinístico (negocio, fecha-hora y posición), así que el pipeline es idempotente. Los check-ins repetidos del mismo negocio a la misma fecha-hora se conservan con `is_duplicate = true` y no cuentan en las métricas.
# MAGIC 4. **Reseñas válidas y cuarentena:** las reseñas que incumplen reglas bloqueantes (estrellas fuera de rango, autor o negocio inexistente, fecha imposible, `review_id` repetido) van a `slv_review_quarantine` con `rejection_reason`. Las duplicadas lógicas (mismo usuario, negocio y texto, DQ-002) sí entran a `slv_review`; Gold las marca con `is_valid = false`.
# MAGIC 5. **Trazabilidad (DQ-015):** todas las tablas Silver llevan `loaded_at` y `source_file`; los hechos Gold llevan `loaded_at` de la carga de Silver.
# MAGIC 6. **Privacidad:** `slv_user.name` y `gld_dim_user.name` se protegen con `COLUMN MASK` en E6; `slv_review.text` no se publica en Gold.
# MAGIC
# MAGIC ---
# MAGIC ### Definition of Done (E3)
# MAGIC - [x] DDL completo de Silver (10 tablas) y Gold (16 tablas), alineado con `yelp_silver.txt`, `yelp_gold.txt` y `yelp_modelo_logico.drawio`.
# MAGIC - [x] 100% de columnas con `COMMENT` (la celda de validación pasa con 0 columnas sin documentar).
# MAGIC - [x] `PRIMARY KEY` / `FOREIGN KEY` informativas y `CHECK` enforzadas.
# MAGIC - [x] `delta.enableChangeDataFeed = true` en todas las tablas.
# MAGIC
# MAGIC > Este notebook se incluye con `%run` desde `E7_pipeline_lakehouse/02_silver_limpieza` (Free Edition permite un solo run activo, por eso no se usa `dbutils.notebook.run`).
