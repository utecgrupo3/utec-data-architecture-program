# Especificación de Diagrama: Modelo Lógico (Silver 3FN) y Modelo Dimensional (Gold Kimball)

> **Artefacto Asociado:** `modelo_logico_lakehouse_v3.drawio`  
> **Sección del Documento Formal:** [Secciones 4.2 y 4.3 · Modelo Lógico y Estrategia Dimensional para la Capa Gold (Entregable E3)](file:///Users/jhuertad/Desktop/cursos/arquitectura_datos/clase_09/Documento_Formal_Arquitectura_Datos_YelpConnect.md#42-modelo-lógico-y-decisiones-de-normalización)  
> **Caso de Uso Prioritario:** Dirección de Producto (Andrew Collins, VP of Product)  
> **Propósito de este archivo:** Insumo exhaustivo para generar el diagrama en **Draw.io** utilizando asistentes de IA, plugins de generación automática o importación nativa de Mermaid/Graphviz.

---

## 1. Prompt Maestro para la IA de Draw.io

```text
Actúa como un Diseñador y Modelador de Datos Senior experto en la Metodología Dimensional de Ralph Kimball y Arquitectura Medallion en Delta Lake.
Genera un diagrama integral en Draw.io estructurado en dos secciones principales conectadas por un flujo de transformación de ETL/ELT:

1. SECCIÓN SUPERIOR / IZQUIERDA: MODELO LÓGICO RELACIONAL EN SILVER (Tercera Forma Normal - 3FN)
   - Tablas normalizadas: slv_business, slv_user, slv_review, slv_checkin, slv_tip.
   - Tabla de excepción: slv_review_cuarentena (para registros que fallan Quality Gates).
   - Características: Claves naturales (UUIDs de negocio), tipificación estricta, restricciones CHECK y NOT NULL, colecciones como ARRAY<STRING>.

2. FLECHA DE TRANSFORMACIÓN CENTRAL:
   - "Conformación & Modelado Dimensional (PySpark / Delta Lake)"
   - Reglas aplicadas: Generación de Surrogate Keys (SK), tratamiento de historia (SCD Tipo 1 y 2), cálculo de Rating Bayesiano (BWR), agregación mensual y cálculo de retención de cohortes.

3. SECCIÓN INFERIOR / DERECHA: MODELO DIMENSIONAL EN GOLD (Esquema Estrella Kimball Multigranularidad)
   - DIMENSIONES CONFORMES:
     * gld_dim_negocio (SCD Tipo 1 para datos descriptivos, SCD Tipo 2 para estado operativo con fecha_inicio, fecha_fin, es_actual).
     * gld_dim_usuario (SCD Tipo 1 para métricas acumuladas, SCD Tipo 2 para estatus Élite, enmascaramiento dinámico de name).
     * gld_dim_tiempo (Dimensión calendario estática: fecha_sk, anio, mes, trimestre, dia_semana).
   - TABLA DE HECHOS ATÓMICOS (Grano Fino):
     * gld_fact_resenas (Grano: Una reseña individual por usuario, negocio y fecha. Métricas: stars, text_len, useful).
   - TABLAS DE HECHOS AGREGADOS PERIÓDICOS (Grano Resumido):
     * gld_kpi_negocio_mensual (Grano: Un registro por negocio y mes. Métricas: BWR, avg_stars, n_resenas, checkins).
     * gld_cohortes_retencion_mensual (Grano: Una cohorte de registro por mes de actividad. Métricas: usuarios_activos, crr_pct).

4. PALETA DE COLORES Y ESTILO VISUAL:
   - Zona Silver (3FN): Cajas con encabezado #64748B (Gris Slate), fondo blanco, bordes #94A3B8.
   - Zona Cuarentena: Encabezado #DC2626 (Rojo alerta), fondo #FEF2F2, borde discontinuo.
   - Dimensiones Gold: Encabezado #2563EB (Azul Real), fondo #EFF6FF, bordes #3B82F6.
   - Hechos Atómicos Gold: Encabezado #D97706 (Ámbar Dorado), fondo #FFFBEB, bordes #F59E0B.
   - Hechos Agregados Gold: Encabezado #059669 (Verde Esmeralda), fondo #ECFDF5, bordes #10B981.
   - Conectores: Ortogonales con flechas orientadas desde Dimensiones hacia Hechos (relación 1 a N).
```

---

## 2. Vista de Arquitectura a Nivel Funcional (Visión de Negocio)

### 2.1. Objetivo Funcional
Explicar a los líderes de la organización (**Andrew Collins de Producto**, **Laura Hernández CDO**) cómo las transacciones normalizadas y crudas se transforman en estructuras analíticas optimizadas que responden sin contradicciones a:
1. ¿Cuál es la verdadera satisfacción de los comensales en cada categoría? (Rating Bayesiano sin sesgo).
2. ¿Qué negocios tienen alta fidelidad y visitas recurrentes? (Check-ins homologados).
3. ¿Cómo se comportan las cohortes de usuarios en el tiempo? (Tasa de retención $CRR$ y Stickiness $DAU/MAU$).

### 2.2. Cumplimiento de los 12 Lineamientos Técnicos de Gold (Sesión 19)

| Lineamiento | Cómo se cumple en el diseño de Gold | Beneficio directo para Andrew Collins (VP Product) |
| :--- | :--- | :--- |
| **1. "El cálculo tiene dueño y propósito"** | Cada columna en Gold está respaldada por un KPI de Producto definido en el Glosario de Negocio. | No existen tablas huérfanas ni métricas no solicitadas. |
| **2. "El grano se declara, no se deduce"** | Cada tabla de hechos y agregados tiene un enunciado estricto de granularidad documentado. | Previene errores fatales de doble conteo al sumar métricas en BI. |
| **3. Prohibido mezclar granularidades** | Hechos transaccionales atómicos (`gld_fact_resenas`) viven en una tabla separada de los mensuales (`gld_kpi_negocio_mensual`). | Permite tanto auditoría profunda de una opinión como dashboards mensuales de alta velocidad. |
| **4. Dimensiones conformes compartidas** | `gld_dim_negocio` y `gld_dim_usuario` sirven tanto a hechos atómicos como agregados. | Garantiza coherencia matemática: la suma de partes siempre cuadra con el total consolidado. |
| **5. Tratamiento histórico formal (SCD)** | Cambios en el estado del local (`esta_abierto`) o estatus Élite del comensal generan nuevas versiones de fila (SCD Tipo 2). | Preserva el contexto histórico exacto en que ocurrió la reseña o la visita. |

### 2.3. Diagrama Funcional en Código Mermaid (Listo para Importar en Draw.io)

```mermaid
graph TD
    %% ESTILOS VISUALES
    classDef silverStyle fill:#F8FAFC,stroke:#64748B,stroke-width:2px,color:#0F172A;
    classDef quaranStyle fill:#FEF2F2,stroke:#DC2626,stroke-width:2px,color:#991B1B,stroke-dasharray: 4 4;
    classDef dimStyle fill:#EFF6FF,stroke:#2563EB,stroke-width:2px,color:#1E40AF;
    classDef factStyle fill:#FFFBEB,stroke:#D97706,stroke-width:2px,color:#92400E;
    classDef kpiStyle fill:#ECFDF5,stroke:#059669,stroke-width:2px,color:#065F46;
    classDef etlStyle fill:#F1F5F9,stroke:#475569,stroke-width:2px,color:#334155;

    %% CAPA SILVER NORMALIZADA
    subgraph SILVER_ZONE ["CAPA SILVER (Conformada, 3FN y Deduplicada)"]
        S_BIZ["🏢 slv_business<br>(Negocios, direccion, coords, categorias array)"]:::silverStyle
        S_USR["👤 slv_user<br>(Usuarios, votos enviados y nombre protegido)"]:::silverStyle
        S_REV["⭐ slv_review<br>(Resenas deduplicadas con rating 1.0 - 5.0)"]:::silverStyle
        S_CHK["📍 slv_checkin<br>(Visitas deduplicadas por ventana de 60 min)"]:::silverStyle
        S_TIP["💬 slv_tip<br>(Consejos breves normalizados)"]:::silverStyle
        S_SOC["🤝 slv_amistad<br>(Red de contactos y amigos del comensal)"]:::silverStyle
        S_QUA["🚨 slv_review_cuarentena<br>(Resenas con fallas de calidad aisladas)"]:::quaranStyle
    end

    %% MOTOR DE TRANSFORMACIÓN
    TRANSFORM["⚡ MOTOR DE CONFORMACIÓN & MODELADO DIMENSIONAL<br>• Generacion de Surrogate Keys (SK)<br>• Aplicacion de SCD Tipo 1 y 2<br>• Formula Rating Bayesiano (BWR)<br>• Agrupaciones Mensuales & Cohortes"]:::etlStyle

    %% CAPA GOLD DIMENSIONAL KIMBALL
    subgraph GOLD_ZONE ["CAPA GOLD (Modelo Dimensional Kimball & Productos de Valor)"]
        
        %% DIMENSIONES CONFORMES
        subgraph DIMS ["DIMENSIONES CONFORMES"]
            D_BIZ["🏛️ gld_dim_negocio<br>• business_sk (PK)<br>• categoria_principal (STRING BI)<br>• SCD 1: Nombres y categorias<br>• SCD 2: esta_abierto y vigencia"]:::dimStyle
            D_USR["🏛️ gld_dim_usuario<br>• user_sk (PK)<br>• SCD 1: Metricas acumuladas<br>• SCD 2: Estatus Elite<br>• Nombre enmascarado"]:::dimStyle
            D_TIME["🏛️ gld_dim_tiempo<br>• date_sk (PK)<br>• Anio, Mes, Trimestre, Dia"]:::dimStyle
        end

        %% HECHOS ATÓMICOS
        subgraph FACTS_ATOMIC ["HECHOS ATÓMICOS TRANSACCIONALES"]
            F_REV["⭐ gld_fact_resenas<br>• Grano: 1 resena individual aprobada<br>• FKs: business_sk, user_sk, date_sk<br>• Metricas: stars, length, useful"]:::factStyle
        end

        %% HECHOS AGREGADOS PERIÓDICOS
        subgraph FACTS_AGG ["HECHOS AGREGADOS PERIÓDICOS (KPIs PRODUCTO)"]
            K_BIZ["📈 gld_kpi_negocio_mensual<br>• Grano: 1 negocio por mes calendario<br>• Metricas: Rating Bayesiano (BWR),<br>  avg_stars, total_resenas, checkins"]:::kpiStyle
            K_COHORT["👥 gld_cohortes_retencion_mensual<br>• Grano: 1 cohorte por mes de actividad<br>• Metricas: Tasa de Retencion (CRR),<br>  Stickiness (DAU/MAU)"]:::kpiStyle
        end
    end

    %% CONEXIONES DE FLUJO
    SILVER_ZONE --> TRANSFORM
    TRANSFORM --> GOLD_ZONE

    %% RELACIONES DIMENSIONALES EN GOLD
    D_BIZ -->|1 : N| F_REV
    D_USR -->|1 : N| F_REV
    D_TIME -->|1 : N| F_REV

    D_BIZ -->|1 : N| K_BIZ
    D_TIME -->|1 : N| K_BIZ

    D_TIME -->|1 : N| K_COHORT
```

---

## 3. Vista de Arquitectura a Nivel Técnico (Esquema Físico y Tipos de Datos)

### 3.1. Objetivo Técnico
Especificar las columnas exactas, claves primarias (`PK`), claves foráneas (`FK`), claves sustitutas (`SK`), atributos de control histórico (`fecha_inicio_vigencia`, `fecha_fin_vigencia`, `es_actual`), tipos de datos de Delta Lake y estrategias de particionado.

### 3.2. Mapeo de Tablas Lógicas vs Objetos Físicos en Unity Catalog

| Tabla Lógica | Tabla Física en Unity Catalog | Rol en el Modelo | Grano Físico Declarado | Clave Primaria / SK |
| :--- | :--- | :--- | :--- | :--- |
| `BUSINESS` | `workspace.yelp_silver.slv_business` | Relacional 3FN | Un registro por negocio activo (excluye `attributes` y `hours` retenidos en Bronze según **ADR-07**). | `business_id` (STRING) |
| `REVIEW` | `workspace.yelp_silver.slv_review` | Relacional 3FN | Una reseña válida deduplicada. | `review_id` (STRING) |
| `USER` | `workspace.yelp_silver.slv_user` | Relacional 3FN | Un usuario registrado. | `user_id` (STRING) |
| `CHECKIN` | `workspace.yelp_silver.slv_checkin` | Relacional 3FN | Un check-in deduplicado por hora. | `checkin_id` (STRING) |
| `CUARENTENA` | `workspace.yelp_silver.slv_review_cuarentena` | Buffer de Aislamiento | Una reseña no conforme retenida. | `review_id` + `motivo_rechazo` |
| `DIM_NEGOCIO` | `workspace.yelp_gold.gld_dim_negocio` | Dimensión SCD 1 & 2 | Un estado de vigencia del negocio. | `business_sk` (BIGINT) |
| `DIM_USUARIO` | `workspace.yelp_gold.gld_dim_usuario` | Dimensión SCD 1 & 2 | Un estado de vigencia del usuario. | `user_sk` (BIGINT) |
| `DIM_TIEMPO` | `workspace.yelp_gold.gld_dim_tiempo` | Dimensión Conforme | Una fecha calendario. | `date_sk` (INT: YYYYMMDD) |
| `FACT_RESENAS`| `workspace.yelp_gold.gld_fact_resenas` | Hecho Atómico | Una reseña validada por evento. | `review_sk` (BIGINT) |
| `KPI_MENSUAL` | `workspace.yelp_gold.gld_kpi_negocio_mensual` | Hecho Agregado Periódico | Un negocio y un mes calendario. | `business_sk` + `mes_sk` |
| `COHORTES` | `workspace.yelp_gold.gld_cohortes_retencion_mensual` | Hecho Agregado Periódico | Una cohorte y un mes evaluado. | `cohorte_sk` + `mes_actividad_sk` |

### 3.3. Diagrama Técnico en Código Mermaid (Diagrama ER Relacional y Estrella)

```mermaid
erDiagram
    %% ESQUEMA SILVER 3FN
    slv_business ||--o{ slv_review : "1:N (business_id)"
    slv_user ||--o{ slv_review : "1:N (user_id)"
    slv_business ||--o{ slv_checkin : "1:N (business_id)"
    
    %% ESQUEMA GOLD KIMBALL
    gld_dim_negocio ||--o{ gld_fact_resenas : "1:N (business_sk)"
    gld_dim_usuario ||--o{ gld_fact_resenas : "1:N (user_sk)"
    gld_dim_tiempo ||--o{ gld_fact_resenas : "1:N (date_sk)"

    gld_dim_negocio ||--o{ gld_kpi_negocio_mensual : "1:N (business_sk)"
    gld_dim_tiempo ||--o{ gld_kpi_negocio_mensual : "1:N (mes_sk)"

    gld_dim_tiempo ||--o{ gld_cohortes_retencion_mensual : "1:N (mes_actividad_sk)"

    %% RELACIÓN DE AMISTAD EN SILVER
    slv_user ||--o{ slv_amistad : "1:N (user_id_1)"
    slv_amistad }o--|| slv_user : "N:1 (user_id_2)"

    %% DEFINICIÓN DE ENTIDADES SILVER
    slv_business {
        STRING business_id PK
        STRING nombre
        STRING direccion
        STRING ciudad
        STRING estado
        STRING codigo_postal
        DOUBLE latitud
        DOUBLE longitud
        DOUBLE estrellas
        INT n_resenas
        BOOLEAN esta_abierto
        ARRAY_STRING categorias
        TIMESTAMP fecha_carga
    }

    slv_amistad {
        STRING user_id_1 PK_FK
        STRING user_id_2 PK_FK
        TIMESTAMP fecha_creacion
    }

    slv_review {
        STRING review_id PK
        STRING business_id FK
        STRING user_id FK
        DOUBLE stars
        STRING text
        TIMESTAMP date
        INT useful
        INT funny
        INT cool
    }

    slv_user {
        STRING user_id PK
        STRING name_masked
        INT review_count
        TIMESTAMP yelping_since
        DOUBLE average_stars
        INT fans
        INT useful_enviados
        INT funny_enviados
        INT cool_enviados
    }

    slv_checkin {
        STRING checkin_id PK
        STRING business_id FK
        TIMESTAMP date_hour
        INT event_count
    }

    %% DEFINICIÓN DE TABLAS GOLD
    gld_dim_negocio {
        BIGINT business_sk PK
        STRING business_id
        STRING nombre
        STRING ciudad
        STRING estado
        STRING categoria_principal
        BOOLEAN esta_abierto
        ARRAY_STRING categorias
        TIMESTAMP fecha_inicio_vigencia
        TIMESTAMP fecha_fin_vigencia
        BOOLEAN es_actual
    }

    gld_dim_usuario {
        BIGINT user_sk PK
        STRING user_id
        STRING nombre_enmascarado
        TIMESTAMP yelping_since
        BOOLEAN es_elite
        INT total_resenas_historicas
        TIMESTAMP fecha_inicio_vigencia
        TIMESTAMP fecha_fin_vigencia
        BOOLEAN es_actual
    }

    gld_dim_tiempo {
        INT date_sk PK
        DATE fecha
        INT anio
        INT mes
        INT trimestre
        INT dia_del_mes
        STRING nombre_mes
        STRING dia_de_semana
    }

    gld_fact_resenas {
        BIGINT review_sk PK
        BIGINT business_sk FK
        BIGINT user_sk FK
        INT date_sk FK
        DOUBLE stars
        INT text_length
        INT useful_votes
        TIMESTAMP ingestion_ts
    }

    gld_kpi_negocio_mensual {
        BIGINT business_sk FK
        INT mes_sk FK
        INT n_resenas_mes
        DOUBLE rating_aritmetico_simple
        DOUBLE rating_ponderado_bayesiano_bwr
        INT usuarios_unicos_comentaron
        INT checkins_homologados_mes
    }

    gld_cohortes_retencion_mensual {
        INT cohorte_sk FK
        INT mes_actividad_sk FK
        INT usuarios_base_cohorte
        INT usuarios_activos_mes
        DOUBLE tasa_retencion_crr_pct
        DOUBLE stickiness_dau_mau_ratio
    }
```

---

## 4. Parámetros de Estilo Recomendados para Draw.io

Para que el asistente de Draw.io cree un diagrama estético de estándar profesional:
* **Layout:** Dual Swimlane / Vertical Partition (Parte superior: Silver 3FN; Parte inferior: Gold Kimball).
* **Forma de Entidades:** Tablas formales tipo base de datos (`shape=table;rowLabels=0;childLayout=tableLayout;`).
* **Estilo de Claves:** Iconos o negritas para `PK` (amarillo dorado/llave), `FK` (azul/conector), `SK` (verde/surogate).
* **Conectores Dimensionales:** Estilo ortogonal limpio con etiqueta de cardinalidad 1 a N claramente legible sin encimarse en las tablas.
