<img src="assets/usfq-logo.png" alt="USFQ" height="70">

# LOGISTPULSE Lakehouse — Data Warehouse + Data Lake

Implementación ejecutable del diagrama *Data Warehouse vs Data Lake* aplicada a **LOGISTPULSE** (pedidos, despachos y flota). Todo corre con Docker Compose; está listo para **GitHub Codespaces** (`.devcontainer/`, 4 CPU / 8 GB recomendado).

## Mapeo del diagrama → componentes

| Diagrama | En LOGISTPULSE | Tecnología |
|---|---|---|
| Datos organizacionales | Pedidos, ítems y despachos | Postgres `ods.*` |
| Datos de terceros | SLA de transportistas, clima diario | Lake raw: `third_party/` |
| Datos demográficos | Población / ingreso por zona | Lake raw: `demographics/` |
| Redes sociales | Reseñas de entregas | Lake raw: `social/` |
| Dispositivos y sensores | Telemetría AMR / vans / camiones | Redpanda (Kafka API) |
| Master Data | Clientes, productos, flota, depósitos | Postgres `master.*` |
| ETL en lotes | Lake + ODS → DW (upserts idempotentes) | `lp/etl.py` + `etl_dw.sql` |
| Operational Data Store | Pedidos + telemetría reciente (2 h) + alertas | Postgres `ods.*` |
| Data Warehouse | Estrella: `fact_shipment` + 6 dimensiones | Postgres `dw.*` |
| Data Marts | `mart_ops`, `mart_commercial` | Vistas |
| OLAP capa semántica | `semantic.v_shipments` + cubo `mv_cube_otif` (GROUPING SETS) | Vista / vista materializada |
| OLAP in-memory | CUBE sobre Parquet | DuckDB en JupyterLab |
| Data Lake | zonas `lake-raw` y `lake-curated` | MinIO (S3) |
| Hadoop | Procesamiento sobre Parquet en S3 | DuckDB + pandas (ver *Extensiones*) |
| Machine Learning | Riesgo de atraso por despacho → vuelve al DW | scikit-learn |
| Streaming Data / Alertas / Monitoreo | Reglas LOW_BATTERY, OVERHEAT, OVERSPEED | `lp/consumer.py` + Grafana |
| Análisis histórico / Informes operativos | Dashboards | Grafana |
| Autoservicio, Analítica avanzada | SQL con rol `lp_reader`, notebook | psql / Jupyter |

## Arranque

```bash
make all        # up + seed + etl + ml + stream  (primera vez: ~5-8 min por las imágenes)
make lab        # opcional: JupyterLab en :8888
```

Paso a paso: `make up` → `make seed` → `make etl` → `make ml` → `make stream`.

| Servicio | Puerto | Credenciales |
|---|---|---|
| Grafana (histórico + tiempo real) | 3000 | admin / admin (lectura anónima habilitada) |
| MinIO Console | 9001 | minioadmin / minioadmin123 |
| Postgres | 5432 | `lp`/`lp_secret` (escritura), `lp_reader`/`lp_reader` (lectura) |
| JupyterLab | 8888 | sin token |

Dashboards: **Histórico y operación** (home) y **Monitoreo en tiempo real** (refresco 5 s).

## Qué ver (guion de clase)

1. `make psql` → `SELECT * FROM semantic.mv_cube_otif;` Las filas con columnas NULL son los cortes del cubo.
2. Grafana → *Atraso medio vs lluvia*: el clima viene del Data Lake, no del ODS.
3. Detén el ETL y vuelve a correrlo: es idempotente (`ON CONFLICT DO UPDATE`).
4. `make ml` y revisa *Riesgo de atraso en tránsito*: DW → ML → DW.
5. `make stream` y mira alertas aparecer en vivo; el histórico de telemetría queda en `lake-raw/telemetry/dt=…/hh=…/`.
6. `make lab` → notebook `01_olap_en_memoria` (CUBE en memoria sobre Parquet + reseñas vs puntualidad).

## Datos sintéticos

`SEED`, `N_ORDERS`, `DAYS` son variables de entorno de `lp.seed`. Con los valores por defecto la tasa de atraso es ~15 % y depende de lluvia y transportista (RapidoCosta es el peor, Flota Propia el mejor), así que los dashboards y el modelo tienen señal real. El AUC del modelo es moderado (~0.7) a propósito: el atraso incluye incidentes aleatorios.

## Extensiones sugeridas (deberes)

- Reemplazar la capa "Hadoop" por Spark (`bitnami/spark`) leyendo `s3a://lake-curated/`.
- SCD tipo 2 en `dim_customer` / `dim_carrier`.
- Carga incremental con marca de agua en `etl_dw.sql`.
- Integrar `PULSEFLEET`: publicar su telemetría en el mismo tópico `amr.telemetry`.
- Pruebas de calidad de datos (Data Trust Gate) sobre `staging.*` antes de cargar el DW.

Manual completo de instalación y solución de problemas: [MANUAL_INSTALACION.md](MANUAL_INSTALACION.md).

## Notas de validación

Probado: scripts SQL, seed, ETL, ML, consultas de ambos dashboards (con el rol de solo lectura) y reglas de alertas contra Postgres 16 real y un S3 simulado. **No probado en esta entrega**: construcción de imágenes Docker, Redpanda (producer/consumer) y arranque de Grafana/MinIO; se validan al primer `make all`.
