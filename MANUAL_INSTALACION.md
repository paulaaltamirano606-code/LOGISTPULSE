# Manual de instalación — LOGISTPULSE Lakehouse

Plataforma Data Warehouse + Data Lake para LOGISTPULSE. Se instala con Docker Compose, en **GitHub Codespaces** (recomendado para estudiantes) o en una **máquina local**.

## 1. Requisitos

| Recurso | Mínimo | Recomendado |
|---|---|---|
| CPU | 2 núcleos | 4 núcleos |
| RAM | 6 GB libres | 8 GB |
| Disco | 6 GB | 10 GB |
| Software | Docker Engine 24+ con Compose v2, `make`, `git` | — |

Los estudiantes **no necesitan** cuenta de Claude ni de ningún servicio de IA; todo corre con contenedores públicos.

## 2. Opción A — GitHub Codespaces

1. Sube el contenido de la carpeta `logistpulse-lakehouse/` a un repositorio nuevo de GitHub (la raíz del repo debe contener `docker-compose.yml`, `Makefile` y `.devcontainer/`).
2. En GitHub: **Code → Codespaces → Create codespace on main**. Elige una máquina de **4 núcleos / 8 GB** (el archivo `devcontainer.json` ya lo solicita).
3. Espera a que el contenedor termine de construirse (Docker-in-Docker queda habilitado automáticamente).
4. En la terminal del Codespace:
   ```bash
   docker compose version     # debe responder v2.x
   make all
   ```
5. Abre la pestaña **Ports** y haz clic en el puerto **3000** (Grafana).

## 3. Opción B — Máquina local (Linux, macOS o Windows con WSL2)

1. Instala Docker Desktop (macOS/Windows) o Docker Engine + plugin Compose (Linux). En Windows activa la integración con WSL2 y trabaja dentro de la terminal de WSL.
2. Descomprime el proyecto y entra a la carpeta:
   ```bash
   unzip logistpulse-lakehouse.zip
   cd logistpulse-lakehouse
   ```
3. Instala `make` si no lo tienes (`sudo apt install make` / `xcode-select --install`).
4. Ejecuta:
   ```bash
   make all
   ```
   La primera vez descarga imágenes y construye la imagen de Python: **5–8 minutos**.

> Sin `make`: ejecuta manualmente `docker compose up -d --build postgres minio minio-init grafana`, luego `docker compose run --rm --build seed`, `docker compose run --rm etl`, `docker compose run --rm ml` y `docker compose --profile streaming up -d --build`.

## 4. Instalación paso a paso (equivale a `make all`)

| Paso | Comando | Qué hace | Salida esperada |
|---|---|---|---|
| 1 | `make up` | Levanta Postgres, MinIO y Grafana; crea esquemas, tablas, vistas y el rol `lp_reader` | Contenedores `healthy` |
| 2 | `make seed` | Genera datos sintéticos en ODS/Master y en el Data Lake (raw) | `[seed] pedidos=4000 despachos≈3900 …` |
| 3 | `make etl` | ETL en lotes → Data Warehouse, cubo OLAP y Parquet curado | `[etl] … dw.fact_shipment≈3900 …` |
| 4 | `make ml` | Entrena el modelo de riesgo de atraso y puntúa envíos en tránsito | `[ml] hgb-…: AUC≈0.7 …` |
| 5 | `make stream` | Levanta Redpanda, productor y consumidor de telemetría | Contenedores `producer` y `consumer` en ejecución |
| 6 (opcional) | `make lab` | JupyterLab con DuckDB sobre el lake | Puerto 8888 |

Los pasos 2 a 4 se pueden repetir cuando quieras: el seed reinicia los datos y el ETL es idempotente.

## 5. Verificación

| Servicio | URL (local) | Credenciales | Qué comprobar |
|---|---|---|---|
| Grafana | http://localhost:3000 | admin / admin | Dashboards en la carpeta *LOGISTPULSE*; el de tiempo real muestra alertas a los pocos minutos |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin123 | Buckets `lake-raw` y `lake-curated` con archivos |
| Postgres | `localhost:5432`, BD `logistpulse` | `lp`/`lp_secret` (escritura); `lp_reader`/`lp_reader` (lectura) | `make psql` |
| JupyterLab | http://localhost:8888 | sin token | Notebook `01_olap_en_memoria` |

Comprobaciones rápidas:

```bash
make psql
logistpulse=# SELECT carrier, otif_pct FROM semantic.mv_cube_otif
              WHERE carrier IS NOT NULL AND year IS NULL AND depot IS NULL AND segment IS NULL;
logistpulse=# SELECT COUNT(*) FROM ods.alerts;      -- crece cuando el streaming está activo
```

En MinIO, la telemetría histórica aparece en `lake-raw/telemetry/dt=AAAA-MM-DD/hh=HH/` cada ~30 s.

## 6. Operación diaria

| Acción | Comando |
|---|---|
| Ver logs del streaming | `make logs` |
| Apagar todo (conserva datos) | `make down` |
| Borrar todo y empezar de cero | `make reset` y luego `make all` |
| Cambiar volumen de datos | `N_ORDERS=8000 DAYS=240 make seed` y luego `make etl ml` |

## 7. Solución de problemas

| Síntoma | Causa probable | Solución |
|---|---|---|
| `Cannot connect to the Docker daemon` | Docker apagado o sin permisos | Inicia Docker Desktop / `sudo systemctl start docker`; añade tu usuario al grupo `docker` |
| Fallos de red al descargar imágenes dentro de Codespaces (DNS, *pull access denied*, tiempos de espera) | Red anidada de Docker-in-Docker | Reintenta `docker compose pull`; si persiste, reconstruye el Codespace (**Rebuild Container**) o usa la opción local |
| `port is already allocated` (5432, 3000, 9000…) | Otro servicio usa ese puerto | Detén el otro servicio o cambia el puerto de la izquierda en `docker-compose.yml` (p. ej. `"5433:5432"`) |
| Contenedor `redpanda` reiniciándose o *unhealthy* | Memoria insuficiente | Sube la RAM de Docker a 8 GB; como alternativa, omite `make stream` (el DW y los dashboards históricos funcionan sin streaming) |
| `minio` *unhealthy* | Versión de imagen sin `mc ready` | En `docker-compose.yml` fija una versión reciente de `minio/minio` o reemplaza el healthcheck por `curl -f http://localhost:9000/minio/health/live` |
| `seed`/`etl` fallan con *connection refused* | Postgres o MinIO aún arrancando | Espera 20 s y repite; confirma con `docker compose ps` |
| `[ml] pocos datos históricos` | Se ejecutó `ml` antes de `seed` y `etl` | Ejecuta `make seed etl ml` en ese orden |
| Grafana sin datos en el panel de tiempo real | Streaming apagado | `make stream`; espera 1–2 minutos |
| Cambiaste los scripts SQL y no se reflejan | Postgres solo ejecuta `sql/` en el primer arranque | `make reset` y `make all` |
| Grafana muestra *datasource not found* | Provisioning no cargó | `docker compose restart grafana` y revisa `docker compose logs grafana` |

## 8. Desinstalación

```bash
make reset                    # elimina contenedores y volúmenes (datos)
docker image rm logistpulse-tools
```

En Codespaces basta con eliminar el Codespace desde GitHub.

## 9. Seguridad (uso académico)

Las contraseñas incluidas son de laboratorio y Grafana permite lectura anónima. **No expongas estos puertos a Internet.** Para producción cambia credenciales, quita el acceso anónimo y no publiques el puerto 5432.

## 10. Estado de validación

Probado antes de la entrega: scripts SQL, seed, ETL, ML, las consultas de ambos dashboards (con el rol de solo lectura) y las reglas de alerta, contra Postgres 16 real y un S3 simulado. **Aún no probado:** construcción de imágenes Docker, Redpanda, MinIO y Grafana reales. Si algo falla en el primer arranque, revisa la sección 7 y envía la salida de `docker compose logs <servicio>`.
