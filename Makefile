DC = docker compose

.PHONY: up seed etl ml stream lab all down reset psql logs

up:            ## Postgres + MinIO + Grafana
	$(DC) up -d --build postgres minio minio-init grafana

seed:          ## Fuentes sintéticas -> ODS/Master (Postgres) y Data Lake raw
	$(DC) run --rm --build seed

etl:           ## ETL en lotes -> DW + marts + cubo + parquet curated
	$(DC) run --rm etl

ml:            ## Entrena y puntúa riesgo de atraso (vuelve al DW)
	$(DC) run --rm ml

stream:        ## Sensores -> Redpanda -> ODS / Lake / alertas en tiempo real
	$(DC) --profile streaming up -d --build

lab:           ## JupyterLab con DuckDB sobre el lake (puerto 8888)
	$(DC) --profile lab up -d jupyter

all: up seed etl ml stream   ## Todo de punta a punta

down:
	$(DC) --profile streaming --profile lab --profile jobs down

reset:         ## Borra volúmenes (empieza de cero)
	$(DC) --profile streaming --profile lab --profile jobs down -v

psql:
	$(DC) exec postgres psql -U lp -d logistpulse

logs:
	$(DC) --profile streaming logs -f producer consumer
