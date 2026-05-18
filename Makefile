PY ?= python3
VENV ?= venv
PIP := $(VENV)/bin/pip
PYBIN := $(VENV)/bin/python
PYPATH := PYTHONPATH=ch4_implementation/stage3:ch3_data_methodology

SAMPLE_AIS := data/sample/ais_sample.csv.zst
SAMPLE_TURBINES := data/sample/turbines.csv
SAMPLE_HELI := data/sample/heli_sample.csv.zst
SAMPLE_DATE := 2024-08-15
NEXT_DATE := 2024-08-16


.PHONY: up down reset schema venv install ingest-sample stage1 ingest-heli stage1-heli stage2-heli stage3-heli smoke-test live-heli

up:
	docker compose up -d
	@echo "Waiting for database to be ready..."
	@until docker compose exec -T timescaledb pg_isready -U thesis -d windfarm 2>/dev/null; do sleep 1; done
	@sleep 10   # settle time before schema apply

down:
	docker compose down

reset:
	docker compose down -v

schema:
	docker compose exec -T timescaledb \
	  psql -U thesis -d windfarm -v ON_ERROR_STOP=1 < schema/init.sql

venv:
	$(PY) -m venv $(VENV)

install: venv
	$(PIP) install -r requirements.txt

ingest-sample:
	$(PYPATH) $(PYBIN) ch3_data_methodology/ingest/ingest_turbines_csv.py $(SAMPLE_TURBINES)
	$(PYPATH) $(PYBIN) ch3_data_methodology/ingest/ingest_ais_csv.py $(SAMPLE_AIS)

stage1:
	$(PYPATH) $(PYBIN) ch4_implementation/stage1/stage1_vessel_tripwire.py --source local --year 2024

ingest-heli:
	zstd -dc $(SAMPLE_HELI) | docker compose exec -T timescaledb \
	  psql -U thesis -d windfarm -c "\COPY stage2_helicopter_tracks FROM STDIN WITH CSV HEADER"

# Live helicopter pipeline (requires OpenSky credentials in .env)
stage1-heli:
	$(PYPATH) $(PYBIN) ch4_implementation/stage1/stage1_helicopter_tripwire.py \
	  --source local --year 2024

stage2-heli:
	$(PYPATH) $(PYBIN) ch4_implementation/stage2/stage2_helicopter_fetch_tracks.py \
	  --year 2024

stage3-heli:
	$(PYPATH) $(PYBIN) ch4_implementation/stage3/stage3_helicopter_events.py \
	  --start $(SAMPLE_DATE) --end $(NEXT_DATE)

live-heli: stage1-heli stage2-heli stage3-heli

smoke-test: up schema ingest-sample stage1 ingest-heli stage3-heli
	@echo ""
	@echo "Result counts:"
	@docker compose exec -T timescaledb psql -U thesis -d windfarm -c \
	  "SELECT (SELECT count(*) FROM stage1_vessel_hits) AS vessel_hits, \
	          (SELECT count(*) FROM stage3_helicopter_events) AS heli_events;"
