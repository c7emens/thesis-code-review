# Thesis code: vessel and helicopter maintenance-event detection at US East Coast offshore wind farms

Source archive accompanying the Bachelor's thesis of the same title. Detects
vessel and helicopter maintenance events at four US East Coast offshore wind
farms (Block Island, South Fork, Revolution Wind, Vineyard Wind) from public
AIS (NOAA MarineCadastre) and ADS-B (OpenSky Network), Jan 2024 – Jun 2025.


## Layout

    ch3_data_methodology/   downloaders, ingestors, file helpers
    ch4_implementation/     Stage 1 – Stage 4 pipeline
    ch5_results/            figure + table generators, MFR validator
    utilities/              ad-hoc query and maintenance scripts
    schema/init.sql         database schema (TimescaleDB + PostGIS)
    data/sample/            1-day AIS sample + turbine inventory


## Setup

Python 3.11+, Docker, ~10 GB free disk for the test stack.

    # 1. Storage (TimescaleDB + PostGIS)
    docker compose up -d

    # 2. Schema (apply after container has settled, ~10 s)
    make schema

    # 3. Python env
    python3 -m venv venv && source venv/bin/activate
    pip install -r requirements.txt

    # 4. Env file (OpenSky credentials — register at
    #    https://opensky-network.org/index.php?option=com_users)
    cp .env.example .env
    # edit .env: OPENSKY_USERNAME, OPENSKY_PASSWORD


## Smoke test

Ingests the bundled 1-day AIS sample, helicopter-tracks sample, and turbine
inventory, then runs Stage 1 (vessel tripwire) and Stage 3 (helicopter event
classifier) to verify both pipelines execute end-to-end against the test
database.

    make smoke-test


## Live helicopter pipeline

The smoke test runs Stage 3 helicopter against the bundled `heli_sample.csv.zst`
(Stage 2 output pre-extracted from the production database). To exercise the
full helicopter pipeline against fresh OpenSky data:

    make stage1-heli      # query OpenSky Trino for candidate (icao24, date) pairs
    make stage2-heli      # fetch full tracks for each candidate (rate-limited)
    make stage3-heli      # classify into maintenance events

Or chained:

    make live-heli

Requires a free OpenSky research account
(https://opensky-network.org/index.php?option=com_users, registration ~5
minutes); credentials in `.env` (see [Setup](#setup) step 4). Stage 1 and
Stage 2 fetch over HTTPS from OpenSky Trino and are subject to per-account
rate limits — expect full-year runs to take hours.


## Data sources

| Source     | Licence        | URL                                                |
|------------|----------------|----------------------------------------------------|
| NOAA AIS   | NOAA open data | https://coast.noaa.gov/htdata/CMSP/AISDataHandler/ |
| OpenSky    | research-only  | https://opensky-network.org/data/historical-data   |
| ICOADS R3  | NOAA open data | https://www.ncei.noaa.gov/products/icoads          |
| OSM        | ODbL           | https://www.openstreetmap.org/                     |
| GEM WPT    | CC BY 4.0      | https://globalenergymonitor.org/                   |
| OurAirports| public domain  | https://ourairports.com/data/                      |
| NGA WPI    | US gov         | https://msi.nga.mil/Publications/WPI               |


## Configuration

Database connection is hardcoded in `pipeline_common.py` (`DB_CONFIG`) at
`localhost:5432`, db `windfarm`, user `thesis`, password `thesis2026` — for
research purposes only. OpenSky credentials are read from `.env`, which is
excluded by `.gitignore` and must not be committed: OpenSky accounts are
per-individual under their Terms of Service, and credentials pushed to a
public repository are harvested by automated scrapers within hours.

Most ingestion scripts default to `/mnt/e/data_lake/...` paths matching the
author's local environment. Override by passing `--input-dir` / `--out-dir`
flags or by symlinking to the expected paths.


## Reproducibility tier

| Tier                       | Status          | Notes                            |
|----------------------------|-----------------|----------------------------------|
| Code inspection            | supported       | —                                |
| Pipeline run, sample data  | supported       | `make smoke-test`                |
| Pipeline run, full corpus  | supported       | requires multi-day ingest        |
| Validation against MFR     | not supported   | HeliService USA proprietary data |

The Master Flight Report (MFR) used for helicopter precision/recall
validation is industry-partner data under non-disclosure and is not
distributable. Pipeline output is reproducible from public sources alone;
the F1/precision/recall numbers in the thesis are not independently
reproducible without MFR access.


## Citation

    @misc{fritzsche2026_thesiscode,
      author = {Fritzsche, Clemens},
      title  = {Thesis code: vessel and helicopter maintenance-event detection
                at US East Coast offshore wind farms},
      year   = {2026},
      url    = {https://github.com/c7emens/thesis-code-review},
    }
