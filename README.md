# Production Data Pipeline

> Medallion Architecture with Spark + Airflow + Python

## Tech Stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow 2.9 |
| Processing | Apache Spark 3.5 + Delta Lake |
| Data Quality | Great Expectations 0.18 |
| Data Lineage | OpenLineage + Marquez |
| Monitoring | Prometheus + Grafana |
| Storage | MinIO (local S3) |
| Infra | Docker Compose |

## Quick Start

```bash
# 1. Clone & setup
cp .env.example .env
make init

# 2. Start all services
make up

# 3. Run tests
make test

# 4. Access UIs
# Airflow:    http://localhost:8080   (admin/admin)
# Marquez:    http://localhost:3000
# Grafana:    http://localhost:3001   (admin/admin)
```

## Architecture

```
Source → Bronze (raw) → [Quality Check] → Silver (clean) → Gold (aggregated) → BI/ML
              ↑                                                     ↑
         OpenLineage tracks lineage across all layers          Metrics → Prometheus → Grafana
```

## Project Structure

```
.
├── dags/               # Airflow DAGs
├── src/
│   ├── bronze/         # Ingest raw data
│   ├── silver/         # Cleanse & transform
│   ├── gold/           # Aggregate for BI/ML
│   ├── quality/        # Great Expectations checks
│   ├── monitoring/     # StatsD metrics emitter
│   └── utils/          # SparkSession, alerts
├── jobs/               # Spark job entrypoints
├── config/             # Service configurations
├── tests/              # Unit & integration tests
└── docker-compose.yml
```
