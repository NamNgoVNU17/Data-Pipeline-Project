.PHONY: up down init logs test lint clean

# ── Infrastructure ──────────────────────────────────────────
up:
	@echo "Starting all services..."
	docker compose up -d
	@echo "Waiting for Airflow to be ready..."
	@sleep 30
	@echo "\n✅ Services ready:"
	@echo "  Airflow:    http://localhost:8080  (admin/admin)"
	@echo "  Spark UI:   http://localhost:8181"
	@echo "  Marquez:    http://localhost:3000"
	@echo "  Grafana:    http://localhost:3001  (admin/admin)"
	@echo "  Prometheus: http://localhost:9090"
	@echo "  MinIO:      http://localhost:9001  (minioadmin/minioadmin)"

down:
	docker compose down -v

restart:
	docker compose restart

logs:
	docker compose logs -f airflow-scheduler

# ── Init ────────────────────────────────────────────────────
init:
	@echo "Initializing project..."
	cp .env.example .env
	mkdir -p data/raw/orders logs/airflow
	# Create MinIO buckets
	docker compose exec minio mc alias set local http://localhost:9000 minioadmin minioadmin
	docker compose exec minio mc mb local/datalake --ignore-existing
	@echo "✅ Project initialized"

# ── Development ─────────────────────────────────────────────
test:
	pytest tests/ -v --cov=src --cov-report=term-missing --cov-report=html:htmlcov

lint:
	@echo "Running linters..."
	ruff check src/ dags/ tests/
	mypy src/ --ignore-missing-imports

format:
	ruff format src/ dags/ tests/

# ── Spark Jobs ───────────────────────────────────────────────
submit-bronze:
	docker compose exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		--jars /opt/spark/libs/delta-core_2.12-2.4.0.jar,/opt/spark/libs/openlineage-spark_2.12-1.14.0.jar \
		/opt/spark/jobs/bronze_ingest.py $(SOURCE) $(DATASET) $(DATE)

# ── Cleanup ──────────────────────────────────────────────────
clean:
	docker compose down -v
	rm -rf logs/airflow/* htmlcov .coverage
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; true
