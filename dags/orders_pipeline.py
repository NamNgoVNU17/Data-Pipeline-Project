"""
DAG: orders_pipeline
Orchestrates Bronze → Quality → Silver → Gold flow cho Orders dataset.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup

from src.utils.alert import airflow_on_failure_callback, airflow_sla_miss_callback

# ------------------------------------------------------------------ Config ---
DATASET      = "orders"
SPARK_CONN   = "spark_default"
PARTITION_DT = "{{ ds }}"   # YYYY-MM-DD từ execution_date

BRONZE_PATH = f"s3a://datalake/bronze/{DATASET}/dt={PARTITION_DT}"
SILVER_PATH = f"s3a://datalake/silver/{DATASET}"
GOLD_PATH   = f"s3a://datalake/gold/daily_order_summary"

SPARK_JARS = (
    "/opt/spark/libs/delta-core_2.12-2.4.0.jar,"
    "/opt/spark/libs/openlineage-spark_2.12-1.14.0.jar"
)

SPARK_CONF = {
    "spark.openlineage.transport.url":      "http://marquez:5000",
    "spark.openlineage.namespace":          "production",
    "spark.sql.extensions":                 "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog":      "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.hadoop.fs.s3a.endpoint":         "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key":       "minioadmin",
    "spark.hadoop.fs.s3a.secret.key":       "minioadmin",
    "spark.hadoop.fs.s3a.path.style.access": "true",
}

# ----------------------------------------------------------------- Default args ---
default_args = {
    "owner":            "data-team",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
    "on_failure_callback": airflow_on_failure_callback,
    "email_on_failure": False,
}

# ----------------------------------------------------------------- Helper tasks ---
def _check_source_data(**context) -> str:
    """Kiểm tra source data tồn tại; branch sang skip nếu không có data."""
    partition_date = context["ds"]
    source_path = f"/opt/airflow/data/raw/{DATASET}/{partition_date}.csv"
    if os.path.exists(source_path):
        return "ingest_bronze"
    return "skip_no_source"


def _emit_pipeline_metrics(**context) -> None:
    """Push custom metrics sau khi pipeline hoàn thành."""
    from src.monitoring.metrics import emit_row_count
    stats = context["ti"].xcom_pull(task_ids="transform_silver", key="stats")
    if stats:
        emit_row_count(DATASET, "silver", stats.get("row_count", 0))


# ----------------------------------------------------------------- DAG definition ---
with DAG(
    dag_id="orders_pipeline",
    description="Bronze → Quality → Silver → Gold for orders dataset",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["orders", "medallion", "production"],
    default_args=default_args,
    sla_miss_callback=airflow_sla_miss_callback,
    doc_md="""
    ## Orders Pipeline
    **Medallion Architecture**: Bronze → Silver → Gold

    ### Flow
    1. `check_source` — kiểm tra nguồn dữ liệu có sẵn
    2. `ingest_bronze` — ingest raw CSV vào Delta Bronze
    3. `quality_check_bronze` — Great Expectations validation
    4. `transform_silver` — cleanse, deduplicate, type casting
    5. `build_gold` — aggregate daily summary
    6. `emit_metrics` — push metrics tới Prometheus
    """,
) as dag:

    # ── 0. Branch: kiểm tra source ---
    check_source = BranchPythonOperator(
        task_id="check_source",
        python_callable=_check_source_data,
    )

    skip_no_source = EmptyOperator(task_id="skip_no_source")

    # ── 1. Bronze Ingest ---
    ingest_bronze = SparkSubmitOperator(
        task_id="ingest_bronze",
        application="/opt/spark/jobs/bronze_ingest.py",
        application_args=[
            f"/opt/airflow/data/raw/{DATASET}/{PARTITION_DT}.csv",
            DATASET,
            PARTITION_DT,
        ],
        conn_id=SPARK_CONN,
        jars=SPARK_JARS,
        conf=SPARK_CONF,
        name=f"bronze_ingest_{DATASET}",
        verbose=False,
        sla=timedelta(minutes=30),
    )

    # ── 2. Quality check Bronze ---
    with TaskGroup("quality_bronze") as quality_bronze_group:
        def _run_quality_bronze(**context) -> dict:
            from pyspark.sql import SparkSession
            from src.quality.expectations import run_quality_check
            from src.utils.spark_session import get_spark_session
            spark = get_spark_session("quality_check_bronze")
            df = spark.read.format("delta").load(BRONZE_PATH)
            result = run_quality_check(df, DATASET, context["ds"])
            spark.stop()
            return result

        quality_bronze = PythonOperator(
            task_id="validate_bronze",
            python_callable=_run_quality_bronze,
            sla=timedelta(minutes=15),
        )

    # ── 3. Silver Transform ---
    transform_silver = SparkSubmitOperator(
        task_id="transform_silver",
        application="/opt/spark/jobs/silver_transform.py",
        application_args=[BRONZE_PATH, SILVER_PATH, PARTITION_DT],
        conn_id=SPARK_CONN,
        jars=SPARK_JARS,
        conf=SPARK_CONF,
        name=f"silver_transform_{DATASET}",
        verbose=False,
        sla=timedelta(hours=1),
    )

    # ── 4. Gold Aggregate ---
    build_gold = SparkSubmitOperator(
        task_id="build_gold",
        application="/opt/spark/jobs/gold_aggregate.py",
        application_args=[SILVER_PATH, GOLD_PATH, PARTITION_DT],
        conn_id=SPARK_CONN,
        jars=SPARK_JARS,
        conf=SPARK_CONF,
        name=f"gold_aggregate_{DATASET}",
        verbose=False,
        sla=timedelta(minutes=30),
    )

    # ── 5. Emit Metrics ---
    emit_metrics = PythonOperator(
        task_id="emit_metrics",
        python_callable=_emit_pipeline_metrics,
        trigger_rule="all_done",
    )

    # ── DAG Dependencies ---
    check_source >> [ingest_bronze, skip_no_source]
    ingest_bronze >> quality_bronze_group >> transform_silver >> build_gold >> emit_metrics
