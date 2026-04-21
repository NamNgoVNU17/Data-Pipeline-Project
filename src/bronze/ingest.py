"""
Bronze Layer: Ingest raw data → lưu Parquet/Delta không transform.
Spark job này được submit qua SparkSubmitOperator.
"""
import sys, logging
from datetime import datetime
from pyspark.sql import functions as F
from src.utils.spark_session import get_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def ingest_to_bronze(source_path: str, dataset_name: str, partition_date: str) -> dict:
    """
    Đọc raw data từ source, thêm metadata, ghi vào Bronze (Delta).
    Returns: dict thống kê ingest.
    """
    spark = get_spark_session(f"bronze_ingest_{dataset_name}")

    logger.info(f"[BRONZE] Ingesting {dataset_name} for {partition_date}")

    # 1. Đọc raw (CSV/JSON/Parquet từ S3 / SFTP / API export)
    df_raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(source_path)  # hoặc .json() / .parquet()
    )

    row_count = df_raw.count()
    logger.info(f"[BRONZE] Raw row count: {row_count}")

    # 2. Thêm metadata columns (KHÔNG transform business logic)
    df_bronze = (
        df_raw
        .withColumn("_ingested_at",  F.current_timestamp())
        .withColumn("_source_path",  F.lit(source_path))
        .withColumn("_partition_dt", F.lit(partition_date))
        .withColumn("_pipeline_run", F.lit(f"airflow_{datetime.now().isoformat()}"))
    )

    # 3. Ghi vào Delta Lake (Bronze zone) — append mode giữ lịch sử
    output_path = f"s3a://datalake/bronze/{dataset_name}/dt={partition_date}"
    (
        df_bronze.write
        .format("delta")
        .mode("append")
        .save(output_path)
    )

    logger.info(f"[BRONZE] Written to {output_path}")
    spark.stop()
    return {"dataset": dataset_name, "row_count": row_count, "path": output_path}


if __name__ == "__main__":
    source_path    = sys.argv[1]
    dataset_name   = sys.argv[2]
    partition_date = sys.argv[3]
    result = ingest_to_bronze(source_path, dataset_name, partition_date)
    logger.info(f"[BRONZE] Done: {result}")
