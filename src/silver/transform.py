"""
Silver Layer: Cleanse, deduplicate, standardize schema.
"""
import sys, logging
from pyspark.sql import functions as F, DataFrame
from pyspark.sql.types import TimestampType, DecimalType
from src.utils.spark_session import get_spark_session
from src.quality.expectations import run_quality_check

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def transform_orders_silver(bronze_path: str, silver_path: str, partition_date: str) -> dict:
    spark = get_spark_session("silver_transform_orders")

    logger.info(f"[SILVER] Reading bronze: {bronze_path}")
    df_bronze = spark.read.format("delta").load(bronze_path)

    # 1. Deduplicate
    df = df_bronze.dropDuplicates(["order_id"])
    dup_count = df_bronze.count() - df.count()
    logger.info(f"[SILVER] Removed {dup_count} duplicates")

    # 2. Cleanse & cast types
    df = (
        df
        .withColumn("order_id",    F.col("order_id").cast("string").alias("order_id"))
        .withColumn("customer_id", F.col("customer_id").cast("string"))
        .withColumn("amount",      F.col("amount").cast(DecimalType(18, 2)))
        .withColumn("order_date",  F.to_timestamp("order_date", "yyyy-MM-dd"))
        .withColumn("status",      F.upper(F.trim(F.col("status"))))
        # Fill nulls
        .fillna({"status": "UNKNOWN", "amount": 0.0})
        # Drop bronze metadata
        .drop("_ingested_at", "_source_path", "_pipeline_run")
        # Add silver metadata
        .withColumn("_silver_at",       F.current_timestamp())
        .withColumn("_partition_dt",    F.lit(partition_date))
    )

    # 3. Quality check TRƯỚC khi ghi vào Silver
    run_quality_check(df, "orders", partition_date, threshold=0.95)

    # 4. Ghi vào Silver (Delta)
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("_partition_dt")
        .option("overwriteSchema", "true")
        .save(silver_path)
    )

    row_count = df.count()
    logger.info(f"[SILVER] Written {row_count} rows to {silver_path}")
    spark.stop()
    return {"row_count": row_count, "path": silver_path}


if __name__ == "__main__":
    bronze_path    = sys.argv[1]  # s3a://datalake/bronze/orders/dt=2024-01-01
    silver_path    = sys.argv[2]  # s3a://datalake/silver/orders
    partition_date = sys.argv[3]
    transform_orders_silver(bronze_path, silver_path, partition_date)
