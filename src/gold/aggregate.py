"""
Gold Layer: Business-logic aggregations cho BI / ML.
"""
import sys, logging
from pyspark.sql import functions as F
from src.utils.spark_session import get_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def build_daily_order_summary(silver_path: str, gold_path: str, partition_date: str) -> dict:
    """
    Aggregate daily order metrics từ Silver → Gold.
    Output: bảng daily_order_summary phục vụ BI dashboard.
    """
    spark = get_spark_session("gold_daily_order_summary")

    df_silver = (
        spark.read.format("delta").load(silver_path)
        .filter(F.col("_partition_dt") == partition_date)
    )

    df_gold = (
        df_silver
        .groupBy(
            F.to_date("order_date").alias("order_date"),
            "status",
        )
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("amount").alias("total_revenue"),
            F.avg("amount").alias("avg_order_value"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
        .withColumn("_gold_at",       F.current_timestamp())
        .withColumn("_partition_dt",  F.lit(partition_date))
    )

    (
        df_gold.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("_partition_dt")
        .option("mergeSchema", "true")
        .save(gold_path)
    )

    row_count = df_gold.count()
    logger.info(f"[GOLD] Written {row_count} rows to {gold_path}")
    spark.stop()
    return {"row_count": row_count, "path": gold_path}


if __name__ == "__main__":
    silver_path    = sys.argv[1]
    gold_path      = sys.argv[2]
    partition_date = sys.argv[3]
    build_daily_order_summary(silver_path, gold_path, partition_date)
