"""Spark session factory với OpenLineage integration."""
from pyspark.sql import SparkSession
import os


def get_spark_session(app_name: str, enable_lineage: bool = True) -> SparkSession:
    """
    Tạo SparkSession đã tích hợp OpenLineage listener.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Tối ưu Spark
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    )

    if enable_lineage:
        marquez_url = os.getenv("MARQUEZ_URL", "http://marquez:5000")
        namespace   = os.getenv("OPENLINEAGE_NAMESPACE", "production")
        builder = (
            builder
            .config("spark.extraListeners",
                    "io.openlineage.spark.agent.OpenLineageSparkListener")
            .config("spark.openlineage.transport.url", marquez_url)
            .config("spark.openlineage.transport.endpoint", "api/v1/lineage")
            .config("spark.openlineage.namespace", namespace)
        )

    return builder.getOrCreate()
