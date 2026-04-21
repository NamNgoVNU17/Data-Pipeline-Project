"""
Quality Layer: Great Expectations với PySpark.
Validate dữ liệu trước khi promote từ Bronze → Silver.
"""
import logging
from typing import Optional
from pyspark.sql import DataFrame
from great_expectations.dataset import SparkDFDataset

logger = logging.getLogger(__name__)


class DataQualityChecker:
    """
    Wrapper quanh Great Expectations cho PySpark DataFrame.
    Raise ValueError nếu quality check thất bại.
    """

    RULES: dict[str, list[dict]] = {
        "orders": [
            {"method": "expect_column_to_exist",                   "kwargs": {"column": "order_id"}},
            {"method": "expect_column_values_to_not_be_null",      "kwargs": {"column": "order_id"}},
            {"method": "expect_column_values_to_be_unique",        "kwargs": {"column": "order_id"}},
            {"method": "expect_column_values_to_not_be_null",      "kwargs": {"column": "customer_id"}},
            {"method": "expect_column_values_to_be_between",       "kwargs": {"column": "amount", "min_value": 0, "max_value": 10_000_000}},
            {"method": "expect_column_values_to_not_be_null",      "kwargs": {"column": "order_date"}},
            {"method": "expect_column_values_to_be_in_set",        "kwargs": {"column": "status", "value_set": ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"]}},
            {"method": "expect_table_row_count_to_be_between",     "kwargs": {"min_value": 1, "max_value": 10_000_000}},
        ],
        "customers": [
            {"method": "expect_column_to_exist",                   "kwargs": {"column": "customer_id"}},
            {"method": "expect_column_values_to_not_be_null",      "kwargs": {"column": "customer_id"}},
            {"method": "expect_column_values_to_be_unique",        "kwargs": {"column": "customer_id"}},
            {"method": "expect_column_values_to_match_regex",      "kwargs": {"column": "email", "regex": r"[^@]+@[^@]+\.[^@]+"}},
        ],
    }

    def __init__(self, dataset_name: str, threshold: float = 0.95):
        self.dataset_name = dataset_name
        self.threshold    = threshold  # tỷ lệ expectation pass tối thiểu

    def validate(self, df: DataFrame, partition_date: str) -> dict:
        """
        Chạy toàn bộ expectations cho dataset.
        Returns: validation result dict.
        Raises: ValueError nếu quality < threshold.
        """
        rules = self.RULES.get(self.dataset_name, [])
        if not rules:
            logger.warning(f"[QUALITY] No rules defined for dataset '{self.dataset_name}'. Skipping.")
            return {"success": True, "skipped": True}

        ge_df = SparkDFDataset(df)
        results = []

        for rule in rules:
            method = getattr(ge_df, rule["method"])
            result = method(**rule["kwargs"])
            results.append(result)
            status = "✅" if result["success"] else "❌"
            logger.info(f"[QUALITY] {status} {rule['method']}({rule['kwargs']})")

        passed       = sum(1 for r in results if r["success"])
        total        = len(results)
        pass_rate    = passed / total if total > 0 else 0.0
        overall_pass = pass_rate >= self.threshold

        summary = {
            "dataset":        self.dataset_name,
            "partition_date": partition_date,
            "total_checks":   total,
            "passed":         passed,
            "failed":         total - passed,
            "pass_rate":      round(pass_rate, 4),
            "threshold":      self.threshold,
            "success":        overall_pass,
        }

        logger.info(f"[QUALITY] Summary: {summary}")

        if not overall_pass:
            raise ValueError(
                f"[QUALITY] FAILED for '{self.dataset_name}' on {partition_date}. "
                f"Pass rate: {pass_rate:.1%} < threshold {self.threshold:.1%}"
            )

        return summary


def run_quality_check(df: DataFrame, dataset_name: str, partition_date: str,
                      threshold: float = 0.95) -> dict:
    """Entrypoint để gọi từ Airflow PythonOperator."""
    checker = DataQualityChecker(dataset_name, threshold)
    return checker.validate(df, partition_date)
