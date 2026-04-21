"""Unit tests cho Data Quality layer."""
import pytest
from unittest.mock import MagicMock, patch
from src.quality.expectations import DataQualityChecker


class TestDataQualityChecker:

    def test_no_rules_skips_validation(self):
        """Dataset không có rules thì skip."""
        checker = DataQualityChecker("unknown_dataset")
        mock_df = MagicMock()
        result = checker.validate(mock_df, "2024-01-01")
        assert result["skipped"] is True
        assert result["success"] is True

    def test_pass_rate_below_threshold_raises(self):
        """Pass rate < threshold thì raise ValueError."""
        checker = DataQualityChecker("orders", threshold=0.99)

        mock_ge_df = MagicMock()
        # Giả lập tất cả expectations fail
        mock_ge_df.expect_column_to_exist.return_value            = {"success": False}
        mock_ge_df.expect_column_values_to_not_be_null.return_value = {"success": False}
        mock_ge_df.expect_column_values_to_be_unique.return_value = {"success": True}
        mock_ge_df.expect_column_values_to_be_between.return_value= {"success": True}
        mock_ge_df.expect_column_values_to_be_in_set.return_value = {"success": False}
        mock_ge_df.expect_table_row_count_to_be_between.return_value = {"success": True}

        with patch("src.quality.expectations.SparkDFDataset", return_value=mock_ge_df):
            with pytest.raises(ValueError, match="FAILED"):
                checker.validate(MagicMock(), "2024-01-01")

    def test_all_pass_returns_success(self):
        """Tất cả expectations pass thì trả về success."""
        checker = DataQualityChecker("orders", threshold=0.95)
        mock_ge_df = MagicMock()
        mock_ge_df.expect_column_to_exist.return_value              = {"success": True}
        mock_ge_df.expect_column_values_to_not_be_null.return_value = {"success": True}
        mock_ge_df.expect_column_values_to_be_unique.return_value   = {"success": True}
        mock_ge_df.expect_column_values_to_be_between.return_value  = {"success": True}
        mock_ge_df.expect_column_values_to_be_in_set.return_value   = {"success": True}
        mock_ge_df.expect_table_row_count_to_be_between.return_value= {"success": True}

        with patch("src.quality.expectations.SparkDFDataset", return_value=mock_ge_df):
            result = checker.validate(MagicMock(), "2024-01-01")

        assert result["success"] is True
        assert result["pass_rate"] == 1.0
