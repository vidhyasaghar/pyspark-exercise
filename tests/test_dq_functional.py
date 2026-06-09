"""
Tests for :mod:`sales_data.dq_checks.dq_functional`.

Uses real local DataFrames (via the shared ``spark`` fixture) and
:mod:`unittest.mock` to assert on the module-level logger.
"""

from unittest.mock import MagicMock, patch

import pytest

import sales_data.dq_checks.dq_functional as dq_functional


def make_df(spark, data, schema):
    """Build a small DataFrame from row tuples using an explicit DDL schema string."""
    return spark.createDataFrame(data, schema)


@pytest.fixture
def mock_logger():
    """Replace the module-level logger with a MagicMock so calls can be asserted on."""
    mock = MagicMock()
    with patch.object(dq_functional, "logger", mock):
        yield mock


class TestCheckCallsSuccessfulGtMade:
    """Tests for :func:`check_calls_successful_gt_made`."""

    def test_returns_true_and_logs_info_when_no_violations(self, spark, mock_logger):
        df = make_df(spark, [(8, 10), (10, 10)], "calls_successful INT, calls_made INT")

        assert dq_functional.check_calls_successful_gt_made(df, "dataset") is True
        mock_logger.info.assert_called_once()
        mock_logger.warning.assert_not_called()

    def test_returns_false_and_logs_warning_when_violations_exist(self, spark, mock_logger):
        df = make_df(spark, [(11, 10), (5, 10)], "calls_successful INT, calls_made INT")

        assert dq_functional.check_calls_successful_gt_made(df, "dataset") is False
        mock_logger.warning.assert_called_once()

    def test_raises_when_halt_on_failure_and_violations_exist(self, spark, mock_logger):
        df = make_df(spark, [(11, 10)], "calls_successful INT, calls_made INT")

        with pytest.raises(RuntimeError, match=r"Dependent attributes check failed in dataset: 1 violation"):
            dq_functional.check_calls_successful_gt_made(df, "dataset", halt_on_failure=True)

    def test_does_not_raise_when_halt_on_failure_and_no_violations(self, spark, mock_logger):
        df = make_df(spark, [(8, 10)], "calls_successful INT, calls_made INT")

        assert dq_functional.check_calls_successful_gt_made(df, "dataset", halt_on_failure=True) is True

    def test_raises_runtime_error_when_columns_do_not_exist(self, spark, mock_logger):
        df = make_df(spark, [(1,)], "id INT")

        with pytest.raises(RuntimeError, match="Error occurred while checking dependent attributes in dataset"):
            dq_functional.check_calls_successful_gt_made(df, "dataset")

        mock_logger.error.assert_called_once()


class TestCheckAddressFormat:
    """Tests for :func:`check_address_format`."""

    def test_returns_true_and_logs_info_when_all_addresses_valid(self, spark, mock_logger):
        df = make_df(
            spark,
            [("Main Street, 12, 1234 AB",), ("Oak Avenue, 5-7, 5678 ZZ",)],
            "address STRING",
        )

        assert dq_functional.check_address_format(df, "dataset") is True
        mock_logger.info.assert_called_once()
        mock_logger.warning.assert_not_called()

    def test_returns_false_and_logs_warning_when_invalid_addresses_exist(self, spark, mock_logger):
        df = make_df(
            spark,
            [("Main Street, 12, 1234 AB",), ("Not An Address",)],
            "address STRING",
        )

        assert dq_functional.check_address_format(df, "dataset") is False
        mock_logger.warning.assert_called_once()

    def test_raises_when_halt_on_failure_and_invalid_addresses_exist(self, spark, mock_logger):
        df = make_df(spark, [("Not An Address",)], "address STRING")

        with pytest.raises(RuntimeError, match=r"Address format check failed in dataset: 1 invalid address"):
            dq_functional.check_address_format(df, "dataset", halt_on_failure=True)

    def test_does_not_raise_when_halt_on_failure_and_all_valid(self, spark, mock_logger):
        df = make_df(spark, [("Main Street, 12, 1234 AB",)], "address STRING")

        assert dq_functional.check_address_format(df, "dataset", halt_on_failure=True) is True

    def test_raises_runtime_error_when_address_column_does_not_exist(self, spark, mock_logger):
        df = make_df(spark, [(1,)], "id INT")

        with pytest.raises(RuntimeError, match="Error occurred while checking address format in dataset"):
            dq_functional.check_address_format(df, "dataset")

        mock_logger.error.assert_called_once()
