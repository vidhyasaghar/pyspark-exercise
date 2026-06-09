"""
Tests for :mod:`sales_data.dq_checks.dq_common`.

Uses real local DataFrames (via the shared ``spark`` fixture) and
:mod:`chispa` for DataFrame-equality assertions, plus :mod:`unittest.mock`
to assert on the module-level logger.
"""

from unittest.mock import MagicMock, patch

import pytest
from chispa import assert_df_equality

import sales_data.dq_checks.dq_common as dq_common


def make_df(spark, data, schema):
    """Build a small DataFrame from row tuples using an explicit DDL schema string."""
    return spark.createDataFrame(data, schema)


@pytest.fixture
def mock_logger():
    """Replace the module-level logger with a MagicMock so calls can be asserted on."""
    mock = MagicMock()
    with patch.object(dq_common, "logger", mock):
        yield mock


class TestCheckRowCount:
    """Tests for :func:`check_row_count`."""

    def test_returns_true_and_logs_info_when_count_matches(self, spark, mock_logger):
        df = make_df(spark, [(1,), (2,), (3,)], "id INT")

        assert dq_common.check_row_count(df, 3, "dataset") is True
        mock_logger.info.assert_called_once()
        mock_logger.warning.assert_not_called()

    def test_returns_false_and_logs_warning_when_mismatch(self, spark, mock_logger):
        df = make_df(spark, [(1,), (2,)], "id INT")

        assert dq_common.check_row_count(df, 3, "dataset") is False
        mock_logger.warning.assert_called_once()

    def test_raises_when_halt_on_failure_and_mismatch(self, spark, mock_logger):
        df = make_df(spark, [(1,), (2,)], "id INT")

        with pytest.raises(RuntimeError, match="Row count mismatch in dataset"):
            dq_common.check_row_count(df, 3, "dataset", halt_on_failure=True)

    def test_does_not_raise_when_halt_on_failure_and_count_matches(self, spark, mock_logger):
        df = make_df(spark, [(1,), (2,)], "id INT")

        assert dq_common.check_row_count(df, 2, "dataset", halt_on_failure=True) is True


class TestCheckColNonNull:
    """Tests for :func:`check_col_non_null`."""

    def test_returns_true_when_no_nulls(self, spark, mock_logger):
        df = make_df(spark, [(1, "a"), (2, "b")], "id INT, name STRING")

        assert dq_common.check_col_non_null(df, "dataset", ["id", "name"]) is True
        mock_logger.warning.assert_not_called()
        mock_logger.error.assert_not_called()

    def test_returns_false_and_logs_warning_when_nulls_present(self, spark, mock_logger):
        df = make_df(spark, [(1, "a"), (2, None)], "id INT, name STRING")

        assert dq_common.check_col_non_null(df, "dataset", ["name"]) is False
        mock_logger.warning.assert_called_once()

    def test_raises_when_halt_on_failure_and_nulls_present(self, spark, mock_logger):
        df = make_df(spark, [(1, "a"), (2, None)], "id INT, name STRING")

        with pytest.raises(RuntimeError, match="Null values found in name"):
            dq_common.check_col_non_null(df, "dataset", ["name"], halt_on_failure=True)

    def test_checks_every_column_even_after_an_earlier_failure(self, spark, mock_logger):
        df = make_df(spark, [(None, None)], "id INT, name STRING")

        assert dq_common.check_col_non_null(df, "dataset", ["id", "name"]) is False
        assert mock_logger.warning.call_count == 2

    def test_returns_false_and_logs_error_when_column_does_not_exist(self, spark, mock_logger):
        df = make_df(spark, [(1,)], "id INT")

        assert dq_common.check_col_non_null(df, "dataset", ["missing"]) is False
        mock_logger.error.assert_called_once()
        mock_logger.warning.assert_not_called()


class TestCheckColUnique:
    """Tests for :func:`check_col_unique`."""

    def test_returns_true_when_column_unique(self, spark, mock_logger):
        df = make_df(spark, [(1,), (2,), (3,)], "id INT")

        assert dq_common.check_col_unique(df, "dataset", ["id"]) is True
        mock_logger.warning.assert_not_called()

    def test_returns_false_and_logs_warning_when_duplicates_present(self, spark, mock_logger):
        df = make_df(spark, [(1,), (1,), (2,)], "id INT")

        assert dq_common.check_col_unique(df, "dataset", ["id"]) is False
        mock_logger.warning.assert_called_once()

    def test_raises_when_halt_on_failure_and_duplicates_present(self, spark, mock_logger):
        df = make_df(spark, [(1,), (1,)], "id INT")

        with pytest.raises(RuntimeError, match="Duplicates found in id"):
            dq_common.check_col_unique(df, "dataset", ["id"], halt_on_failure=True)

    def test_total_row_count_is_computed_once_for_multiple_columns(self, spark, mock_logger):
        """Regression test: df.count() must run once, not once per checked column."""
        df = make_df(spark, [(1, "a"), (2, "b"), (3, "c")], "id INT, name STRING")

        with patch.object(df, "count", wraps=df.count) as count_spy:
            dq_common.check_col_unique(df, "dataset", ["id", "name"])

        assert count_spy.call_count == 1

    def test_distinct_values_match_expected_set(self, spark):
        """chispa-based check that distinct() yields exactly the unique values."""
        df = make_df(spark, [(1,), (1,), (2,), (3,), (3,)], "id INT")
        expected = make_df(spark, [(1,), (2,), (3,)], "id INT")

        assert_df_equality(df.select("id").distinct(), expected, ignore_row_order=True)


class TestCheckColNonNegative:
    """Tests for :func:`check_col_non_negative`."""

    def test_returns_true_when_all_non_negative(self, spark, mock_logger):
        df = make_df(spark, [(1, 2), (3, 4)], "a INT, b INT")

        assert dq_common.check_col_non_negative(df, ["a", "b"], "dataset") is True
        mock_logger.warning.assert_not_called()

    def test_returns_false_and_logs_warning_when_negative_values_present(self, spark, mock_logger):
        df = make_df(spark, [(1, -2), (3, 4)], "a INT, b INT")

        assert dq_common.check_col_non_negative(df, ["b"], "dataset") is False
        mock_logger.warning.assert_called_once()

    def test_raises_when_halt_on_failure_and_negative_values_present(self, spark, mock_logger):
        df = make_df(spark, [(1, -2)], "a INT, b INT")

        with pytest.raises(RuntimeError, match="Negative values found in b"):
            dq_common.check_col_non_negative(df, ["b"], "dataset", halt_on_failure=True)


class TestCheckReferentialIntegrity:
    """Tests for :func:`check_referential_integrity`."""

    def test_returns_true_when_all_child_rows_match_parent(self, spark, mock_logger):
        parent = make_df(spark, [(1,), (2,), (3,)], "id INT")
        child = make_df(spark, [(1,), (2,)], "id INT")

        assert dq_common.check_referential_integrity(parent, ["id"], child, ["id"], "dataset") is True
        mock_logger.warning.assert_not_called()

    def test_returns_false_and_logs_warning_when_orphans_exist(self, spark, mock_logger):
        parent = make_df(spark, [(1,), (2,)], "id INT")
        child = make_df(spark, [(1,), (99,)], "id INT")

        assert dq_common.check_referential_integrity(parent, ["id"], child, ["id"], "dataset") is False
        mock_logger.warning.assert_called_once()

    def test_raises_with_dataset_name_and_orphan_count_when_halt_on_failure(self, spark, mock_logger):
        """Regression test for the previously-broken RuntimeError message formatting."""
        parent = make_df(spark, [(1,)], "id INT")
        child = make_df(spark, [(1,), (2,), (3,)], "id INT")

        with pytest.raises(RuntimeError, match=r"Referential integrity failed in dataset: 2 orphan row"):
            dq_common.check_referential_integrity(parent, ["id"], child, ["id"], "dataset", halt_on_failure=True)

    def test_raises_runtime_error_when_join_keys_do_not_exist(self, spark, mock_logger):
        parent = make_df(spark, [(1,)], "id INT")
        child = make_df(spark, [(1,)], "other_id INT")

        with pytest.raises(RuntimeError, match="Error occurred while checking referential integrity in dataset"):
            dq_common.check_referential_integrity(parent, ["id"], child, ["id"], "dataset")

        mock_logger.error.assert_called_once()

    def test_orphan_rows_match_expected_set(self, spark):
        """chispa-based check that the subtract() diff yields exactly the orphan rows."""
        parent = make_df(spark, [(1,), (2,)], "id INT")
        child = make_df(spark, [(1,), (3,), (4,)], "id INT")

        expected_orphans = make_df(spark, [(3,), (4,)], "id INT")
        actual_orphans = child.select("id").subtract(parent.select("id"))

        assert_df_equality(actual_orphans, expected_orphans, ignore_row_order=True)
