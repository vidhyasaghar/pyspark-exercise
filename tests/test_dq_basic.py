"""Tests for eternalsalesdata.dq_checks.dq_basic."""

import logging
from typing import Any

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from eternalsalesdata.dq_checks.dq_basic import (
    check_col_non_negative,
    check_col_non_null,
    check_col_unique,
    check_row_count,
)

DATASET = "test_dataset"
DQ_LOGGER = "eternalsalesdata.dq_checks.dq_basic"


def _str_schema(columns: list[str]) -> StructType:
    return StructType([StructField(c, StringType(), True) for c in columns])


def _int_schema(columns: list[str]) -> StructType:
    return StructType([StructField(c, IntegerType(), True) for c in columns])


# ---------------------------------------------------------------------------
# check_row_count
# ---------------------------------------------------------------------------


def test_check_row_count_passes(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
    assert check_row_count(df, 3, DATASET) is True


def test_check_row_count_halt_true_raises(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1,), (2,)], ["id"])
    with pytest.raises(RuntimeError, match=DATASET):
        check_row_count(df, 5, DATASET, halt_on_failure=True)


def test_check_row_count_halt_false_returns_false(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1,), (2,)], ["id"])
    assert check_row_count(df, 5, DATASET, halt_on_failure=False) is False


def test_check_row_count_halt_none_returns_false(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1,), (2,)], ["id"])
    assert check_row_count(df, 5, DATASET, halt_on_failure=None) is False  # type: ignore[arg-type]


@pytest.mark.parametrize("expected,passes", [(0, True), (3, False)])
def test_check_row_count_empty_dataframe(spark: SparkSession, expected: int, passes: bool) -> None:
    df = spark.createDataFrame([], _int_schema(["id"]))
    assert check_row_count(df, expected, DATASET) is passes


# ---------------------------------------------------------------------------
# check_col_non_null
# ---------------------------------------------------------------------------


def test_check_col_non_null_passes(spark: SparkSession) -> None:
    df = spark.createDataFrame([("alice", "a@b.com")], _str_schema(["name", "email"]))
    assert check_col_non_null(df, DATASET, ["name", "email"]) is True


def test_check_col_non_null_halt_true_raises(spark: SparkSession) -> None:
    df = spark.createDataFrame([(None,)], _str_schema(["name"]))
    with pytest.raises(RuntimeError, match="name"):
        check_col_non_null(df, DATASET, ["name"], halt_on_failure=True)


def test_check_col_non_null_halt_false_returns_false(spark: SparkSession) -> None:
    df = spark.createDataFrame([(None,)], _str_schema(["name"]))
    assert check_col_non_null(df, DATASET, ["name"], halt_on_failure=False) is False


def test_check_col_non_null_halt_none_returns_false(spark: SparkSession) -> None:
    df = spark.createDataFrame([(None,)], _str_schema(["name"]))
    assert check_col_non_null(df, DATASET, ["name"], halt_on_failure=None) is False  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "data, fail_cols, pass_cols",
    [
        # Both columns have nulls
        ([(None, "a@b.com"), ("alice", None)], ["name", "email"], []),
        # Only "name" has a null — per-column log lines prove "email" was checked and passed
        ([(None, "a@b.com"), ("alice", "b@c.com")], ["name"], ["email"]),
    ],
    ids=["all-fail", "partial-fail"],
)
def test_check_col_non_null_column_failures(
    spark: SparkSession,
    caplog: pytest.LogCaptureFixture,
    data: list[tuple[Any, ...]],
    fail_cols: list[str],
    pass_cols: list[str],
) -> None:
    df = spark.createDataFrame(data, _str_schema(["name", "email"]))
    columns = ["name", "email"]

    # Return value
    with caplog.at_level(logging.WARNING, logger=DQ_LOGGER):
        assert check_col_non_null(df, DATASET, columns, halt_on_failure=False) is False

    # Log content: each failing column gets its own warning line
    log_text = " ".join(r.message for r in caplog.records)
    for col in fail_cols:
        assert col in log_text, f"expected '{col}' in warning logs"
    for col in pass_cols:
        assert col not in log_text, f"'{col}' should not appear in warning logs"

    # Error message on halt: only failing columns named
    with pytest.raises(RuntimeError) as exc_info:
        check_col_non_null(df, DATASET, columns, halt_on_failure=True)
    error_msg = str(exc_info.value)
    for col in fail_cols:
        assert col in error_msg
    for col in pass_cols:
        assert col not in error_msg


def test_check_col_non_null_nonexistent_column_raises(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1,)], ["id"])
    with pytest.raises(ValueError, match="nonexistent_col"):
        check_col_non_null(df, DATASET, ["nonexistent_col"])


def test_check_col_non_null_empty_column_list(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1,)], ["id"])
    assert check_col_non_null(df, DATASET, []) is True


# ---------------------------------------------------------------------------
# check_col_unique
# ---------------------------------------------------------------------------


def test_check_col_unique_passes(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "code"])
    assert check_col_unique(df, DATASET, ["id", "code"]) is True


def test_check_col_unique_halt_true_raises(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1,), (1,)], ["id"])
    with pytest.raises(RuntimeError, match="id"):
        check_col_unique(df, DATASET, ["id"], halt_on_failure=True)


def test_check_col_unique_halt_false_returns_false(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1,), (1,)], ["id"])
    assert check_col_unique(df, DATASET, ["id"], halt_on_failure=False) is False


def test_check_col_unique_halt_none_returns_false(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1,), (1,)], ["id"])
    assert check_col_unique(df, DATASET, ["id"], halt_on_failure=None) is False  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "data, fail_cols, pass_cols",
    [
        # Both columns have duplicates
        ([(1, "x"), (1, "x"), (2, "y")], ["id", "code"], []),
        # Only "id" has duplicates — per-column log lines prove "code" was checked and passed
        ([(1, "a"), (1, "b"), (2, "c")], ["id"], ["code"]),
    ],
    ids=["all-fail", "partial-fail"],
)
def test_check_col_unique_column_failures(
    spark: SparkSession,
    caplog: pytest.LogCaptureFixture,
    data: list[tuple[Any, ...]],
    fail_cols: list[str],
    pass_cols: list[str],
) -> None:
    df = spark.createDataFrame(data, ["id", "code"])
    columns = ["id", "code"]

    # Return value
    with caplog.at_level(logging.WARNING, logger=DQ_LOGGER):
        assert check_col_unique(df, DATASET, columns, halt_on_failure=False) is False

    # Log content: each failing column gets its own warning line
    log_text = " ".join(r.message for r in caplog.records)
    for col in fail_cols:
        assert col in log_text, f"expected '{col}' in warning logs"
    for col in pass_cols:
        assert col not in log_text, f"'{col}' should not appear in warning logs"

    # Error message on halt: only failing columns named
    with pytest.raises(RuntimeError) as exc_info:
        check_col_unique(df, DATASET, columns, halt_on_failure=True)
    error_msg = str(exc_info.value)
    for col in fail_cols:
        assert col in error_msg
    for col in pass_cols:
        assert col not in error_msg


def test_check_col_unique_nonexistent_column_raises(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1,)], ["id"])
    with pytest.raises(ValueError, match="nonexistent_col"):
        check_col_unique(df, DATASET, ["nonexistent_col"])


def test_check_col_unique_empty_column_list(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1,)], ["id"])
    assert check_col_unique(df, DATASET, []) is True


def test_check_col_unique_empty_dataframe(spark: SparkSession) -> None:
    df = spark.createDataFrame([], _int_schema(["id"]))
    assert check_col_unique(df, DATASET, ["id"]) is True


# ---------------------------------------------------------------------------
# check_col_non_negative
# ---------------------------------------------------------------------------


def test_check_col_non_negative_passes(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1, 10), (2, 20)], ["qty", "price"])
    assert check_col_non_negative(df, ["qty", "price"], DATASET) is True


def test_check_col_non_negative_zero_is_allowed(spark: SparkSession) -> None:
    df = spark.createDataFrame([(0,), (1,), (5,)], ["qty"])
    assert check_col_non_negative(df, ["qty"], DATASET) is True


def test_check_col_non_negative_halt_true_raises(spark: SparkSession) -> None:
    df = spark.createDataFrame([(-1,)], ["qty"])
    with pytest.raises(RuntimeError, match="qty"):
        check_col_non_negative(df, ["qty"], DATASET, halt_on_failure=True)


def test_check_col_non_negative_halt_false_returns_false(spark: SparkSession) -> None:
    df = spark.createDataFrame([(-1,)], ["qty"])
    assert check_col_non_negative(df, ["qty"], DATASET, halt_on_failure=False) is False


def test_check_col_non_negative_halt_none_returns_false(spark: SparkSession) -> None:
    df = spark.createDataFrame([(-1,)], ["qty"])
    assert check_col_non_negative(df, ["qty"], DATASET, halt_on_failure=None) is False  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "data, fail_cols, pass_cols",
    [
        # Both columns have negative values
        ([(-1, -5), (0, 0), (5, 10)], ["qty", "price"], []),
        # Only "qty" has negatives — per-column log lines prove "price" was checked and passed
        ([(-1, 5), (0, 10), (3, 15)], ["qty"], ["price"]),
    ],
    ids=["all-fail", "partial-fail"],
)
def test_check_col_non_negative_column_failures(
    spark: SparkSession,
    caplog: pytest.LogCaptureFixture,
    data: list[tuple[Any, ...]],
    fail_cols: list[str],
    pass_cols: list[str],
) -> None:
    df = spark.createDataFrame(data, ["qty", "price"])
    columns = ["qty", "price"]

    # Return value
    with caplog.at_level(logging.WARNING, logger=DQ_LOGGER):
        assert check_col_non_negative(df, columns, DATASET, halt_on_failure=False) is False

    # Log content: each failing column gets its own warning line
    log_text = " ".join(r.message for r in caplog.records)
    for col in fail_cols:
        assert col in log_text, f"expected '{col}' in warning logs"
    for col in pass_cols:
        assert col not in log_text, f"'{col}' should not appear in warning logs"

    # Error message on halt: only failing columns named
    with pytest.raises(RuntimeError) as exc_info:
        check_col_non_negative(df, columns, DATASET, halt_on_failure=True)
    error_msg = str(exc_info.value)
    for col in fail_cols:
        assert col in error_msg
    for col in pass_cols:
        assert col not in error_msg


def test_check_col_non_negative_nonexistent_column_raises(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1,)], ["qty"])
    with pytest.raises(ValueError, match="nonexistent_col"):
        check_col_non_negative(df, ["nonexistent_col"], DATASET)


def test_check_col_non_negative_empty_column_list(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1,)], ["qty"])
    assert check_col_non_negative(df, [], DATASET) is True
