"""Tests for sales_data.dq_checks.dq_functional."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from sales_data.dq_checks.dq_intermediate import (
    check_address_format,
    check_calls_successful_gt_made,
)

# ---------------------------------------------------------------------------
# check_calls_successful_gt_made
# ---------------------------------------------------------------------------


def test_check_calls_successful_gt_made_passes(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [(5, 10), (7, 7), (0, 1)],
        ["calls_successful", "calls_made"],
    )
    assert check_calls_successful_gt_made(df, "test_dataset") is True


def test_check_calls_successful_gt_made_equal_values_pass(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [(10, 10), (5, 5)],
        ["calls_successful", "calls_made"],
    )
    assert check_calls_successful_gt_made(df, "test_dataset") is True


def test_check_calls_successful_gt_made_zero_successful_passes(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [(0, 0), (0, 5)],
        ["calls_successful", "calls_made"],
    )
    assert check_calls_successful_gt_made(df, "test_dataset") is True


def test_check_calls_successful_gt_made_single_violation_halt_false(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [(5, 10), (8, 6)],
        ["calls_successful", "calls_made"],
    )
    assert check_calls_successful_gt_made(df, "test_dataset", halt_on_failure=False) is False


def test_check_calls_successful_gt_made_single_violation_halt_true(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [(5, 10), (8, 6)],
        ["calls_successful", "calls_made"],
    )
    with pytest.raises(RuntimeError, match="test_dataset"):
        check_calls_successful_gt_made(df, "test_dataset", halt_on_failure=True)


def test_check_calls_successful_gt_made_multiple_violations_halt_false(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [(9, 5), (7, 3), (2, 10)],
        ["calls_successful", "calls_made"],
    )
    assert check_calls_successful_gt_made(df, "test_dataset", halt_on_failure=False) is False


def test_check_calls_successful_gt_made_multiple_violations_halt_true(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [(9, 5), (7, 3), (2, 10)],
        ["calls_successful", "calls_made"],
    )
    with pytest.raises(RuntimeError, match="test_dataset"):
        check_calls_successful_gt_made(df, "test_dataset", halt_on_failure=True)


def test_check_calls_successful_gt_made_all_rows_violate(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [(10, 5), (8, 3)],
        ["calls_successful", "calls_made"],
    )
    assert check_calls_successful_gt_made(df, "test_dataset", halt_on_failure=False) is False


def test_check_calls_successful_gt_made_empty_dataframe(spark: SparkSession) -> None:
    schema = StructType(
        [
            StructField("calls_successful", IntegerType(), True),
            StructField("calls_made", IntegerType(), True),
        ]
    )
    df = spark.createDataFrame([], schema)
    assert check_calls_successful_gt_made(df, "test_dataset") is True


def test_check_calls_successful_gt_made_halt_none_treated_as_false(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [(8, 6)],
        ["calls_successful", "calls_made"],
    )
    assert check_calls_successful_gt_made(df, "test_dataset", halt_on_failure=None) is False  # type: ignore[arg-type]


def test_check_calls_successful_gt_made_missing_column_always_raises(spark: SparkSession) -> None:
    df = spark.createDataFrame([(10,)], ["calls_made"])
    with pytest.raises(RuntimeError):
        check_calls_successful_gt_made(df, "test_dataset", halt_on_failure=False)


def test_check_calls_successful_gt_made_missing_calls_made_column_always_raises(
    spark: SparkSession,
) -> None:
    df = spark.createDataFrame([(5,)], ["calls_successful"])
    with pytest.raises(RuntimeError):
        check_calls_successful_gt_made(df, "test_dataset", halt_on_failure=False)


# ---------------------------------------------------------------------------
# check_address_format
# Pattern: ^[A-Za-z0-9-\s]+, \d+(?:-\d+)?, \d{4} [A-Z]{2}$
# ---------------------------------------------------------------------------


def test_check_address_format_passes_simple(spark: SparkSession) -> None:
    df = spark.createDataFrame([("Main Street, 10, 1234 AB",)], ["address"])
    assert check_address_format(df, "test_dataset") is True


def test_check_address_format_passes_hyphenated_street(spark: SparkSession) -> None:
    df = spark.createDataFrame([("Oak-Avenue, 5, 4321 XY",)], ["address"])
    assert check_address_format(df, "test_dataset") is True


def test_check_address_format_passes_range_number(spark: SparkSession) -> None:
    df = spark.createDataFrame([("Main Street, 12-14, 1234 AB",)], ["address"])
    assert check_address_format(df, "test_dataset") is True


def test_check_address_format_passes_alphanumeric_street(spark: SparkSession) -> None:
    df = spark.createDataFrame([("1st Avenue, 7, 9876 ZZ",)], ["address"])
    assert check_address_format(df, "test_dataset") is True


def test_check_address_format_single_invalid_halt_false(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [("Main Street, 10, 1234 AB",), ("not a valid address",)],
        ["address"],
    )
    assert check_address_format(df, "test_dataset", halt_on_failure=False) is False


def test_check_address_format_single_invalid_halt_true(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [("Main Street, 10, 1234 AB",), ("not a valid address",)],
        ["address"],
    )
    with pytest.raises(RuntimeError, match="test_dataset"):
        check_address_format(df, "test_dataset", halt_on_failure=True)


def test_check_address_format_multiple_invalid_halt_false(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [("Main Street, 10, 1234 AB",), ("bad address one",), ("bad address two",)],
        ["address"],
    )
    assert check_address_format(df, "test_dataset", halt_on_failure=False) is False


def test_check_address_format_multiple_invalid_halt_true(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [("Main Street, 10, 1234 AB",), ("bad address one",), ("bad address two",)],
        ["address"],
    )
    with pytest.raises(RuntimeError, match="test_dataset"):
        check_address_format(df, "test_dataset", halt_on_failure=True)


def test_check_address_format_all_invalid(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [("bad address one",), ("bad address two",)],
        ["address"],
    )
    assert check_address_format(df, "test_dataset", halt_on_failure=False) is False


def test_check_address_format_missing_zip_code(spark: SparkSession) -> None:
    df = spark.createDataFrame([("Main Street, 10",)], ["address"])
    assert check_address_format(df, "test_dataset") is False


def test_check_address_format_five_digit_zip(spark: SparkSession) -> None:
    df = spark.createDataFrame([("Main Street, 10, 12345 AB",)], ["address"])
    assert check_address_format(df, "test_dataset") is False


def test_check_address_format_lowercase_zip_suffix(spark: SparkSession) -> None:
    df = spark.createDataFrame([("Main Street, 10, 1234 ab",)], ["address"])
    assert check_address_format(df, "test_dataset") is False


def test_check_address_format_single_letter_zip_suffix(spark: SparkSession) -> None:
    df = spark.createDataFrame([("Main Street, 10, 1234 A",)], ["address"])
    assert check_address_format(df, "test_dataset") is False


def test_check_address_format_three_letter_zip_suffix(spark: SparkSession) -> None:
    df = spark.createDataFrame([("Main Street, 10, 1234 ABC",)], ["address"])
    assert check_address_format(df, "test_dataset") is False


def test_check_address_format_empty_string(spark: SparkSession) -> None:
    df = spark.createDataFrame([("",)], ["address"])
    assert check_address_format(df, "test_dataset") is False


def test_check_address_format_null_address_not_flagged(spark: SparkSession) -> None:
    # rlike(pattern) on null yields null; filter(~null) excludes the row entirely,
    # so null addresses are silently skipped and not counted as invalid.
    schema = StructType([StructField("address", StringType(), True)])
    df = spark.createDataFrame([(None,)], schema)
    assert check_address_format(df, "test_dataset") is True


def test_check_address_format_empty_dataframe(spark: SparkSession) -> None:
    schema = StructType([StructField("address", StringType(), True)])
    df = spark.createDataFrame([], schema)
    assert check_address_format(df, "test_dataset") is True


def test_check_address_format_halt_none_treated_as_false(spark: SparkSession) -> None:
    df = spark.createDataFrame([("not a valid address",)], ["address"])
    assert check_address_format(df, "test_dataset", halt_on_failure=None) is False  # type: ignore[arg-type]


def test_check_address_format_missing_column_always_raises(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1,)], ["id"])
    with pytest.raises(RuntimeError):
        check_address_format(df, "test_dataset", halt_on_failure=False)
