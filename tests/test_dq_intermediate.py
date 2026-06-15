"""Tests for eternalsalesdata.dq_checks.dq_intermediate."""

from typing import Any

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from eternalsalesdata.dq_checks.dq_intermediate import (
    check_address_format,
    check_calls_successful_gt_made,
    check_referential_integrity,
)

DATASET = "test_dataset"

# ---------------------------------------------------------------------------
# check_referential_integrity
# ---------------------------------------------------------------------------


def test_check_referential_integrity_passes(spark: SparkSession) -> None:
    parent = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
    child = spark.createDataFrame([(1,), (2,), (3,)], ["caller_id"])
    assert check_referential_integrity(parent, ["id"], child, ["caller_id"], DATASET) is True


def test_check_referential_integrity_halt_true_raises(spark: SparkSession) -> None:
    parent = spark.createDataFrame([(1,), (2,)], ["id"])
    child = spark.createDataFrame([(1,), (99,)], ["caller_id"])
    with pytest.raises(RuntimeError, match=DATASET):
        check_referential_integrity(
            parent, ["id"], child, ["caller_id"], DATASET, halt_on_failure=True
        )


def test_check_referential_integrity_halt_false_returns_false(spark: SparkSession) -> None:
    parent = spark.createDataFrame([(1,), (2,)], ["id"])
    child = spark.createDataFrame([(1,), (99,)], ["caller_id"])
    assert (
        check_referential_integrity(
            parent, ["id"], child, ["caller_id"], DATASET, halt_on_failure=False
        )
        is False
    )


def test_check_referential_integrity_halt_none_returns_false(spark: SparkSession) -> None:
    parent = spark.createDataFrame([(1,), (2,)], ["id"])
    child = spark.createDataFrame([(1,), (99,)], ["caller_id"])
    assert check_referential_integrity(parent, ["id"], child, ["caller_id"], DATASET, halt_on_failure=None) is False  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "child_data",
    [
        [(1,), (99,)],  # one orphan, one matched
        [(99,), (100,)],  # all rows are orphans
    ],
    ids=["partial-orphans", "all-orphans"],
)
def test_check_referential_integrity_orphans(
    spark: SparkSession, child_data: list[tuple[Any, ...]]
) -> None:
    parent = spark.createDataFrame([(1,), (2,)], ["id"])
    child = spark.createDataFrame(child_data, ["caller_id"])

    # Return value
    assert (
        check_referential_integrity(
            parent, ["id"], child, ["caller_id"], DATASET, halt_on_failure=False
        )
        is False
    )

    # Error on halt
    with pytest.raises(RuntimeError, match=DATASET):
        check_referential_integrity(
            parent, ["id"], child, ["caller_id"], DATASET, halt_on_failure=True
        )


def test_check_referential_integrity_empty_child_passes(spark: SparkSession) -> None:
    parent = spark.createDataFrame([(1,), (2,)], ["id"])
    child = spark.createDataFrame([], StructType([StructField("caller_id", IntegerType(), True)]))
    assert check_referential_integrity(parent, ["id"], child, ["caller_id"], DATASET) is True


def test_check_referential_integrity_missing_parent_key_raises(spark: SparkSession) -> None:
    parent = spark.createDataFrame([(1,)], ["id"])
    child = spark.createDataFrame([(1,)], ["caller_id"])
    with pytest.raises(ValueError, match="nonexistent"):
        check_referential_integrity(parent, ["nonexistent"], child, ["caller_id"], DATASET)


def test_check_referential_integrity_missing_child_key_raises(spark: SparkSession) -> None:
    parent = spark.createDataFrame([(1,)], ["id"])
    child = spark.createDataFrame([(1,)], ["caller_id"])
    with pytest.raises(ValueError, match="nonexistent"):
        check_referential_integrity(parent, ["id"], child, ["nonexistent"], DATASET)


# ---------------------------------------------------------------------------
# check_calls_successful_gt_made
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "data",
    [
        [(5, 10), (7, 7), (0, 1)],  # successful < made
        [(10, 10), (5, 5)],  # equal values are allowed
        [(0, 0), (0, 5)],  # zero successful
    ],
    ids=["normal", "equal-values", "zero-successful"],
)
def test_check_calls_successful_gt_made_passes(
    spark: SparkSession, data: list[tuple[int, int]]
) -> None:
    df = spark.createDataFrame(data, ["calls_successful", "calls_made"])
    assert check_calls_successful_gt_made(df, DATASET) is True


def test_check_calls_successful_gt_made_halt_true_raises(spark: SparkSession) -> None:
    df = spark.createDataFrame([(8, 6)], ["calls_successful", "calls_made"])
    with pytest.raises(RuntimeError, match=DATASET):
        check_calls_successful_gt_made(df, DATASET, halt_on_failure=True)


def test_check_calls_successful_gt_made_halt_false_returns_false(spark: SparkSession) -> None:
    df = spark.createDataFrame([(8, 6)], ["calls_successful", "calls_made"])
    assert check_calls_successful_gt_made(df, DATASET, halt_on_failure=False) is False


def test_check_calls_successful_gt_made_halt_none_returns_false(spark: SparkSession) -> None:
    df = spark.createDataFrame([(8, 6)], ["calls_successful", "calls_made"])
    assert check_calls_successful_gt_made(df, DATASET, halt_on_failure=None) is False  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "data",
    [
        [(5, 10), (8, 6)],  # one violation, one clean row
        [(9, 5), (7, 3), (2, 10)],  # multiple violations
        [(10, 5), (8, 3)],  # every row violates
    ],
    ids=["single-violation", "multiple-violations", "all-violate"],
)
def test_check_calls_successful_gt_made_violations(
    spark: SparkSession, data: list[tuple[int, int]]
) -> None:
    df = spark.createDataFrame(data, ["calls_successful", "calls_made"])

    # Return value
    assert check_calls_successful_gt_made(df, DATASET, halt_on_failure=False) is False

    # Error on halt
    with pytest.raises(RuntimeError, match=DATASET):
        check_calls_successful_gt_made(df, DATASET, halt_on_failure=True)


def test_check_calls_successful_gt_made_empty_dataframe(spark: SparkSession) -> None:
    schema = StructType(
        [
            StructField("calls_successful", IntegerType(), True),
            StructField("calls_made", IntegerType(), True),
        ]
    )
    df = spark.createDataFrame([], schema)
    assert check_calls_successful_gt_made(df, DATASET) is True


def test_check_calls_successful_gt_made_missing_calls_successful_raises(
    spark: SparkSession,
) -> None:
    df = spark.createDataFrame([(10,)], ["calls_made"])
    with pytest.raises(ValueError, match="calls_successful"):
        check_calls_successful_gt_made(df, DATASET)


def test_check_calls_successful_gt_made_missing_calls_made_raises(spark: SparkSession) -> None:
    df = spark.createDataFrame([(5,)], ["calls_successful"])
    with pytest.raises(ValueError, match="calls_made"):
        check_calls_successful_gt_made(df, DATASET)


# ---------------------------------------------------------------------------
# check_address_format
# Pattern: ^[A-Za-z0-9-\s]+, \d+, \d{4} [A-Z]{2}$
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "address",
    [
        "Main Street, 10, 1234 AB",  # simple
        "Oak-Avenue, 5, 4321 XY",  # hyphenated street name
        "1st Avenue, 7, 9876 ZZ",  # alphanumeric street name
    ],
    ids=["simple", "hyphenated-street", "alphanumeric-street"],
)
def test_check_address_format_passes(spark: SparkSession, address: str) -> None:
    df = spark.createDataFrame([(address,)], ["address"])
    assert check_address_format(df, DATASET) is True


def test_check_address_format_halt_true_raises(spark: SparkSession) -> None:
    df = spark.createDataFrame([("not a valid address",)], ["address"])
    with pytest.raises(RuntimeError, match=DATASET):
        check_address_format(df, DATASET, halt_on_failure=True)


def test_check_address_format_halt_false_returns_false(spark: SparkSession) -> None:
    df = spark.createDataFrame([("not a valid address",)], ["address"])
    assert check_address_format(df, DATASET, halt_on_failure=False) is False


def test_check_address_format_halt_none_returns_false(spark: SparkSession) -> None:
    df = spark.createDataFrame([("not a valid address",)], ["address"])
    assert check_address_format(df, DATASET, halt_on_failure=None) is False  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "address",
    [
        "Main Street, 12-14, 1234 AB",  # range house number — not a non-negative integer
        "Main Street, 10",  # missing zip code
        "Main Street, 10, 12345 AB",  # five-digit zip (must be four)
        "Main Street, 10, 1234 ab",  # lowercase zip suffix (must be uppercase)
        "Main Street, 10, 1234 A",  # single letter zip suffix (must be two)
        "Main Street, 10, 1234 ABC",  # three letter zip suffix (must be two)
        "",  # empty string
        None,  # null address
    ],
    ids=[
        "range-number",
        "missing-zip",
        "five-digit-zip",
        "lowercase-suffix",
        "single-letter-suffix",
        "three-letter-suffix",
        "empty-string",
        "null",
    ],
)
def test_check_address_format_invalid_patterns(spark: SparkSession, address: str | None) -> None:
    schema = StructType([StructField("address", StringType(), True)])
    df = spark.createDataFrame([(address,)], schema)
    assert check_address_format(df, DATASET) is False


def test_check_address_format_mixed_valid_and_invalid(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [("Main Street, 10, 1234 AB",), ("not a valid address",)],
        ["address"],
    )

    # Return value
    assert check_address_format(df, DATASET, halt_on_failure=False) is False

    # Error on halt
    with pytest.raises(RuntimeError, match=DATASET):
        check_address_format(df, DATASET, halt_on_failure=True)


def test_check_address_format_empty_dataframe(spark: SparkSession) -> None:
    schema = StructType([StructField("address", StringType(), True)])
    df = spark.createDataFrame([], schema)
    assert check_address_format(df, DATASET) is True


def test_check_address_format_missing_column_raises(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1,)], ["id"])
    with pytest.raises(ValueError, match="address"):
        check_address_format(df, DATASET)
