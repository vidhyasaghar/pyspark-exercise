"""Tests for sales_data.dq_checks.dq_common."""

import pytest
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from eternalsalesData.dq_checks.dq_basic import (
    check_col_non_negative,
    check_col_non_null,
    check_col_unique,
    check_row_count,
)

# ---------------------------------------------------------------------------
# check_row_count
# ---------------------------------------------------------------------------


def test_check_row_count_passes(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
    assert check_row_count(df, 3, "test_dataset") is True


def test_check_row_count_mismatch_halt_false(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1,), (2,)], ["id"])
    assert check_row_count(df, 5, "test_dataset", halt_on_failure=False) is False


def test_check_row_count_mismatch_halt_true(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1,), (2,)], ["id"])
    with pytest.raises(RuntimeError, match="test_dataset"):
        check_row_count(df, 5, "test_dataset", halt_on_failure=True)


def test_check_row_count_halt_none_treated_as_false(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1,), (2,)], ["id"])
    assert check_row_count(df, 5, "test_dataset", halt_on_failure=None) is False  # type: ignore[arg-type]


def test_check_row_count_empty_dataframe_matches_zero(spark: SparkSession) -> None:
    schema = StructType([StructField("id", IntegerType(), True)])
    df = spark.createDataFrame([], schema)
    assert check_row_count(df, 0, "test_dataset") is True


def test_check_row_count_empty_dataframe_mismatch(spark: SparkSession) -> None:
    schema = StructType([StructField("id", IntegerType(), True)])
    df = spark.createDataFrame([], schema)
    assert check_row_count(df, 3, "test_dataset") is False


# ---------------------------------------------------------------------------
# check_col_non_null
# ---------------------------------------------------------------------------


def test_check_col_non_null_passes(spark: SparkSession) -> None:
    df = spark.createDataFrame([("alice", 1), ("bob", 2)], ["name", "age"])
    assert check_col_non_null(df, "test_dataset", ["name", "age"]) is True


@pytest.mark.parametrize(
    "columns,data",
    [
        (["name"], [("alice",), (None,)]),
        (["name", "email"], [(None, "a@b.com"), ("alice", None)]),
    ],
)
def test_check_col_non_null_nulls_halt_false_logs(
    spark: SparkSession, caplog, columns, data
) -> None:
    schema_fields = [StructField("name", StringType(), True)]
    if "email" in columns:
        schema_fields.append(StructField("email", StringType(), True))

    df = spark.createDataFrame(data, StructType(schema_fields))

    with caplog.at_level(logging.WARNING):
        result = check_col_non_null(
            df,
            "test_dataset",
            columns,
            halt_on_failure=False,
        )

    assert result is False

    messages = [record.message for record in caplog.records]

    for col in columns:
        assert any(col in msg for msg in messages), f"{col} not found in logs"

    assert any("test_dataset" in msg for msg in messages)


@pytest.mark.parametrize(
    "columns,data",
    [
        (["name"], [("alice",), (None,)]),
        (["name", "email"], [(None, "a@b.com"), ("alice", None)]),
    ],
)
def test_check_col_non_null_nulls_halt_true(spark: SparkSession, columns, data) -> None:
    schema_fields = [StructField("name", StringType(), True)]
    if "email" in columns:
        schema_fields.append(StructField("email", StringType(), True))

    df = spark.createDataFrame(data, StructType(schema_fields))

    with pytest.raises(RuntimeError) as exc:
        check_col_non_null(
            df,
            "test_dataset",
            columns,
            halt_on_failure=True,
        )

    message = str(exc.value)

    for col in columns:
        assert col in message


@pytest.mark.parametrize("halt", [False, True])
def test_check_col_non_null_nonexistent_column(spark: SparkSession, halt: bool) -> None:
    df = spark.createDataFrame([(1,)], ["id"])

    if halt:
        with pytest.raises(RuntimeError, match="nonexistent_col"):
            check_col_non_null(
                df,
                "test_dataset",
                ["nonexistent_col"],
                halt_on_failure=True,
            )
    else:
        result = check_col_non_null(
            df,
            "test_dataset",
            ["nonexistent_col"],
            halt_on_failure=False,
        )
        assert result is False


def test_check_col_non_null_mixed_columns_partial_failure(
    spark: SparkSession,
) -> None:
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
        ]
    )

    df = spark.createDataFrame([(None, "a@b.com"), ("alice", "b@c.com")], schema)

    # halt=False → False
    assert check_col_non_null(df, "test_dataset", ["name", "email"], halt_on_failure=False) is False

    # halt=True → only failing column should appear
    with pytest.raises(RuntimeError) as exc:
        check_col_non_null(df, "test_dataset", ["name", "email"], halt_on_failure=True)

    message = str(exc.value)

    assert "name" in message
    assert "email" not in message


def test_check_col_non_null_empty_column_list(
    spark: SparkSession,
) -> None:
    df = spark.createDataFrame([(1,)], ["id"])
    assert check_col_non_null(df, "test_dataset", []) is True


def test_check_col_non_null_halt_none_treated_as_false(
    spark: SparkSession,
) -> None:
    schema = StructType([StructField("name", StringType(), True)])
    df = spark.createDataFrame([(None,)], schema)

    result = check_col_non_null(
        df,
        "test_dataset",
        ["name"],
        halt_on_failure=None,  # type: ignore[arg-type]
    )

    assert result is False


# ---------------------------------------------------------------------------
# check_col_unique
# ---------------------------------------------------------------------------


def test_check_col_unique_passes(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "code"])
    assert check_col_unique(df, "test_dataset", ["id", "code"]) is True


def test_check_col_unique_single_column_duplicates_halt_false(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1,), (1,), (2,)], ["id"])
    assert check_col_unique(df, "test_dataset", ["id"], halt_on_failure=False) is False


def test_check_col_unique_single_column_duplicates_halt_true(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1,), (1,), (2,)], ["id"])
    with pytest.raises(RuntimeError, match="id"):
        check_col_unique(df, "test_dataset", ["id"], halt_on_failure=True)


def test_check_col_unique_multiple_columns_duplicates_halt_false(spark: SparkSession) -> None:
    # both id and code have duplicates
    df = spark.createDataFrame([(1, "x"), (1, "x"), (2, "y")], ["id", "code"])
    assert check_col_unique(df, "test_dataset", ["id", "code"], halt_on_failure=False) is False


def test_check_col_unique_multiple_columns_duplicates_halt_true(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1, "x"), (1, "x"), (2, "y")], ["id", "code"])
    with pytest.raises(RuntimeError) as exc_info:
        check_col_unique(df, "test_dataset", ["id", "code"], halt_on_failure=True)
    message = str(exc_info.value)
    assert "id" in message
    assert "code" in message


def test_check_col_unique_mixed_columns_partial_failure(spark: SparkSession) -> None:
    # id has duplicates (1 appears twice); code is unique across all 3 rows
    df = spark.createDataFrame([(1, "a"), (1, "b"), (2, "c")], ["id", "code"])
    assert check_col_unique(df, "test_dataset", ["id", "code"], halt_on_failure=False) is False
    with pytest.raises(RuntimeError) as exc_info:
        check_col_unique(df, "test_dataset", ["id", "code"], halt_on_failure=True)
    message = str(exc_info.value)
    assert "id" in message
    assert "code" not in message


def test_check_col_unique_nonexistent_column_halt_false(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1,)], ["id"])
    assert check_col_unique(df, "test_dataset", ["nonexistent_col"], halt_on_failure=False) is False


def test_check_col_unique_nonexistent_column_halt_true(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1,)], ["id"])
    with pytest.raises(RuntimeError, match="nonexistent_col"):
        check_col_unique(df, "test_dataset", ["nonexistent_col"], halt_on_failure=True)


def test_check_col_unique_empty_column_list(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1,)], ["id"])
    assert check_col_unique(df, "test_dataset", []) is True


def test_check_col_unique_halt_none_treated_as_false(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1,), (1,), (2,)], ["id"])
    assert check_col_unique(df, "test_dataset", ["id"], halt_on_failure=None) is False  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# check_col_non_negative
# ---------------------------------------------------------------------------


def test_check_col_non_negative_passes(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1, 10), (2, 20)], ["qty", "price"])
    assert check_col_non_negative(df, ["qty", "price"], "test_dataset") is True


def test_check_col_non_negative_zero_is_allowed(spark: SparkSession) -> None:
    df = spark.createDataFrame([(0,), (1,), (5,)], ["qty"])
    assert check_col_non_negative(df, ["qty"], "test_dataset") is True


def test_check_col_non_negative_single_column_negatives_halt_false(spark: SparkSession) -> None:
    df = spark.createDataFrame([(-1,), (0,), (5,)], ["qty"])
    assert check_col_non_negative(df, ["qty"], "test_dataset", halt_on_failure=False) is False


def test_check_col_non_negative_single_column_negatives_halt_true(spark: SparkSession) -> None:
    df = spark.createDataFrame([(-1,), (0,), (5,)], ["qty"])
    with pytest.raises(RuntimeError, match="qty"):
        check_col_non_negative(df, ["qty"], "test_dataset", halt_on_failure=True)


def test_check_col_non_negative_multiple_columns_negatives_halt_false(spark: SparkSession) -> None:
    df = spark.createDataFrame([(-1, -5), (0, 0), (5, 10)], ["qty", "price"])
    assert (
        check_col_non_negative(df, ["qty", "price"], "test_dataset", halt_on_failure=False) is False
    )


def test_check_col_non_negative_multiple_columns_negatives_halt_true(spark: SparkSession) -> None:
    df = spark.createDataFrame([(-1, -5), (0, 0), (5, 10)], ["qty", "price"])
    with pytest.raises(RuntimeError) as exc_info:
        check_col_non_negative(df, ["qty", "price"], "test_dataset", halt_on_failure=True)
    message = str(exc_info.value)
    assert "qty" in message
    assert "price" in message


def test_check_col_non_negative_mixed_columns_partial_failure(spark: SparkSession) -> None:
    # qty has a negative; price is clean
    df = spark.createDataFrame([(-1, 5), (0, 10), (3, 15)], ["qty", "price"])
    assert (
        check_col_non_negative(df, ["qty", "price"], "test_dataset", halt_on_failure=False) is False
    )
    with pytest.raises(RuntimeError) as exc_info:
        check_col_non_negative(df, ["qty", "price"], "test_dataset", halt_on_failure=True)
    message = str(exc_info.value)
    assert "qty" in message
    assert "price" not in message


def test_check_col_non_negative_nonexistent_column_halt_false(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1,)], ["qty"])
    assert (
        check_col_non_negative(df, ["nonexistent_col"], "test_dataset", halt_on_failure=False)
        is False
    )


def test_check_col_non_negative_nonexistent_column_halt_true(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1,)], ["qty"])
    with pytest.raises(RuntimeError, match="nonexistent_col"):
        check_col_non_negative(df, ["nonexistent_col"], "test_dataset", halt_on_failure=True)


def test_check_col_non_negative_empty_columns_list(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1,)], ["qty"])
    assert check_col_non_negative(df, [], "test_dataset") is True


def test_check_col_non_negative_halt_none_treated_as_false(spark: SparkSession) -> None:
    df = spark.createDataFrame([(-1,), (0,), (5,)], ["qty"])
    assert check_col_non_negative(df, ["qty"], "test_dataset", halt_on_failure=None) is False  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# check_referential_integrity
# ---------------------------------------------------------------------------


def test_check_referential_integrity_passes(spark: SparkSession) -> None:
    parent = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
    child = spark.createDataFrame([(1,), (2,)], ["parent_id"])
    assert check_referential_integrity(parent, ["id"], child, ["parent_id"], "test_dataset") is True


def test_check_referential_integrity_orphan_rows_halt_false(spark: SparkSession) -> None:
    parent = spark.createDataFrame([(1,), (2,)], ["id"])
    child = spark.createDataFrame([(1,), (99,)], ["parent_id"])
    assert (
        check_referential_integrity(
            parent, ["id"], child, ["parent_id"], "test_dataset", halt_on_failure=False
        )
        is False
    )


def test_check_referential_integrity_orphan_rows_halt_true(spark: SparkSession) -> None:
    parent = spark.createDataFrame([(1,), (2,)], ["id"])
    child = spark.createDataFrame([(1,), (99,)], ["parent_id"])
    with pytest.raises(RuntimeError, match="test_dataset"):
        check_referential_integrity(
            parent, ["id"], child, ["parent_id"], "test_dataset", halt_on_failure=True
        )


def test_check_referential_integrity_composite_keys_all_match(spark: SparkSession) -> None:
    parent = spark.createDataFrame([(1, "a"), (2, "b")], ["region_id", "code"])
    child = spark.createDataFrame([(1, "a"), (2, "b")], ["region_id", "code"])
    assert (
        check_referential_integrity(
            parent, ["region_id", "code"], child, ["region_id", "code"], "test_dataset"
        )
        is True
    )


def test_check_referential_integrity_composite_keys_with_orphans(spark: SparkSession) -> None:
    parent = spark.createDataFrame([(1, "a"), (2, "b")], ["region_id", "code"])
    child = spark.createDataFrame([(1, "a"), (2, "c")], ["region_id", "code"])
    assert (
        check_referential_integrity(
            parent,
            ["region_id", "code"],
            child,
            ["region_id", "code"],
            "test_dataset",
            halt_on_failure=False,
        )
        is False
    )


def test_check_referential_integrity_empty_child_table(spark: SparkSession) -> None:
    parent = spark.createDataFrame([(1,), (2,)], ["id"])
    schema = StructType([StructField("parent_id", IntegerType(), True)])
    child = spark.createDataFrame([], schema)
    assert check_referential_integrity(parent, ["id"], child, ["parent_id"], "test_dataset") is True


def test_check_referential_integrity_all_children_are_orphans(spark: SparkSession) -> None:
    schema = StructType([StructField("id", IntegerType(), True)])
    parent = spark.createDataFrame([], schema)
    child = spark.createDataFrame([(1,), (2,)], ["parent_id"])
    assert (
        check_referential_integrity(
            parent, ["id"], child, ["parent_id"], "test_dataset", halt_on_failure=False
        )
        is False
    )


def test_check_referential_integrity_halt_none_treated_as_false(spark: SparkSession) -> None:
    parent = spark.createDataFrame([(1,)], ["id"])
    child = spark.createDataFrame([(99,)], ["parent_id"])
    assert check_referential_integrity(parent, ["id"], child, ["parent_id"], "test_dataset", halt_on_failure=None) is False  # type: ignore[arg-type]


def test_check_referential_integrity_invalid_child_key_always_raises(spark: SparkSession) -> None:
    parent = spark.createDataFrame([(1,)], ["id"])
    child = spark.createDataFrame([(1,)], ["parent_id"])
    with pytest.raises(RuntimeError):
        check_referential_integrity(
            parent, ["id"], child, ["nonexistent_col"], "test_dataset", halt_on_failure=False
        )


def test_check_referential_integrity_invalid_parent_key_always_raises(spark: SparkSession) -> None:
    parent = spark.createDataFrame([(1,)], ["id"])
    child = spark.createDataFrame([(1,)], ["parent_id"])
    with pytest.raises(RuntimeError):
        check_referential_integrity(
            parent, ["nonexistent_col"], child, ["parent_id"], "test_dataset", halt_on_failure=False
        )
