"""Tests for eternalsalesdata.pipeline.step_03_generate_report."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StructField, StructType

from eternalsalesdata.pipeline.context import ExecutionStatus, PipelineContext
from eternalsalesdata.pipeline.step_03_generate_report import (
    TRANSFORMS,
    run,
    transform,
    transform_best_performer_per_department,
    transform_best_salesperson_per_country,
    transform_department_breakdown,
    transform_it_data,
    transform_marketing_address_info,
    transform_top_3_sold_products_nl,
)

_PATCH_GET_SPARK = (
    "eternalsalesdata.pipeline.step_03_generate_report.spark_session.get_spark_session"
)
_PATCH_READ_CSV = (
    "eternalsalesdata.pipeline.step_03_generate_report.spark_utils.read_csv_with_header"
)
_PATCH_WRITE_CSV = "eternalsalesdata.pipeline.step_03_generate_report.spark_utils.write_df_to_csv"
_PATCH_TRANSFORMS = "eternalsalesdata.pipeline.step_03_generate_report.TRANSFORMS"


def _make_ctx() -> PipelineContext:
    return PipelineContext(
        dataset_one=Path("/fake/one.csv"),
        dataset_two=Path("/fake/two.csv"),
        dataset_three=Path("/fake/three.csv"),
        output_dir=Path("/fake/output"),
    )


# ---------------------------------------------------------------------------
# transform decorator
# ---------------------------------------------------------------------------


def test_transform_decorator_registers_function_in_transforms() -> None:
    @transform("_test_sentinel_dir")
    def _fn(*_):
        pass

    assert any(v == "_test_sentinel_dir" for v in TRANSFORMS.values())


def test_transform_decorator_wrapped_function_returns_original_result() -> None:
    obj = object()

    @transform("_test_passthrough_dir")
    def _fn(*_):
        return obj

    assert _fn(None, None, None) is obj


# ---------------------------------------------------------------------------
# transform_it_data
# df1: id, name, area   |   df2: id, sales_amount   |   df3: unused
# ---------------------------------------------------------------------------


def test_transform_it_data_returns_only_it_rows(spark: SparkSession) -> None:
    df1 = spark.createDataFrame(
        [(1, "Alice", "IT"), (2, "Bob", "Marketing")],
        ["id", "name", "area"],
    )
    df2 = spark.createDataFrame([(1, 500.0), (2, 800.0)], ["id", "sales_amount"])
    df3 = spark.range(0)

    result = transform_it_data(df1, df2, df3)

    expected = spark.createDataFrame(
        [(1, "Alice", "IT", 500.0)],
        ["id", "name", "area", "sales_amount"],
    )
    assert_df_equality(
        result.select("id", "name", "area", "sales_amount"), expected, ignore_nullable=True
    )


def test_transform_it_data_ordered_by_sales_amount_desc(spark: SparkSession) -> None:
    df1 = spark.createDataFrame(
        [(1, "Alice", "IT"), (2, "Bob", "IT")],
        ["id", "name", "area"],
    )
    df2 = spark.createDataFrame([(1, 500.0), (2, 1000.0)], ["id", "sales_amount"])
    df3 = spark.range(0)

    result = transform_it_data(df1, df2, df3)

    rows = [r["name"] for r in result.collect()]
    assert rows == ["Bob", "Alice"]


def test_transform_it_data_null_sales_amount_sorted_last(spark: SparkSession) -> None:
    df1 = spark.createDataFrame(
        [(1, "Alice", "IT"), (2, "Bob", "IT")],
        ["id", "name", "area"],
    )
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("sales_amount", DoubleType(), True),
        ]
    )
    df2 = spark.createDataFrame([(1, 500.0), (2, None)], schema)
    df3 = spark.range(0)

    result = transform_it_data(df1, df2, df3)

    rows = [r["name"] for r in result.collect()]
    assert rows[0] == "Alice"  # 500.0 first
    assert rows[1] == "Bob"  # null → 0.0, last


def test_transform_it_data_inner_join_excludes_unmatched_employees(spark: SparkSession) -> None:
    df1 = spark.createDataFrame(
        [(1, "Alice", "IT"), (2, "Bob", "IT")],
        ["id", "name", "area"],
    )
    df2 = spark.createDataFrame([(1, 500.0)], ["id", "sales_amount"])  # Bob (id=2) has no match
    df3 = spark.range(0)

    result = transform_it_data(df1, df2, df3)

    assert result.count() == 1
    assert result.first()["name"] == "Alice"


def test_transform_it_data_limited_to_100_rows(spark: SparkSession) -> None:
    df1 = spark.createDataFrame(
        [(i, f"emp_{i}", "IT") for i in range(1, 112)],
        ["id", "name", "area"],
    )
    df2 = spark.createDataFrame(
        [(i, float(i * 10)) for i in range(1, 112)],
        ["id", "sales_amount"],
    )
    df3 = spark.range(0)

    result = transform_it_data(df1, df2, df3)

    assert result.count() == 100


def test_transform_it_data_empty_when_no_it_employees(spark: SparkSession) -> None:
    df1 = spark.createDataFrame([(1, "Alice", "Marketing")], ["id", "name", "area"])
    df2 = spark.createDataFrame([(1, 500.0)], ["id", "sales_amount"])
    df3 = spark.range(0)

    result = transform_it_data(df1, df2, df3)

    assert result.count() == 0


# ---------------------------------------------------------------------------
# transform_marketing_address_info
# df1: id, name, area   |   df2: id, address   |   df3: unused
# Output: name, address (zip stripped), zip_code
# Address format (DQ-validated): "Street, HouseNumber, DDDD XX"
# ---------------------------------------------------------------------------


def test_transform_marketing_address_info_filters_marketing_only(spark: SparkSession) -> None:
    df1 = spark.createDataFrame(
        [(1, "Alice", "Marketing"), (2, "Bob", "IT")],
        ["id", "name", "area"],
    )
    df2 = spark.createDataFrame(
        [(1, "Main Street, 10, 1234 AB"), (2, "Oak Lane, 5, 5678 CD")],
        ["id", "address"],
    )
    df3 = spark.range(0)

    result = transform_marketing_address_info(df1, df2, df3)

    assert result.count() == 1
    assert result.first().name == "Alice"


def test_transform_marketing_address_info_address_parsing(spark: SparkSession) -> None:
    df1 = spark.createDataFrame([(1, "Alice", "Marketing")], ["id", "name", "area"])
    df2 = spark.createDataFrame([(1, "Main Street, 10, 1234 AB")], ["id", "address"])
    df3 = spark.range(0)

    result = transform_marketing_address_info(df1, df2, df3)

    assert result.columns == ["name", "address", "zip_code"]
    row = result.first()
    assert row.address == "Main Street, 10"
    assert row.zip_code == "1234 AB"


def test_transform_marketing_address_info_zip_at_start(spark: SparkSession) -> None:
    # Real-data variant: "DDDD XX, City" — zip precedes the city name
    df1 = spark.createDataFrame([(1, "Alice", "Marketing")], ["id", "name", "area"])
    df2 = spark.createDataFrame([(1, "2588 VD, Kropswolde")], ["id", "address"])
    df3 = spark.range(0)

    result = transform_marketing_address_info(df1, df2, df3)

    row = result.first()
    assert row.address == "Kropswolde"
    assert row.zip_code == "2588 VD"


def test_transform_marketing_address_info_zip_in_middle(spark: SparkSession) -> None:
    # Real-data variant: "Street HouseNumber, DDDD XX, City"
    df1 = spark.createDataFrame([(1, "Alice", "Marketing")], ["id", "name", "area"])
    df2 = spark.createDataFrame([(1, "Lindehof 5, 4133 HB, Nederhemert")], ["id", "address"])
    df3 = spark.range(0)

    result = transform_marketing_address_info(df1, df2, df3)

    row = result.first()
    assert row.address == "Lindehof 5, Nederhemert"
    assert row.zip_code == "4133 HB"


def test_transform_marketing_address_info_empty_when_no_marketing_employees(
    spark: SparkSession,
) -> None:
    df1 = spark.createDataFrame([(1, "Bob", "IT")], ["id", "name", "area"])
    df2 = spark.createDataFrame([(1, "Main Street, 10, 1234 AB")], ["id", "address"])
    df3 = spark.range(0)

    result = transform_marketing_address_info(df1, df2, df3)

    assert result.count() == 0


# ---------------------------------------------------------------------------
# transform_department_breakdown
# df1: id, area   |   df2: id, sales_amount, calls_successful, calls_made   |   df3: unused
# ---------------------------------------------------------------------------


def test_transform_department_breakdown_produces_one_row_per_area(spark: SparkSession) -> None:
    df1 = spark.createDataFrame([(1, "IT"), (2, "IT"), (3, "Marketing")], ["id", "area"])
    df2 = spark.createDataFrame(
        [(1, 500.0, 4, 5), (2, 300.0, 3, 4), (3, 800.0, 5, 6)],
        ["id", "sales_amount", "calls_successful", "calls_made"],
    )
    df3 = spark.range(0)

    result = transform_department_breakdown(df1, df2, df3)

    assert result.count() == 2


def test_transform_department_breakdown_aggregation_and_formatting(spark: SparkSession) -> None:
    # 3/4 calls successful = 75.0% | total sales 1500 → "1,500.00"
    df1 = spark.createDataFrame([(1, "IT")], ["id", "area"])
    df2 = spark.createDataFrame(
        [(1, 1500.0, 3, 4)],
        ["id", "sales_amount", "calls_successful", "calls_made"],
    )
    df3 = spark.range(0)

    result = transform_department_breakdown(df1, df2, df3)

    assert result.columns == ["area", "total_sales_amount", "success_rate_pct"]
    row = result.first()
    assert row.total_sales_amount == "1,500.00"
    assert row.success_rate_pct == "75.00 %"


# ---------------------------------------------------------------------------
# transform_best_performer_per_department
# df1: id, name, area   |   df2: id, sales_amount, calls_successful, calls_made   |   df3: unused
# ---------------------------------------------------------------------------


def test_transform_best_performer_per_department_excludes_at_or_below_75_pct_success_rate(
    spark: SparkSession,
) -> None:
    # Alice: 3/4 = 0.75 → excluded (strictly >)
    # Bob:   4/5 = 0.80 → included
    df1 = spark.createDataFrame(
        [(1, "Alice", "IT"), (2, "Bob", "IT")],
        ["id", "name", "area"],
    )
    df2 = spark.createDataFrame(
        [(1, 1000.0, 3, 4), (2, 800.0, 4, 5)],
        ["id", "sales_amount", "calls_successful", "calls_made"],
    )
    df3 = spark.range(0)

    result = transform_best_performer_per_department(df1, df2, df3)

    names = [row.name for row in result.collect()]
    assert "Alice" not in names
    assert "Bob" in names


def test_transform_best_performer_per_department_top_3_ranking_and_output(
    spark: SparkSession,
) -> None:
    # 5 qualifying IT employees (all 4/5 = 0.80); top 3 by sales; emp_5 (500) is rank 1
    df1 = spark.createDataFrame(
        [(i, f"emp_{i}", "IT") for i in range(1, 6)],
        ["id", "name", "area"],
    )
    df2 = spark.createDataFrame(
        [(i, float(i * 100), 4, 5) for i in range(1, 6)],
        ["id", "sales_amount", "calls_successful", "calls_made"],
    )
    df3 = spark.range(0)

    result = transform_best_performer_per_department(df1, df2, df3)

    assert result.count() == 3
    assert result.columns == ["area", "name", "sales_amount", "success_rate_pct", "rank"]
    assert result.filter(result.rank == 1).first().name == "emp_5"


def test_transform_best_performer_per_department_empty_when_none_qualify(
    spark: SparkSession,
) -> None:
    # Both at or below the 0.75 threshold
    df1 = spark.createDataFrame(
        [(1, "Alice", "IT"), (2, "Bob", "IT")],
        ["id", "name", "area"],
    )
    df2 = spark.createDataFrame(
        [(1, 1000.0, 3, 4), (2, 800.0, 2, 4)],
        ["id", "sales_amount", "calls_successful", "calls_made"],
    )
    df3 = spark.range(0)

    result = transform_best_performer_per_department(df1, df2, df3)

    assert result.count() == 0


# ---------------------------------------------------------------------------
# transform_top_3_sold_products_nl
# df1: id, area   |   df2: unused   |   df3: caller_id, country, product_sold, quantity
# Join key: df1.id == df3.caller_id
# ---------------------------------------------------------------------------


def test_transform_top_3_sold_products_nl_filters_netherlands_only(spark: SparkSession) -> None:
    df1 = spark.createDataFrame([(1, "IT"), (2, "IT")], ["id", "area"])
    df3 = spark.createDataFrame(
        [(1, "Netherlands", "Widget", 10), (2, "Germany", "Gadget", 20)],
        ["caller_id", "country", "product_sold", "quantity"],
    )
    df2 = spark.range(0)

    result = transform_top_3_sold_products_nl(df1, df2, df3)

    assert result.count() == 1
    assert result.first().product_sold == "Widget"


def test_transform_top_3_sold_products_nl_aggregation_and_output(spark: SparkSession) -> None:
    # Two NL sales for Widget in IT → total_quantity 15, rank 1
    df1 = spark.createDataFrame([(1, "IT"), (2, "IT")], ["id", "area"])
    df3 = spark.createDataFrame(
        [(1, "Netherlands", "Widget", 10), (2, "Netherlands", "Widget", 5)],
        ["caller_id", "country", "product_sold", "quantity"],
    )
    df2 = spark.range(0)

    result = transform_top_3_sold_products_nl(df1, df2, df3)

    assert result.columns == ["area", "product_sold", "total_quantity", "rank"]
    expected = spark.createDataFrame(
        [("IT", "Widget", "15", 1)],
        ["area", "product_sold", "total_quantity", "rank"],
    )
    assert_df_equality(result, expected, ignore_nullable=True, ignore_schema=True)


def test_transform_top_3_sold_products_nl_limits_to_top_3_per_department(
    spark: SparkSession,
) -> None:
    # 4 distinct products in IT, NL → only top 3 by quantity survive
    df1 = spark.createDataFrame([(i, "IT") for i in range(1, 5)], ["id", "area"])
    df3 = spark.createDataFrame(
        [
            (1, "Netherlands", "Widget", 40),
            (2, "Netherlands", "Gadget", 30),
            (3, "Netherlands", "Doohickey", 20),
            (4, "Netherlands", "Thingamajig", 10),
        ],
        ["caller_id", "country", "product_sold", "quantity"],
    )
    df2 = spark.range(0)

    result = transform_top_3_sold_products_nl(df1, df2, df3)

    assert result.count() == 3
    assert "Thingamajig" not in [row.product_sold for row in result.collect()]


def test_transform_top_3_sold_products_nl_empty_when_no_netherlands_sales(
    spark: SparkSession,
) -> None:
    df1 = spark.createDataFrame([(1, "IT")], ["id", "area"])
    df3 = spark.createDataFrame(
        [(1, "Germany", "Widget", 10)],
        ["caller_id", "country", "product_sold", "quantity"],
    )
    df2 = spark.range(0)

    result = transform_top_3_sold_products_nl(df1, df2, df3)

    assert result.count() == 0


# ---------------------------------------------------------------------------
# transform_best_salesperson_per_country
# df1: unused   |   df2: id, name, sales_amount   |   df3: caller_id, country, quantity
# Join key: df2.id == df3.caller_id
# ---------------------------------------------------------------------------


def test_transform_best_salesperson_per_country_returns_one_per_country(
    spark: SparkSession,
) -> None:
    df2 = spark.createDataFrame(
        [(1, "Alice", 500.0), (2, "Bob", 300.0), (3, "Charlie", 400.0)],
        ["id", "name", "sales_amount"],
    )
    df3 = spark.createDataFrame(
        [(1, "Netherlands", 10), (2, "Netherlands", 5), (3, "Germany", 15)],
        ["caller_id", "country", "quantity"],
    )
    df1 = spark.range(0)

    result = transform_best_salesperson_per_country(df1, df2, df3)

    assert result.count() == 2
    assert {row.country for row in result.collect()} == {"Netherlands", "Germany"}


def test_transform_best_salesperson_per_country_winner_and_output(spark: SparkSession) -> None:
    # Alice total_quantity 20 > Bob total_quantity 5 → Alice wins Netherlands
    df2 = spark.createDataFrame(
        [(1, "Alice", 500.0), (2, "Bob", 300.0)],
        ["id", "name", "sales_amount"],
    )
    df3 = spark.createDataFrame(
        [(1, "Netherlands", 10), (1, "Netherlands", 10), (2, "Netherlands", 5)],
        ["caller_id", "country", "quantity"],
    )
    df1 = spark.range(0)

    result = transform_best_salesperson_per_country(df1, df2, df3)

    assert result.columns == ["name", "country", "total_quantity", "sales_amount"]
    expected = spark.createDataFrame(
        [("Alice", "Netherlands", "20", "500.00")],
        ["name", "country", "total_quantity", "sales_amount"],
    )
    assert_df_equality(result, expected, ignore_nullable=True)


def test_transform_best_salesperson_per_country_same_name_different_id_not_merged(
    spark: SparkSession,
) -> None:
    # Two employees share the name "Alice" but have different ids — they must not be merged.
    df2 = spark.createDataFrame(
        [(1, "Alice", 500.0), (2, "Alice", 300.0)],
        ["id", "name", "sales_amount"],
    )
    df3 = spark.createDataFrame(
        [(1, "Netherlands", 10), (2, "Netherlands", 20)],
        ["caller_id", "country", "quantity"],
    )
    df1 = spark.range(0)

    result = transform_best_salesperson_per_country(df1, df2, df3)

    # id=2 has total_quantity=20, wins. id=1 has total_quantity=10, loses.
    assert result.count() == 1
    winner = result.first()
    assert winner.total_quantity == "20"


def test_transform_best_salesperson_per_country_empty_when_no_join_matches(
    spark: SparkSession,
) -> None:
    df2 = spark.createDataFrame([(1, "Alice", 500.0)], ["id", "name", "sales_amount"])
    df3 = spark.createDataFrame([(99, "Netherlands", 10)], ["caller_id", "country", "quantity"])
    df1 = spark.range(0)

    result = transform_best_salesperson_per_country(df1, df2, df3)

    assert result.count() == 0


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


def test_run_sets_report_success_and_does_not_touch_dq_status() -> None:
    ctx = _make_ctx()
    mock_df = MagicMock()
    stub = MagicMock(return_value=mock_df)

    with (
        patch(_PATCH_GET_SPARK),
        patch(_PATCH_READ_CSV, return_value=mock_df),
        patch(_PATCH_WRITE_CSV),
        patch(_PATCH_TRANSFORMS, {stub: "report_a"}),
        patch.object(Path, "mkdir"),
    ):
        run(ctx)

    assert ctx.report_statuses["report_a"] == ExecutionStatus.SUCCESS
    # run() must never write dq_status
    from eternalsalesdata.pipeline.context import ExecutionStatus as ES

    assert ctx.dq_status == ES.PENDING


def test_run_transform_failure_records_failed_continues_loop_no_raise() -> None:
    ctx = _make_ctx()
    mock_df = MagicMock()
    stub_fail = MagicMock(side_effect=RuntimeError("transform error"))
    stub_ok = MagicMock(return_value=mock_df)

    with (
        patch(_PATCH_GET_SPARK),
        patch(_PATCH_READ_CSV, return_value=mock_df),
        patch(_PATCH_WRITE_CSV),
        patch(_PATCH_TRANSFORMS, {stub_fail: "report_b", stub_ok: "report_c"}),
        patch.object(Path, "mkdir"),
    ):
        result = run(ctx)  # must NOT raise

    assert result is ctx
    assert ctx.report_statuses.get("report_b") == ExecutionStatus.FAILED
    assert ctx.report_statuses.get("report_c") == ExecutionStatus.SUCCESS
    assert len(ctx.errors) == 1  # exactly one error per failing report
    assert "report_b" in ctx.errors[0]
    # dq_status untouched
    assert ctx.dq_status == ExecutionStatus.PENDING


def test_run_propagates_spark_init_failure_without_touching_dq_status() -> None:
    ctx = _make_ctx()

    with (
        patch(_PATCH_GET_SPARK, side_effect=RuntimeError("spark down")),
        pytest.raises(RuntimeError),
    ):
        run(ctx)

    # dq_status is owned by step_02, not step_03
    assert ctx.dq_status == ExecutionStatus.PENDING
