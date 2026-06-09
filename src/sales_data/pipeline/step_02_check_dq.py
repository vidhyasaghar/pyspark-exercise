"""Data quality checks step: validate input datasets."""

from pyspark.sql import DataFrame

import sales_data.dq_checks as dq
from sales_data.pipeline.context import ExecutionStatus, PipelineContext
from sales_data.utils import spark_session, spark_utils
from sales_data.utils.logger_config import get_logger

logger = get_logger(__name__)


def run(ctx: PipelineContext) -> PipelineContext:
    """Run data quality checks on all three input datasets.

    :param ctx: Pipeline context with dataset paths and halt configuration.
    :type ctx: PipelineContext
    :returns: Updated context with ``dq_status`` set to SUCCESS or FAILED.
    :rtype: PipelineContext
    :raises RuntimeError: If a halt-enabled DQ check fails.
    """
    logger.info("=== Data Quality Checks ===")

    try:
        spark = spark_session.get_spark_session("Data Quality Checks")
        df1 = spark_utils.read_csv_with_header(spark, str(ctx.dataset_one))
        df2 = spark_utils.read_csv_with_header(spark, str(ctx.dataset_two))
        df3 = spark_utils.read_csv_with_header(spark, str(ctx.dataset_three))

        run_common_checks(df1, df2, df3, ctx)
        run_functional_checks(df1, df2, ctx)

        ctx.dq_status = ExecutionStatus.SUCCESS
        logger.info("Data quality checks completed successfully")
        return ctx

    except RuntimeError:
        ctx.dq_status = ExecutionStatus.FAILED
        raise
    except Exception as e:  # pylint: disable=broad-except
        logger.error("Data quality checks failed with exception: %s", e)
        ctx.dq_status = ExecutionStatus.FAILED
        ctx.add_error(str(e))
        raise


def run_common_checks(
    df1: DataFrame,
    df2: DataFrame,
    df3: DataFrame,
    ctx: PipelineContext,
) -> None:
    """Run basic data quality checks (row counts, uniqueness, nulls) on all datasets.

    :param df1: Employee_details DataFrame.
    :type df1: DataFrame
    :param df2: Employee_calls DataFrame.
    :type df2: DataFrame
    :param df3: Sales_details DataFrame.
    :type df3: DataFrame
    :param ctx: Pipeline context with halt configuration.
    :type ctx: PipelineContext
    :rtype: None
    """
    logger.info("=== Common data quality checks ===")

    for df, dataset_name, expected_row_count in zip(
        [df1, df2, df3],
        ["Employee_details", "Employee_calls", "Sales_details"],
        [1000, 1000, 10000],
    ):
        should_halt = "row_count" in ctx.halt_checks
        dq.check_row_count(df, expected_row_count, dataset_name, should_halt)

        should_halt = "col_unique" in ctx.halt_checks
        dq.check_col_unique(df, dataset_name, ["id"], should_halt)

        should_halt = "col_non_null" in ctx.halt_checks
        dq.check_col_non_null(df, dataset_name, ["id"], should_halt)

    should_halt = "referential_integrity" in ctx.halt_checks
    dq.check_referential_integrity(df1, ["id"], df2, ["id"], "Employee_details", should_halt)
    dq.check_referential_integrity(df2, ["id"], df1, ["id"], "Employee_calls", should_halt)
    dq.check_referential_integrity(df2, ["id"], df3, ["caller_id"], "Employee_calls", should_halt)
    dq.check_referential_integrity(df1, ["id"], df3, ["caller_id"], "Employee_details", should_halt)


def run_functional_checks(
    df1: DataFrame,
    df2: DataFrame,
    ctx: PipelineContext,
) -> None:
    """Run functional data quality checks (dependent attributes, address formats).

    :param df1: Employee_calls DataFrame.
    :type df1: DataFrame
    :param df2: Employee_details DataFrame.
    :type df2: DataFrame
    :param ctx: Pipeline context with halt configuration.
    :type ctx: PipelineContext
    :rtype: None
    """
    logger.info("=== Functional data quality checks ===")

    should_halt = "calls_successful_gt_made" in ctx.halt_checks
    dq.check_calls_successful_gt_made(df1, "Employee_calls", should_halt)

    should_halt = "address_format" in ctx.halt_checks
    dq.check_address_format(df2, "Employee_details", should_halt)
