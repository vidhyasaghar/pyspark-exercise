"""Report generation step: runs all registered transforms and writes CSVs."""

from pathlib import Path
from functools import wraps
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from eternalsalesData.pipeline.context import ExecutionStatus, PipelineContext
from eternalsalesData.utils import spark_session, spark_utils
from eternalsalesData.utils.logger_config import get_logger

logger = get_logger(__name__)

# Transform registry
TRANSFORMS = {}


def transform(output_dir: str):
    """Register a transform function under the given output directory name.

    :param output_dir: Subdirectory name under the pipeline output path.
    :type output_dir: str
    :returns: Decorator that registers and returns the wrapped function unchanged.
    :rtype: Callable
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        TRANSFORMS[func] = output_dir
        return wrapper

    return decorator


# ============ Transform Functions ============


@transform("it_data")
def transform_it_data(df1: DataFrame, df2: DataFrame, df3: DataFrame) -> DataFrame:
    # pylint: disable=unused-argument
    """Return the top 100 IT department employees ordered by sales amount descending.

    :param df1: Employee_details DataFrame.
    :type df1: DataFrame
    :param df2: Employee_calls DataFrame.
    :type df2: DataFrame
    :param df3: Sales_details DataFrame (unused).
    :type df3: DataFrame
    :returns: Top-100 IT rows ordered by sales amount descending.
    :rtype: DataFrame
    """
    result = (
        df1.join(df2, on="id", how="inner")
        .filter(F.col("area") == "IT")
        .orderBy(F.col("sales_amount").desc())
        .limit(100)
    )
    return result


@transform("marketing_address_info")
def transform_marketing_address_info(df1: DataFrame, df2: DataFrame, df3: DataFrame) -> DataFrame:
    # pylint: disable=unused-argument
    """Extract structured address components for Marketing department employees.

    :param df1: Employee_details DataFrame.
    :type df1: DataFrame
    :param df2: Employee_calls DataFrame containing address column.
    :type df2: DataFrame
    :param df3: Sales_details DataFrame (unused).
    :type df3: DataFrame
    :returns: DataFrame with name, street_and_number, area, and zip_code columns.
    :rtype: DataFrame
    """
    result = (
        df1.join(df2, on="id", how="inner")
        .filter(F.col("area") == "Marketing")
        .withColumn(
            "address_without_zip", F.regexp_replace(F.col("address"), r"\d{4} [A-Z]{2}", "")
        )
        .withColumn(
            "only_area", F.regexp_replace(F.col("address_without_zip"), r"([A-Za-z ]+ \d+)", "")
        )
        .withColumn("zip_code", F.regexp_extract(F.col("address"), r"(\d{4} [A-Z]{2})", 1))
        .withColumn(
            "street_and_number",
            F.regexp_extract(F.col("address_without_zip"), r"([A-Za-z ]+ \d+)", 1),
        )
        .withColumn("area", F.regexp_replace(F.col("only_area"), r"[,\s+]", ""))
        .select("name", "street_and_number", "area", "zip_code")
    )
    return result


@transform("department_breakdown")
def transform_department_breakdown(df1: DataFrame, df2: DataFrame, df3: DataFrame) -> DataFrame:
    # pylint: disable=unused-argument
    """Aggregate total sales and call success rate per department.

    :param df1: Employee_details DataFrame.
    :type df1: DataFrame
    :param df2: Employee_calls DataFrame.
    :type df2: DataFrame
    :param df3: Sales_details DataFrame (unused).
    :type df3: DataFrame
    :returns: Aggregated totals and success rate per department area.
    :rtype: DataFrame
    """
    result = (
        df1.join(df2, on="id", how="inner")
        .groupBy("area")
        .agg(
            F.sum(F.col("sales_amount")).alias("total_sales"),
            (F.sum(F.col("calls_successful")) / F.sum(F.col("calls_made")) * 100).alias(
                "success_rate"
            ),
        )
        .withColumn("total_sales_amount", F.format_string("%,.2f", F.col("total_sales")))
        .withColumn("success_rate_pct", F.format_string("%.2f %%", F.col("success_rate")))
        .select("area", "total_sales_amount", "success_rate_pct")
    )
    return result


@transform("top_3")
def transform_best_performer_per_department(
    df1: DataFrame, df2: DataFrame, df3: DataFrame
) -> DataFrame:
    # pylint: disable=unused-argument
    """Return the top 3 performers per department with a call success rate above 75%.

    :param df1: Employee_details DataFrame.
    :type df1: DataFrame
    :param df2: Employee_calls DataFrame.
    :type df2: DataFrame
    :param df3: Sales_details DataFrame (unused).
    :type df3: DataFrame
    :returns: Top-3 ranked employees per area ordered by sales amount and success rate.
    :rtype: DataFrame
    """
    result = (
        df1.join(df2, on="id", how="inner")
        .withColumn("success_rate", F.col("calls_successful") / F.col("calls_made"))
        .filter(F.col("success_rate") > 0.75)
        .withColumn(
            "rank",
            F.rank().over(
                Window.partitionBy("area").orderBy(
                    F.col("sales_amount").desc(), F.col("success_rate").desc()
                )
            ),
        )
        .filter(F.col("rank") <= 3)
        .select(
            "area",
            "name",
            F.format_string("%,.2f", F.col("sales_amount")).alias("sales_amount"),
            F.format_string("%.2f %%", F.col("success_rate") * 100).alias("success_rate_pct"),
            "rank",
        )
    )
    return result


@transform("top_3_most_sold_per_department_netherlands")
def transform_top_3_sold_products_nl(df1: DataFrame, df2: DataFrame, df3: DataFrame) -> DataFrame:
    # pylint: disable=unused-argument
    """Return the top 3 most-sold products per department for Netherlands sales.

    :param df1: Employee_details DataFrame.
    :type df1: DataFrame
    :param df2: Employee_calls DataFrame (unused).
    :type df2: DataFrame
    :param df3: Sales_details DataFrame with country, product_sold, and quantity columns.
    :type df3: DataFrame
    :returns: Top-3 products by total quantity per department area in the Netherlands.
    :rtype: DataFrame
    """
    result = (
        df1.join(df3, df1.id == df3.caller_id, how="inner")
        .filter(F.col("country") == "Netherlands")
        .groupBy("area", "product_sold")
        .agg(F.sum("quantity").alias("total_quantity"))
        .withColumn(
            "rank",
            F.rank().over(Window.partitionBy("area").orderBy(F.col("total_quantity").desc())),
        )
        .filter(F.col("rank") <= 3)
        .select(
            "area",
            "product_sold",
            F.format_string("%,2d", F.col("total_quantity")).alias("total_quantity"),
            "rank",
        )
    )
    return result


@transform("best_salesperson")
def transform_best_salesperson_per_country(
    df1: DataFrame, df2: DataFrame, df3: DataFrame
) -> DataFrame:
    # pylint: disable=unused-argument
    """Return the best salesperson per country ranked by total quantity sold.

    :param df1: Employee_details DataFrame (unused).
    :type df1: DataFrame
    :param df2: Employee_calls DataFrame.
    :type df2: DataFrame
    :param df3: Sales_details DataFrame with country, quantity, and sales_amount columns.
    :type df3: DataFrame
    :returns: Top-ranked salesperson per country with total quantity and sales amount.
    :rtype: DataFrame
    """
    result = (
        df2.join(df3, df2.id == df3.caller_id, how="inner")
        .groupBy("country", "name", "sales_amount")
        .agg(F.sum("quantity").alias("total_quantity"))
        .withColumn(
            "rank",
            F.rank().over(
                Window.partitionBy("country").orderBy(
                    F.col("total_quantity").desc(), F.col("sales_amount").desc()
                )
            ),
        )
        .filter(F.col("rank") == 1)
        .select(
            "name",
            "country",
            F.format_string("%,d", F.col("total_quantity")).alias("total_quantity"),
            F.format_string("%,.2f", F.col("sales_amount")).alias("sales_amount"),
        )
    )
    return result


# ============ Main Pipeline ============


def run(ctx: PipelineContext) -> PipelineContext:
    """Execute all registered transforms and write each result to CSV.

    :param ctx: Pipeline context with validated dataset paths and output directory.
    :type ctx: PipelineContext
    :returns: Updated context with per-report statuses in ``report_statuses``.
    :rtype: PipelineContext
    :raises RuntimeError: If any individual transform raises an exception.
    """
    logger.info("=== Generate Reports ===")

    try:
        spark = spark_session.get_spark_session("Report Generation")

        # Read datasets once
        df1 = spark_utils.read_csv_with_header(spark, str(ctx.dataset_one))
        df2 = spark_utils.read_csv_with_header(spark, str(ctx.dataset_two))
        df3 = spark_utils.read_csv_with_header(spark, str(ctx.dataset_three))

        # Execute all registered transforms
        for transform_func, output_dir in TRANSFORMS.items():
            try:
                logger.info("Generating for : %s", output_dir)

                result_df = transform_func(df1, df2, df3)
                output_path = ctx.output_dir / output_dir

                Path(output_path).mkdir(parents=True, exist_ok=True)
                spark_utils.write_df_to_csv(result_df, str(output_path))

                ctx.report_statuses[output_dir] = ExecutionStatus.SUCCESS
                logger.info("%s saved to %s", output_dir, output_path)

            except Exception as e:  # pylint: disable=broad-except
                logger.error("%s failed: %s", output_dir, e)
                ctx.report_statuses[output_dir] = ExecutionStatus.FAILED
                ctx.add_error(f"{output_dir}: {e}")
                raise
        if not ctx.errors:
            ctx.dq_status = ExecutionStatus.SUCCESS
            logger.info("All reports generated successfully")
        return ctx

    except Exception as e:  # pylint: disable=broad-except
        logger.error("Report generation failed: %s", e)
        ctx.dq_status = ExecutionStatus.FAILED
        ctx.add_error(str(e))
        raise
