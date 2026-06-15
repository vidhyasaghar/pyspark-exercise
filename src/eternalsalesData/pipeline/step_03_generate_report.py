"""Report generation step: runs all registered transforms and writes CSVs."""

from pathlib import Path
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from eternalsalesdata.pipeline.context import ExecutionStatus, PipelineContext
from eternalsalesdata.utils import spark_session, spark_utils
from eternalsalesdata.utils.logger_config import get_logger

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
        TRANSFORMS[func] = output_dir
        return func

    return decorator


# ============ Transform Functions ============


@transform("it_data")
def transform_it_data(df1: DataFrame, df2: DataFrame, df3: DataFrame) -> DataFrame:
    # pylint: disable=unused-argument
    """Return the top 100 IT department employees ordered by sales amount descending.

    :param df1: Employee_calls DataFrame (area, calls columns).
    :type df1: DataFrame
    :param df2: Employee_details DataFrame (sales_amount column).
    :type df2: DataFrame
    :param df3: Sales_details DataFrame (unused).
    :type df3: DataFrame
    :returns: Top-100 IT rows ordered by sales amount descending.
    :rtype: DataFrame
    """
    result = (
        df1.join(df2, on="id", how="inner")
        .filter(F.col("area") == "IT")
        .withColumn("sales_amount", F.coalesce(F.col("sales_amount"), F.lit(0.0)))
        .orderBy(F.col("sales_amount").desc())
        .limit(100)
    )
    return result


@transform("marketing_address_info")
def transform_marketing_address_info(df1: DataFrame, df2: DataFrame, df3: DataFrame) -> DataFrame:
    # pylint: disable=unused-argument
    """Extract name, address (without zip), and zip code for Marketing department employees.

    :param df1: Employee_calls DataFrame (area, name columns).
    :type df1: DataFrame
    :param df2: Employee_details DataFrame (address column).
    :type df2: DataFrame
    :param df3: Sales_details DataFrame (unused).
    :type df3: DataFrame
    :returns: DataFrame with name, address (zip stripped), and zip_code columns.
    :rtype: DataFrame
    """
    result = (
        df1.join(df2, on="id", how="inner")
        .filter(F.col("area") == "Marketing")
        .withColumn("zip_code", F.regexp_extract(F.col("address"), r"(\d{4} [A-Z]{2})", 1))
        .withColumn(
            "address",
            F.trim(
                F.regexp_replace(
                    F.regexp_replace(F.col("address"), r"\d{4} [A-Z]{2},?\s*", ""),
                    r"^,\s*|,\s*$",
                    "",
                )
            ),
        )
        .select("name", "address", "zip_code")
    )
    return result


@transform("department_breakdown")
def transform_department_breakdown(df1: DataFrame, df2: DataFrame, df3: DataFrame) -> DataFrame:
    # pylint: disable=unused-argument
    """Aggregate total sales and call success rate per department.

    :param df1: Employee_calls DataFrame (area, calls_successful, calls_made columns).
    :type df1: DataFrame
    :param df2: Employee_details DataFrame (sales_amount column).
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

    :param df1: Employee_calls DataFrame (area, calls_successful, calls_made columns).
    :type df1: DataFrame
    :param df2: Employee_details DataFrame (name, sales_amount columns).
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

    :param df1: Employee_calls DataFrame (area column; joined on id == caller_id).
    :type df1: DataFrame
    :param df2: Employee_details DataFrame (unused).
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
            F.format_string("%,d", F.col("total_quantity")).alias("total_quantity"),
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

    :param df1: Employee_calls DataFrame (unused).
    :type df1: DataFrame
    :param df2: Employee_details DataFrame (name, sales_amount columns).
    :type df2: DataFrame
    :param df3: Sales_details DataFrame with country, quantity, and sales_amount columns.
    :type df3: DataFrame
    :returns: Top-ranked salesperson per country with total quantity and sales amount.
    :rtype: DataFrame
    """
    result = (
        df2.join(df3, df2.id == df3.caller_id, how="inner")
        .groupBy(df2["id"], "country", "name", "sales_amount")
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
    :raises Exception: If Spark initialisation or dataset reads fail (not per-report failures).
    """
    logger.info("=== Generate Reports ===")

    spark = spark_session.get_spark_session("Report Generation")
    df1 = spark_utils.read_csv_with_header(spark, str(ctx.dataset_one))
    df2 = spark_utils.read_csv_with_header(spark, str(ctx.dataset_two))
    df3 = spark_utils.read_csv_with_header(spark, str(ctx.dataset_three))

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

    logger.info("All reports processed")
    return ctx
