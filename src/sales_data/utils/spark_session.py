"""SparkSession factory for the sales-data pipeline."""

from pyspark.sql import SparkSession

from sales_data.utils.logger_config import get_logger


def get_spark_session(app_name: str, logger=None) -> SparkSession:
    """Create or retrieve a SparkSession with recommended adaptive query configs."""
    if not app_name:
        app_name = "SparkSession"

    if logger:
        logger.info("Initialising SparkSession: %s", app_name)
    try:
        spark = SparkSession.builder.appName(app_name).config("spark.sql.adaptive.enabled", "true").config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "100MB").config("spark.sql.shuffle.partitions", "10").getOrCreate()  # type: ignore[attr-defined]
    except Exception as e:
        if logger:
            logger.error("Failed to initialise SparkSession: %s", e)
        raise

    if logger:
        logger = get_logger(logger.name, spark_session=spark)
        logger.info("SparkSession '%s' initialised successfully", app_name)
    return spark
