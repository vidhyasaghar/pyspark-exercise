"""CSV read/write utilities for PySpark DataFrames."""

from pathlib import Path
from pyspark.sql import DataFrame

from sales_data.utils.logger_config import get_logger

logger = get_logger(__name__)


def read_csv_with_header(spark, path: str) -> DataFrame:
    """Read a CSV file with header row and inferred schema."""
    if not path.endswith(".csv") or not Path(path).is_file():
        logger.error("Invalid file path: %s", path)
        raise ValueError(f"Invalid file path: {path}")
    try:
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
        logger.info("Successfully read CSV from %s", path)
        return df
    except Exception as e:
        logger.error("Error reading CSV from %s: %s", path, e)
        raise


def write_df_to_csv(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    """Write a DataFrame to CSV, coalescing to a single output file."""
    if not Path(path).parent.exists():
        logger.error("Output directory does not exist: %s", path)
        raise ValueError(f"Output directory does not exist: {path}")
    try:
        df.coalesce(1).write.mode(mode).option("header", "true").csv(path)
        logger.info("Successfully wrote DataFrame to CSV at %s", path)
    except Exception as e:
        logger.error("Error writing DataFrame to CSV at %s: %s", path, e)
        raise
