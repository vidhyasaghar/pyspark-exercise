"""
Basic data quality checks for Sales data analysis pipeline.

:description: Common validations applied to each dataset
    before any processing begins. Warnings are logged for every issue found.
    Passing ``halt_on_failure=True`` raises a ``RuntimeError`` on failure.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from eternalsalesdata.utils.logger_config import get_logger

logger = get_logger(__name__)


def check_row_count(
    df: DataFrame, expected: int, dataset_name: str, halt_on_failure: bool = False
) -> bool:
    """
    Verify that *df* contains *expected* rows.

    :param df: DataFrame to check.
    :type df: pyspark.sql.DataFrame
    :param expected: Expected row count.
    :type expected: int
    :param dataset_name: Label used in log messages.
    :type dataset_name: str
    :param halt_on_failure: Raise ``RuntimeError`` on failure when ``True``.
    :type halt_on_failure: bool
    :return: ``True`` when the count matches.
    :rtype: bool
    """
    actual = df.count()
    if actual != expected:
        logger.warning(
            "[%s] Row count mismatch: expected %d, got %d.",
            dataset_name,
            expected,
            actual,
        )
        if halt_on_failure:
            raise RuntimeError(f"Row count mismatch in {dataset_name}. Check logs for details.")
        return False
    logger.info("[%s] Row count OK: %d rows.", dataset_name, actual)
    return True


def check_col_non_null(
    df: DataFrame, dataset_name: str, column_names: list[str], halt_on_failure: bool = False
) -> bool:
    """
    Ensure the specified columns have no nulls.

    :param df: DataFrame to check.
    :type df: pyspark.sql.DataFrame
    :param dataset_name: Label used in log messages.
    :type dataset_name: str
    :param column_names: Names of the columns to check.
    :type column_names: list[str]
    :param halt_on_failure: Raise ``RuntimeError`` on failure when ``True``.
    :type halt_on_failure: bool
    :return: ``True`` when all checks pass.
    :rtype: bool
    """
    missing = [col for col in column_names if col not in df.columns]
    if missing:
        raise ValueError(f"Column(s) not found in DataFrame: {', '.join(missing)}")
    ok = True
    errors: list[str] = []
    for col in column_names:
        null_count = df.filter(F.col(col).isNull()).count()
        if null_count > 0:
            logger.warning("[%s] %s has %d null(s).", dataset_name, col, null_count)
            errors.append(col)
            ok = False
    if errors and halt_on_failure:
        raise RuntimeError(
            f"Null values found in {', '.join(errors)} column(s) of {dataset_name}."
            " Check logs for details."
        )
    if ok:
        logger.info("[%s] %s non-null: OK.", dataset_name, ", ".join(column_names))
    return ok


def check_col_unique(
    df: DataFrame, dataset_name: str, column_names: list[str], halt_on_failure: bool = False
) -> bool:
    """
    Ensure the specified columns have no duplicates.

    :param df: DataFrame to check.
    :type df: pyspark.sql.DataFrame
    :param dataset_name: Label used in log messages.
    :type dataset_name: str
    :param column_names: Names of the columns to check.
    :type column_names: list[str]
    :param halt_on_failure: Raise ``RuntimeError`` on failure when ``True``.
    :type halt_on_failure: bool
    :return: ``True`` when all checks pass.
    :rtype: bool
    """
    missing = [col for col in column_names if col not in df.columns]
    if missing:
        raise ValueError(f"Column(s) not found in DataFrame: {', '.join(missing)}")
    ok = True
    errors: list[str] = []
    total = df.count()
    for col in column_names:
        distinct = df.select(col).distinct().count()
        if distinct != total:
            logger.warning(
                "[%s] %s has duplicates: %d total vs %d distinct.",
                dataset_name,
                col,
                total,
                distinct,
            )
            errors.append(col)
            ok = False
    if errors and halt_on_failure:
        raise RuntimeError(
            f"Duplicates found in {', '.join(errors)} column(s) of {dataset_name}."
            " Check logs for details."
        )

    if ok:
        logger.info("[%s] All columns unique: OK.", dataset_name)
    return ok


def check_col_non_negative(
    df: DataFrame, dataset_name: str, columns: list[str], halt_on_failure: bool = False
) -> bool:
    """
    Ensure numeric *columns* contain no negative values.

    :param df: DataFrame to check.
    :type df: pyspark.sql.DataFrame
    :param dataset_name: Label used in log messages.
    :type dataset_name: str
    :param columns: Column names to validate.
    :type columns: list[str]
    :param halt_on_failure: Raise ``RuntimeError`` on failure when ``True``.
    :type halt_on_failure: bool
    :return: ``True`` when all columns pass.
    :rtype: bool
    """
    missing = [col for col in columns if col not in df.columns]
    if missing:
        raise ValueError(f"Column(s) not found in DataFrame: {', '.join(missing)}")
    ok = True
    errors: list[str] = []
    for col in columns:
        neg_count = df.filter(F.col(col) < 0).count()
        if neg_count > 0:
            logger.warning(
                "[%s] Column '%s' has %d negative value(s).",
                dataset_name,
                col,
                neg_count,
            )
            errors.append(col)
            ok = False
    if errors and halt_on_failure:
        raise RuntimeError(
            f"Negative values found in {', '.join(errors)} column(s) of {dataset_name}."
            " Check logs for details."
        )

    if ok:
        logger.info("[%s] All specified columns non-negative: OK.", dataset_name)

    return ok
