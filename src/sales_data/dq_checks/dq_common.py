"""
Data quality checks for Sales data analysis pipeline.

:description: Basic and intermediate validations applied to each dataset
    before any processing begins. Warnings are logged for every issue found.
    Passing ``halt_on_failure=True`` raises a ``RuntimeError`` on failure.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sales_data.utils.logger_config import get_logger

logger = get_logger(__name__)


def check_row_count(df: DataFrame, expected: int, dataset_name: str, halt_on_failure: bool = False) -> bool:
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


def check_col_non_null(df: DataFrame, dataset_name: str, column_names: list[str], halt_on_failure: bool = False) -> bool:
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
    ok = True
    errors: list[str] = []
    for col in column_names:
        try:
            null_count = df.filter(F.col(col).isNull()).count()
        except Exception as e:  # pylint: disable=broad-except
            logger.error(
                "[%s] Error checking nulls in column '%s': %s",
                dataset_name,
                col,
                e,
            )
            errors.append(col)
            ok = False
            continue
        if null_count > 0:
            logger.warning("[%s] %s has %d null(s).", dataset_name, col, null_count)
            errors.append(col)
            ok = False
    if errors and halt_on_failure:
        raise RuntimeError(f"Null values found in {', '.join(errors)} column(s) of {dataset_name}. Check logs for details.")
    if ok:
        logger.info("[%s] %s non-null: OK.", dataset_name, ", ".join(column_names))
    return ok


def check_col_unique(df: DataFrame, dataset_name: str, column_names: list[str], halt_on_failure: bool = False) -> bool:
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
    ok = True
    errors: list[str] = []
    total = df.count()
    for col in column_names:
        try:
            distinct = df.select(col).distinct().count()
        except Exception as e:  # pylint: disable=broad-except
            logger.error(
                "[%s] Error checking uniqueness in column '%s': %s",
                dataset_name,
                col,
                e,
            )
            errors.append(col)
            ok = False
            continue
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
        raise RuntimeError(f"Duplicates found in {', '.join(errors)} column(s) of {dataset_name}. Check logs for details.")

    if ok:
        logger.info("[%s] All columns unique: OK.", dataset_name)
    return ok


def check_col_non_negative(df: DataFrame, columns: list[str], dataset_name: str, halt_on_failure: bool = False) -> bool:
    """
    Ensure numeric *columns* contain no negative values.

    :param df: DataFrame to check.
    :type df: pyspark.sql.DataFrame
    :param columns: Column names to validate.
    :type columns: list[str]
    :param dataset_name: Label used in log messages.
    :type dataset_name: str
    :param halt_on_failure: Raise ``RuntimeError`` on failure when ``True``.
    :type halt_on_failure: bool
    :return: ``True`` when all columns pass.
    :rtype: bool
    """
    ok = True
    errors: list[str] = []
    for col in columns:
        try:
            neg_count = df.filter(F.col(col) < 0).count()
        except Exception as e:  # pylint: disable=broad-except
            logger.error(
                "[%s] Error checking non-negativity in column '%s': %s",
                dataset_name,
                col,
                e,
            )
            errors.append(col)
            ok = False
            continue
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
        raise RuntimeError(f"Negative values found in {', '.join(errors)} column(s) of {dataset_name}. Check logs for details.")

    if ok:
        logger.info("[%s] All specified columns non-negative: OK.", dataset_name)

    return ok


def check_referential_integrity(  # pylint: disable=too-many-arguments
    parent_table: DataFrame,
    parent_keys: list[str],
    child_table: DataFrame,
    child_keys: list[str],
    dataset_name: str,
    halt_on_failure: bool = False,
) -> bool:
    """
    Verify every row in *child_table* has a matching row in *parent_table*
    on the given *join_keys*.

    :param parent_table: The parent dataset containing the reference values.
    :type parent_table: pyspark.sql.DataFrame
    :param child_table: The child dataset containing the values to check.
    :type child_table: pyspark.sql.DataFrame
    :param parent_keys: List of columns in the parent table to join on.
    :type parent_keys: list[str]
    :param child_keys: List of columns in the child table to join on.
    :type child_keys: list[str]
    :param dataset_name: Label used in log messages.
    :type dataset_name: str
    :param halt_on_failure: Raise ``RuntimeError`` on failure when ``True``.
    :type halt_on_failure: bool
    :return: ``True`` when every child row has a matching parent row.
    :rtype: bool
    :raises RuntimeError: When *halt_on_failure* is ``True`` and orphans exist,
        or when the check itself fails to run.
    """
    try:
        orphan_count = child_table.select(*child_keys).subtract(parent_table.select(*parent_keys)).count()
    except Exception as e:  # pylint: disable=broad-except
        logger.error(
            "[%s] Error occurred while checking referential integrity: %s",
            dataset_name,
            e,
        )
        raise RuntimeError(f"Error occurred while checking referential integrity in {dataset_name}: {e}") from e
    if orphan_count > 0:
        logger.warning(
            "[%s] Referential integrity check failed: %d row(s) in child table have no match in parent table.",
            dataset_name,
            orphan_count,
        )
        if halt_on_failure:
            raise RuntimeError(f"Referential integrity failed in {dataset_name}: {orphan_count} orphan row(s).")
        return False
    logger.info("[%s] Referential integrity: All rows matched: OK.", dataset_name)
    return True
