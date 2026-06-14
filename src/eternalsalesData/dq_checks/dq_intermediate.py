"""
Intermediate data quality checks for Sales data analysis pipeline.

:description: Cross-column and pattern-based business-rule validations applied
    to each dataset. Warnings are logged for every issue found. Passing
    ``halt_on_failure=True`` raises a ``RuntimeError`` on failure.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from eternalsalesdata.utils.logger_config import get_logger

logger = get_logger(__name__)


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
    :raises ValueError: When a key column is missing from its respective DataFrame.
    :raises RuntimeError: When *halt_on_failure* is ``True`` and orphans exist.
    """
    missing_parent = [k for k in parent_keys if k not in parent_table.columns]
    if missing_parent:
        raise ValueError(f"Column(s) not found in parent_table: {', '.join(missing_parent)}")
    missing_child = [k for k in child_keys if k not in child_table.columns]
    if missing_child:
        raise ValueError(f"Column(s) not found in child_table: {', '.join(missing_child)}")
    orphan_count = (
        child_table.select(*child_keys).subtract(parent_table.select(*parent_keys)).count()
    )
    if orphan_count > 0:
        logger.warning(
            "[%s] Referential integrity check failed: %d row(s) in child table have no match in parent table.",
            dataset_name,
            orphan_count,
        )
        if halt_on_failure:
            raise RuntimeError(
                f"Referential integrity failed in {dataset_name}: {orphan_count} orphan row(s)."
            )
        return False
    logger.info("[%s] Referential integrity: All rows matched: OK.", dataset_name)
    return True


def check_calls_successful_gt_made(
    df: DataFrame, dataset_name: str, halt_on_failure: bool = False
) -> bool:
    """
    Ensure ``calls_successful`` is never greater than ``calls_made``.

    :param df: DataFrame to check.
    :type df: pyspark.sql.DataFrame
    :param dataset_name: Label used in log messages.
    :type dataset_name: str
    :param halt_on_failure: Raise ``RuntimeError`` on failure when ``True``.
    :type halt_on_failure: bool
    :return: ``True`` when no violations are found.
    :rtype: bool
    :raises ValueError: When ``calls_successful`` or ``calls_made`` is not in the DataFrame.
    :raises RuntimeError: When *halt_on_failure* is ``True`` and violations exist.
    """
    for col in ("calls_successful", "calls_made"):
        if col not in df.columns:
            raise ValueError(f"Column '{col}' not found in DataFrame")
    violations = df.filter(F.col("calls_successful") > F.col("calls_made")).count()
    if violations > 0:
        logger.warning(
            "[%s] %d row(s) where calls_successful > calls_made.",
            dataset_name,
            violations,
        )
        if halt_on_failure:
            raise RuntimeError(
                f"Dependent attributes check failed in {dataset_name}: {violations} violation(s)."
            )
        return False
    logger.info("[%s] calls_successful <= calls_made: OK.", dataset_name)
    return True


def check_address_format(df: DataFrame, dataset_name: str, halt_on_failure: bool = False) -> bool:
    """
    Validate addresses follow the format ``[street], [number], [DDDD XX]``.

    The zip code must be four digits, a space, and two capital letters.

    :param df: DataFrame to check.
    :type df: pyspark.sql.DataFrame
    :param dataset_name: Label used in log messages.
    :type dataset_name: str
    :param halt_on_failure: Raise ``RuntimeError`` on failure when ``True``.
    :type halt_on_failure: bool
    :return: ``True`` when all addresses match the expected format.
    :rtype: bool
    :raises ValueError: When the ``address`` column is not in the DataFrame.
    :raises RuntimeError: When *halt_on_failure* is ``True`` and invalid addresses exist.
    """
    if "address" not in df.columns:
        raise ValueError("Column 'address' not found in DataFrame")
    pattern = r"^[A-Za-z0-9-\s]+, \d+, \d{4} [A-Z]{2}$"
    invalid = df.filter(F.col("address").isNull() | ~F.col("address").rlike(pattern)).count()
    if invalid > 0:
        logger.warning(
            "[%s] %d address(es) do not match the expected format.",
            dataset_name,
            invalid,
        )
        if halt_on_failure:
            raise RuntimeError(
                f"Address format check failed in {dataset_name}: {invalid} invalid address(es)."
            )
        return False
    logger.info("[%s] All addresses valid: OK.", dataset_name)
    return True
