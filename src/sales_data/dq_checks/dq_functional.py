"""
Functional data quality checks for Sales data analysis pipeline.

:description: Cross-column and pattern-based business-rule validations applied
    to each dataset. Warnings are logged for every issue found. Passing
    ``halt_on_failure=True`` raises a ``RuntimeError`` on failure.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sales_data.utils.logger_config import get_logger

logger = get_logger(__name__)


def check_calls_successful_gt_made(df: DataFrame, dataset_name: str, halt_on_failure: bool = False) -> bool:
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
    :raises RuntimeError: When *halt_on_failure* is ``True`` and violations exist,
        or when the check itself fails to run.
    """
    try:
        violations = df.filter(F.col("calls_successful") > F.col("calls_made")).count()
    except Exception as e:
        logger.error(
            "[%s] Error occurred while checking calls_successful <= calls_made: %s",
            dataset_name,
            e,
        )
        raise RuntimeError(f"Error occurred while checking dependent attributes in {dataset_name}: {e}") from e
    if violations > 0:
        logger.warning(
            "[%s] %d row(s) where calls_successful > calls_made.",
            dataset_name,
            violations,
        )
        if halt_on_failure:
            raise RuntimeError(f"Dependent attributes check failed in {dataset_name}: {violations} violation(s).")
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
    :raises RuntimeError: When *halt_on_failure* is ``True`` and invalid addresses exist,
        or when the check itself fails to run.
    """
    pattern = r"^[A-Za-z0-9-\s]+, \d+(?:-\d+)?, \d{4} [A-Z]{2}$"
    try:
        invalid = df.filter(~F.col("address").rlike(pattern)).count()
    except Exception as e:
        logger.error(
            "[%s] Error occurred while checking address format: %s",
            dataset_name,
            e,
        )
        raise RuntimeError(f"Error occurred while checking address format in {dataset_name}: {e}") from e
    if invalid > 0:
        logger.warning(
            "[%s] %d address(es) do not match the expected format.",
            dataset_name,
            invalid,
        )
        if halt_on_failure:
            raise RuntimeError(f"Address format check failed in {dataset_name}: {invalid} invalid address(es).")
        return False
    logger.info("[%s] All addresses valid: OK.", dataset_name)
    return True
