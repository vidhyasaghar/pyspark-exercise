"""CLI entry point for sales data processing pipeline."""

import argparse
from pathlib import Path

from sales_data.pipeline.orchestrator import Orchestrator
from sales_data.utils.logger_config import get_logger

logger = get_logger(__name__)


def main() -> int:
    """Parse CLI arguments and run the pipeline orchestrator.

    :returns: Exit code (0 for success, 1 for failure).
    :rtype: int
    """

    _DQ_CHECKS = {
        "row_count",
        "col_unique",
        "col_non_null",
        "referential_integrity",
        "calls_successful_gt_made",
        "address_format",
    }

    parser = argparse.ArgumentParser(
        description="EternalTeleSales data processing pipeline",
        prog="sales-data",
    )

    parser.add_argument(
        "dataset1",
        type=Path,
        help="Path to dataset_one.csv (employee details with department and call counts)",
    )
    parser.add_argument(
        "dataset2",
        type=Path,
        help="Path to dataset_two.csv (employee personal and sales information)",
    )
    parser.add_argument(
        "dataset3",
        type=Path,
        help="Path to dataset_three.csv (sales transactions and product details)",
    )
    parser.add_argument(
        "output_dir",
        type=Path,
        help="Output directory where results will be saved",
    )
    parser.add_argument(
        "--halt-all",
        action="store_true",
        default=False,
        help="Halt on all data quality check failures",
    )
    parser.add_argument(
        "--halt",
        type=str,
        default="",
        help="Comma-separated list of specific DQ checks to halt on " "(row_count, col_unique, col_non_null, referential_integrity, " "calls_successful_gt_made, address_format)",
    )
    args = parser.parse_args()

    # Compute halt_checks set from arguments
    halt_checks = set()
    if args.halt_all:
        halt_checks = _DQ_CHECKS
    if args.halt:
        halt_checks.update(c.strip() for c in args.halt.split(",") if c.strip())

    logger.info("Starting sales data pipeline")
    logger.info("Input datasets: %s, %s, %s", args.dataset1, args.dataset2, args.dataset3)
    logger.info("Output directory: %s", args.output_dir)
    if halt_checks:
        logger.info("Halt on DQ failure for: %s", ", ".join(sorted(halt_checks)))
    else:
        logger.info("DQ checks will warn only (no halts)")

    args.halt_checks = halt_checks
    try:
        orchestrator = Orchestrator(args)
        ctx = orchestrator.run()

        if ctx.errors:
            logger.error("Pipeline completed with %d error(s)", len(ctx.errors))
            return 1

        logger.info("Pipeline completed successfully")
        return 0

    except Exception as e:  # pylint: disable=broad-except
        logger.error("Pipeline failed with exception: %s", str(e))
        return 1


if __name__ == "__main__":
    main()
