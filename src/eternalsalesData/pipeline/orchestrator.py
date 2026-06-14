"""Pipeline orchestrator: runs initialize, DQ, and report steps in order."""

import argparse
from pathlib import Path

from eternalsalesData.utils.logger_config import get_logger
from eternalsalesData.pipeline.context import PipelineContext
from eternalsalesData.pipeline import step_01_initialize as initialize
from eternalsalesData.pipeline import step_02_check_dq as dq_check
from eternalsalesData.pipeline import step_03_generate_report as generate_report

logger = get_logger(__name__)


# pylint: disable=too-few-public-methods
class Orchestrator:
    """Drives the sales data pipeline from initialization through report generation.

    :param args: Parsed CLI arguments containing dataset paths, output directory, and halt checks.
    :type args: argparse.Namespace
    """

    def __init__(self, args: argparse.Namespace) -> None:
        self._ctx = PipelineContext(
            dataset_one=Path(args.dataset1).resolve(),
            dataset_two=Path(args.dataset2).resolve(),
            dataset_three=Path(args.dataset3).resolve(),
            output_dir=Path(args.output_dir).resolve(),
            halt_checks=args.halt_checks,
        )

    def run(self) -> PipelineContext:
        """Execute all pipeline steps and return the final context.

        :returns: Pipeline context reflecting the final state after all steps.
        :rtype: PipelineContext
        """
        try:
            self._ctx = initialize.run(self._ctx)
        except FileNotFoundError as exc:
            logger.error("Initialize failed — aborting: %s", exc)
            return self._ctx

        try:
            self._ctx = dq_check.run(self._ctx)
        except RuntimeError:
            logger.error("DQ checks failed with halt enabled — aborting pipeline.")
            return self._ctx

        try:
            self._ctx = generate_report.run(self._ctx)
        except RuntimeError:
            logger.error("Report generation failed — aborting pipeline.")
            return self._ctx

        self._summarise()
        return self._ctx

    def _summarise(self) -> None:
        """Log a human-readable summary of step statuses and any accumulated errors.

        :rtype: None
        """
        ctx = self._ctx
        logger.info("--- Pipeline summary ---")
        logger.info("  initialize:     %s", ctx.init_status.name)
        logger.info("  dq_check:       %s", ctx.dq_status.name)
        for job, status in ctx.report_statuses.items():
            logger.info("  %-45s %s", job, status.name)

        if ctx.errors:
            logger.warning("Errors encountered:")
            for err in ctx.errors:
                logger.warning("  %s", err)
        else:
            logger.info("Pipeline completed with no errors.")
