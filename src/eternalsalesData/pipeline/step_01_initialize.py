"""Initialize step: validate input datasets and output directory."""

from eternalsalesData.pipeline.context import PipelineContext, ExecutionStatus
from eternalsalesData.utils.logger_config import get_logger

logger = get_logger(__name__)


def run(ctx: PipelineContext) -> PipelineContext:
    """Validate input datasets and output directory before pipeline execution.

    :param ctx: Pipeline context carrying dataset paths and output directory.
    :type ctx: PipelineContext
    :returns: Updated context with ``init_status`` set to SUCCESS.
    :rtype: PipelineContext
    :raises FileNotFoundError: If any dataset path is missing, not a file, or not a CSV.
    :raises OSError: If the output directory cannot be created.
    """
    logger.info("=== Initialize: Validating input ===")

    try:
        # Validate input datasets exist
        for dataset_path, name in [
            (ctx.dataset_one, "dataset_one"),
            (ctx.dataset_two, "dataset_two"),
            (ctx.dataset_three, "dataset_three"),
        ]:
            if not dataset_path.exists():
                raise FileNotFoundError(f"{name} not found at {dataset_path}")
            if not dataset_path.is_file():
                raise FileNotFoundError(f"{name} is not a file: {dataset_path}")
            if not dataset_path.suffix.lower() == ".csv":
                raise FileNotFoundError(f"{name} is not a CSV file: {dataset_path}")
            logger.info(" %s found: %s", name, dataset_path)

        # Validate output directory
        if not ctx.output_dir.exists():
            ctx.output_dir.mkdir(parents=True, exist_ok=True)
            logger.info("Created output directory: %s", ctx.output_dir)
        elif not ctx.output_dir.is_dir():
            raise FileNotFoundError(f"output_dir exists but is not a directory: {ctx.output_dir}")
        logger.info(" %s found: %s", "output_dir", ctx.output_dir)
        ctx.init_status = ExecutionStatus.SUCCESS
        logger.info("Initialize completed successfully")
        return ctx

    except (FileNotFoundError, OSError) as e:
        logger.error("Initialize failed: %s", e)
        ctx.init_status = ExecutionStatus.FAILED
        ctx.add_error(str(e))
        raise
