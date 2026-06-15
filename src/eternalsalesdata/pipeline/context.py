"""Pipeline execution context and status tracking."""

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path


class ExecutionStatus(Enum):
    """Status of a pipeline step execution."""

    PENDING = "pending"
    SUCCESS = "success"
    FAILED = "failed"


@dataclass
# pylint: disable=too-many-instance-attributes
class PipelineContext:
    """Context object passed through the pipeline with datasets and configuration."""

    dataset_one: Path
    dataset_two: Path
    dataset_three: Path
    output_dir: Path
    halt_checks: set[str] = field(default_factory=set)

    # Status tracking
    init_status: ExecutionStatus = field(default=ExecutionStatus.PENDING)
    dq_status: ExecutionStatus = field(default=ExecutionStatus.PENDING)
    report_statuses: dict[str, ExecutionStatus] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    def add_error(self, error: str) -> None:
        """Append an error message to the context error list.

        :param error: Human-readable error description.
        :type error: str
        :rtype: None
        """
        self.errors.append(error)
