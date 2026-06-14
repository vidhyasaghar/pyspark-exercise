"""Tests for sales_data.pipeline.context."""

from pathlib import Path

from eternalsalesData.pipeline.context import ExecutionStatus, PipelineContext


def _make_ctx() -> PipelineContext:
    return PipelineContext(
        dataset_one=Path("/fake/one.csv"),
        dataset_two=Path("/fake/two.csv"),
        dataset_three=Path("/fake/three.csv"),
        output_dir=Path("/fake/output"),
    )


# ---------------------------------------------------------------------------
# PipelineContext.add_error
# ---------------------------------------------------------------------------


def test_add_error_appends_message_to_errors() -> None:
    ctx = _make_ctx()

    ctx.add_error("something went wrong")

    assert ctx.errors == ["something went wrong"]


def test_add_error_multiple_calls_accumulate_in_order() -> None:
    ctx = _make_ctx()

    ctx.add_error("first")
    ctx.add_error("second")
    ctx.add_error("third")

    assert ctx.errors == ["first", "second", "third"]


# ---------------------------------------------------------------------------
# PipelineContext default values
# ---------------------------------------------------------------------------


def test_default_statuses_are_pending() -> None:
    ctx = _make_ctx()

    assert ctx.init_status == ExecutionStatus.PENDING
    assert ctx.dq_status == ExecutionStatus.PENDING


def test_default_errors_and_halt_checks_are_empty() -> None:
    ctx = _make_ctx()

    assert ctx.errors == []
    assert ctx.halt_checks == set()


def test_mutable_defaults_not_shared_between_instances() -> None:
    ctx1 = _make_ctx()
    ctx2 = _make_ctx()

    ctx1.add_error("ctx1 error")
    ctx1.halt_checks.add("row_count")

    assert ctx2.errors == []
    assert ctx2.halt_checks == set()
