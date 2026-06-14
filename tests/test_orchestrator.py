"""Tests for sales_data.pipeline.orchestrator."""

import argparse
from unittest.mock import MagicMock, patch

import pytest

from sales_data.pipeline.orchestrator import Orchestrator

_PATCH_INIT = "sales_data.pipeline.orchestrator.initialize.run"
_PATCH_DQ = "sales_data.pipeline.orchestrator.dq_check.run"
_PATCH_REPORT = "sales_data.pipeline.orchestrator.generate_report.run"


def _make_orchestrator() -> Orchestrator:
    args = argparse.Namespace(
        dataset1="/fake/one.csv",
        dataset2="/fake/two.csv",
        dataset3="/fake/three.csv",
        output_dir="/fake/output",
        halt_checks=set(),
    )
    return Orchestrator(args)


# ---------------------------------------------------------------------------
# Orchestrator.__init__
# ---------------------------------------------------------------------------


def test_init_resolves_paths_and_populates_context() -> None:
    args = argparse.Namespace(
        dataset1="data/one.csv",
        dataset2="data/two.csv",
        dataset3="data/three.csv",
        output_dir="output",
        halt_checks={"row_count"},
    )

    orch = Orchestrator(args)
    ctx = orch._ctx

    assert ctx.dataset_one.is_absolute()
    assert ctx.dataset_two.is_absolute()
    assert ctx.dataset_three.is_absolute()
    assert ctx.output_dir.is_absolute()
    assert ctx.halt_checks == {"row_count"}


# ---------------------------------------------------------------------------
# Orchestrator.run
# ---------------------------------------------------------------------------


def test_run_all_steps_succeed_returns_ctx_and_calls_summarise() -> None:
    orch = _make_orchestrator()
    original_ctx = orch._ctx
    mock_summarise = MagicMock()
    orch._summarise = mock_summarise  # type: ignore[method-assign]

    with (
        patch(_PATCH_INIT, return_value=original_ctx),
        patch(_PATCH_DQ, return_value=original_ctx),
        patch(_PATCH_REPORT, return_value=original_ctx),
    ):
        result = orch.run()

    mock_summarise.assert_called_once()
    assert result is original_ctx


def test_run_aborts_after_initialize_file_not_found() -> None:
    orch = _make_orchestrator()
    mock_dq = MagicMock()
    mock_report = MagicMock()

    with (
        patch(_PATCH_INIT, side_effect=FileNotFoundError("missing")),
        patch(_PATCH_DQ, new=mock_dq),
        patch(_PATCH_REPORT, new=mock_report),
    ):
        result = orch.run()

    mock_dq.assert_not_called()
    mock_report.assert_not_called()
    assert result is orch._ctx


def test_run_aborts_after_dq_runtime_error() -> None:
    orch = _make_orchestrator()
    original_ctx = orch._ctx
    mock_report = MagicMock()

    with (
        patch(_PATCH_INIT, return_value=original_ctx),
        patch(_PATCH_DQ, side_effect=RuntimeError("dq halt")),
        patch(_PATCH_REPORT, new=mock_report),
    ):
        result = orch.run()

    mock_report.assert_not_called()
    assert result is orch._ctx


def test_run_aborts_after_report_runtime_error() -> None:
    orch = _make_orchestrator()
    original_ctx = orch._ctx
    mock_summarise = MagicMock()
    orch._summarise = mock_summarise  # type: ignore[method-assign]

    with (
        patch(_PATCH_INIT, return_value=original_ctx),
        patch(_PATCH_DQ, return_value=original_ctx),
        patch(_PATCH_REPORT, side_effect=RuntimeError("report failed")),
    ):
        result = orch.run()

    mock_summarise.assert_not_called()
    assert result is orch._ctx


def test_run_oserror_from_initialize_propagates_uncaught() -> None:
    orch = _make_orchestrator()
    mock_dq = MagicMock()
    mock_report = MagicMock()

    with (
        patch(_PATCH_INIT, side_effect=OSError("permission denied")),
        patch(_PATCH_DQ, new=mock_dq),
        patch(_PATCH_REPORT, new=mock_report),
        pytest.raises(OSError, match="permission denied"),
    ):
        orch.run()

    mock_dq.assert_not_called()
    mock_report.assert_not_called()
