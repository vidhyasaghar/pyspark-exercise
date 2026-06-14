"""Tests for sales_data.main."""

from unittest.mock import MagicMock, patch

from sales_data.main import _DQ_CHECKS, main

_PATCH_ORCHESTRATOR = "sales_data.main.Orchestrator"
_BASE_ARGV = ["sales-data", "/fake/ds1.csv", "/fake/ds2.csv", "/fake/ds3.csv", "/fake/out"]


def _mock_ctx(errors: list | None = None) -> MagicMock:
    ctx = MagicMock()
    ctx.errors = errors if errors is not None else []
    return ctx


# ---------------------------------------------------------------------------
# Return code behaviour
# ---------------------------------------------------------------------------


def test_main_returns_zero_when_pipeline_has_no_errors() -> None:
    with (
        patch("sys.argv", _BASE_ARGV),
        patch(_PATCH_ORCHESTRATOR) as mock_orch_class,
    ):
        mock_orch_class.return_value.run.return_value = _mock_ctx()
        result = main()

    assert result == 0


def test_main_returns_one_when_ctx_has_errors() -> None:
    with (
        patch("sys.argv", _BASE_ARGV),
        patch(_PATCH_ORCHESTRATOR) as mock_orch_class,
    ):
        mock_orch_class.return_value.run.return_value = _mock_ctx(errors=["something failed"])
        result = main()

    assert result == 1


def test_main_returns_one_when_orchestrator_raises() -> None:
    with (
        patch("sys.argv", _BASE_ARGV),
        patch(_PATCH_ORCHESTRATOR, side_effect=Exception("unexpected failure")),
    ):
        result = main()

    assert result == 1


# ---------------------------------------------------------------------------
# halt_checks computation
# ---------------------------------------------------------------------------


def test_main_no_halt_flags_produces_empty_halt_checks() -> None:
    with (
        patch("sys.argv", _BASE_ARGV),
        patch(_PATCH_ORCHESTRATOR) as mock_orch_class,
    ):
        mock_orch_class.return_value.run.return_value = _mock_ctx()
        main()

    args = mock_orch_class.call_args.args[0]
    assert args.halt_checks == set()


def test_main_halt_all_sets_all_six_dq_checks() -> None:
    with (
        patch("sys.argv", _BASE_ARGV + ["--halt-all"]),
        patch(_PATCH_ORCHESTRATOR) as mock_orch_class,
    ):
        mock_orch_class.return_value.run.return_value = _mock_ctx()
        main()

    args = mock_orch_class.call_args.args[0]
    assert args.halt_checks == _DQ_CHECKS


def test_main_halt_parses_comma_separated_check_names() -> None:
    with (
        patch("sys.argv", _BASE_ARGV + ["--halt", "row_count,col_unique"]),
        patch(_PATCH_ORCHESTRATOR) as mock_orch_class,
    ):
        mock_orch_class.return_value.run.return_value = _mock_ctx()
        main()

    args = mock_orch_class.call_args.args[0]
    assert args.halt_checks == {"row_count", "col_unique"}


def test_main_halt_trims_whitespace_around_check_names() -> None:
    with (
        patch("sys.argv", _BASE_ARGV + ["--halt", " row_count , col_unique "]),
        patch(_PATCH_ORCHESTRATOR) as mock_orch_class,
    ):
        mock_orch_class.return_value.run.return_value = _mock_ctx()
        main()

    args = mock_orch_class.call_args.args[0]
    assert args.halt_checks == {"row_count", "col_unique"}


def test_main_halt_filters_empty_comma_segments() -> None:
    with (
        patch("sys.argv", _BASE_ARGV + ["--halt", "row_count,,col_unique"]),
        patch(_PATCH_ORCHESTRATOR) as mock_orch_class,
    ):
        mock_orch_class.return_value.run.return_value = _mock_ctx()
        main()

    args = mock_orch_class.call_args.args[0]
    assert args.halt_checks == {"row_count", "col_unique"}
    assert "" not in args.halt_checks


def test_main_halt_all_and_halt_together_accumulate() -> None:
    with (
        patch("sys.argv", _BASE_ARGV + ["--halt-all", "--halt", "custom_check"]),
        patch(_PATCH_ORCHESTRATOR) as mock_orch_class,
    ):
        mock_orch_class.return_value.run.return_value = _mock_ctx()
        main()

    args = mock_orch_class.call_args.args[0]
    assert _DQ_CHECKS.issubset(args.halt_checks)
    assert "custom_check" in args.halt_checks
