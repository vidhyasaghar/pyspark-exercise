"""Tests for sales_data.pipeline.step_02_check_dq."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from sales_data.pipeline.context import ExecutionStatus, PipelineContext
from sales_data.pipeline.step_02_check_dq import run, run_common_checks, run_functional_checks

_PATCH_GET_SPARK = "sales_data.pipeline.step_02_check_dq.spark_session.get_spark_session"
_PATCH_READ_CSV = "sales_data.pipeline.step_02_check_dq.spark_utils.read_csv_with_header"
_PATCH_COMMON = "sales_data.pipeline.step_02_check_dq.run_common_checks"
_PATCH_FUNCTIONAL = "sales_data.pipeline.step_02_check_dq.run_functional_checks"
_PATCH_DQ = "sales_data.pipeline.step_02_check_dq.dq"


def _make_ctx(halt_checks: set | None = None) -> PipelineContext:
    return PipelineContext(
        dataset_one=Path("/fake/one.csv"),
        dataset_two=Path("/fake/two.csv"),
        dataset_three=Path("/fake/three.csv"),
        output_dir=Path("/fake/output"),
        halt_checks=halt_checks if halt_checks is not None else set(),
    )


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


def test_run_success_sets_dq_status() -> None:
    ctx = _make_ctx()

    with (
        patch(_PATCH_GET_SPARK),
        patch(_PATCH_READ_CSV, return_value=MagicMock()),
        patch(_PATCH_COMMON),
        patch(_PATCH_FUNCTIONAL),
    ):
        run(ctx)

    assert ctx.dq_status == ExecutionStatus.SUCCESS


def test_run_runtime_error_sets_failed_and_reraises() -> None:
    ctx = _make_ctx()

    with (
        patch(_PATCH_GET_SPARK),
        patch(_PATCH_READ_CSV, return_value=MagicMock()),
        patch(_PATCH_COMMON, side_effect=RuntimeError("halt")),
        patch(_PATCH_FUNCTIONAL),
        pytest.raises(RuntimeError),
    ):
        run(ctx)

    assert ctx.dq_status == ExecutionStatus.FAILED
    assert ctx.errors == []


def test_run_generic_exception_sets_failed_adds_error_and_reraises() -> None:
    ctx = _make_ctx()

    with (
        patch(_PATCH_GET_SPARK),
        patch(_PATCH_READ_CSV, return_value=MagicMock()),
        patch(_PATCH_COMMON, side_effect=Exception("boom")),
        patch(_PATCH_FUNCTIONAL),
        pytest.raises(Exception, match="boom"),
    ):
        run(ctx)

    assert ctx.dq_status == ExecutionStatus.FAILED
    assert "boom" in ctx.errors


def test_run_runtime_error_from_functional_checks() -> None:
    ctx = _make_ctx()

    with (
        patch(_PATCH_GET_SPARK),
        patch(_PATCH_READ_CSV, return_value=MagicMock()),
        patch(_PATCH_COMMON),
        patch(_PATCH_FUNCTIONAL, side_effect=RuntimeError("functional halt")),
        pytest.raises(RuntimeError),
    ):
        run(ctx)

    assert ctx.dq_status == ExecutionStatus.FAILED


# ---------------------------------------------------------------------------
# run_common_checks
# ---------------------------------------------------------------------------


def test_common_checks_no_halt_checks() -> None:
    ctx = _make_ctx(halt_checks=set())
    df1, df2, df3 = MagicMock(), MagicMock(), MagicMock()

    with patch(_PATCH_DQ) as mock_dq:
        run_common_checks(df1, df2, df3, ctx)

    for c in mock_dq.check_row_count.call_args_list:
        assert c.args[3] is False
    for c in mock_dq.check_col_unique.call_args_list:
        assert c.args[3] is False
    for c in mock_dq.check_col_non_null.call_args_list:
        assert c.args[3] is False
    for c in mock_dq.check_referential_integrity.call_args_list:
        assert c.args[5] is False


def test_common_checks_row_count_halt() -> None:
    ctx = _make_ctx(halt_checks={"row_count"})
    df1, df2, df3 = MagicMock(), MagicMock(), MagicMock()

    with patch(_PATCH_DQ) as mock_dq:
        run_common_checks(df1, df2, df3, ctx)

    for c in mock_dq.check_row_count.call_args_list:
        assert c.args[3] is True
    for c in mock_dq.check_col_unique.call_args_list:
        assert c.args[3] is False
    for c in mock_dq.check_col_non_null.call_args_list:
        assert c.args[3] is False


def test_common_checks_col_unique_halt() -> None:
    ctx = _make_ctx(halt_checks={"col_unique"})
    df1, df2, df3 = MagicMock(), MagicMock(), MagicMock()

    with patch(_PATCH_DQ) as mock_dq:
        run_common_checks(df1, df2, df3, ctx)

    for c in mock_dq.check_col_unique.call_args_list:
        assert c.args[3] is True
    for c in mock_dq.check_row_count.call_args_list:
        assert c.args[3] is False
    for c in mock_dq.check_col_non_null.call_args_list:
        assert c.args[3] is False


def test_common_checks_col_non_null_halt() -> None:
    ctx = _make_ctx(halt_checks={"col_non_null"})
    df1, df2, df3 = MagicMock(), MagicMock(), MagicMock()

    with patch(_PATCH_DQ) as mock_dq:
        run_common_checks(df1, df2, df3, ctx)

    for c in mock_dq.check_col_non_null.call_args_list:
        assert c.args[3] is True
    for c in mock_dq.check_row_count.call_args_list:
        assert c.args[3] is False
    for c in mock_dq.check_col_unique.call_args_list:
        assert c.args[3] is False


def test_common_checks_referential_integrity_halt() -> None:
    ctx = _make_ctx(halt_checks={"referential_integrity"})
    df1, df2, df3 = MagicMock(), MagicMock(), MagicMock()

    with patch(_PATCH_DQ) as mock_dq:
        run_common_checks(df1, df2, df3, ctx)

    assert mock_dq.check_referential_integrity.call_count == 4
    for c in mock_dq.check_referential_integrity.call_args_list:
        assert c.args[5] is True


def test_common_checks_correct_dataset_names_and_row_counts() -> None:
    ctx = _make_ctx()
    df1, df2, df3 = MagicMock(), MagicMock(), MagicMock()

    with patch(_PATCH_DQ) as mock_dq:
        run_common_checks(df1, df2, df3, ctx)

    calls = mock_dq.check_row_count.call_args_list
    assert len(calls) == 3
    # args: (df, expected_count, dataset_name, should_halt)
    name_to_count = {c.args[2]: c.args[1] for c in calls}
    assert name_to_count == {
        "Employee_details": 1000,
        "Employee_calls": 1000,
        "Sales_details": 10000,
    }


def test_common_checks_four_referential_integrity_calls() -> None:
    ctx = _make_ctx()
    df1, df2, df3 = MagicMock(), MagicMock(), MagicMock()

    with patch(_PATCH_DQ) as mock_dq:
        run_common_checks(df1, df2, df3, ctx)

    assert mock_dq.check_referential_integrity.call_count == 4
    # args: (parent_table, parent_keys, child_table, child_keys, dataset_name, should_halt)
    parent_child_pairs = [
        (c.args[0], c.args[2]) for c in mock_dq.check_referential_integrity.call_args_list
    ]
    assert (df1, df2) in parent_child_pairs
    assert (df2, df1) in parent_child_pairs
    assert (df2, df3) in parent_child_pairs
    assert (df1, df3) in parent_child_pairs


# ---------------------------------------------------------------------------
# run_functional_checks
# ---------------------------------------------------------------------------


def test_functional_checks_no_halt_checks() -> None:
    ctx = _make_ctx(halt_checks=set())
    df1, df2 = MagicMock(), MagicMock()

    with patch(_PATCH_DQ) as mock_dq:
        run_functional_checks(df1, df2, ctx)

    # args: (df, dataset_name, should_halt)
    assert mock_dq.check_calls_successful_gt_made.call_args.args[2] is False
    assert mock_dq.check_address_format.call_args.args[2] is False


def test_functional_checks_calls_successful_gt_made_halt() -> None:
    ctx = _make_ctx(halt_checks={"calls_successful_gt_made"})
    df1, df2 = MagicMock(), MagicMock()

    with patch(_PATCH_DQ) as mock_dq:
        run_functional_checks(df1, df2, ctx)

    assert mock_dq.check_calls_successful_gt_made.call_args.args[2] is True
    assert mock_dq.check_address_format.call_args.args[2] is False


def test_functional_checks_address_format_halt() -> None:
    ctx = _make_ctx(halt_checks={"address_format"})
    df1, df2 = MagicMock(), MagicMock()

    with patch(_PATCH_DQ) as mock_dq:
        run_functional_checks(df1, df2, ctx)

    assert mock_dq.check_calls_successful_gt_made.call_args.args[2] is False
    assert mock_dq.check_address_format.call_args.args[2] is True


def test_functional_checks_both_halt() -> None:
    ctx = _make_ctx(halt_checks={"calls_successful_gt_made", "address_format"})
    df1, df2 = MagicMock(), MagicMock()

    with patch(_PATCH_DQ) as mock_dq:
        run_functional_checks(df1, df2, ctx)

    assert mock_dq.check_calls_successful_gt_made.call_args.args[2] is True
    assert mock_dq.check_address_format.call_args.args[2] is True


def test_functional_checks_correct_dataset_labels_and_dataframe_args() -> None:
    ctx = _make_ctx()
    df1, df2 = MagicMock(), MagicMock()

    with patch(_PATCH_DQ) as mock_dq:
        run_functional_checks(df1, df2, ctx)

    calls_args = mock_dq.check_calls_successful_gt_made.call_args.args
    assert calls_args[0] is df1
    assert calls_args[1] == "Employee_calls"

    addr_args = mock_dq.check_address_format.call_args.args
    assert addr_args[0] is df2
    assert addr_args[1] == "Employee_details"
