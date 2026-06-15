"""Tests for eternalsalesdata.pipeline.step_02_check_dq."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from eternalsalesdata.pipeline.context import ExecutionStatus, PipelineContext
from eternalsalesdata.pipeline.step_02_check_dq import (
    run,
    run_basic_checks,
    run_intermediate_checks,
)

_PATCH_GET_SPARK = "eternalsalesdata.pipeline.step_02_check_dq.spark_session.get_spark_session"
_PATCH_READ_CSV = "eternalsalesdata.pipeline.step_02_check_dq.spark_utils.read_csv_with_header"
_PATCH_BASIC = "eternalsalesdata.pipeline.step_02_check_dq.run_basic_checks"
_PATCH_INTERMEDIATE = "eternalsalesdata.pipeline.step_02_check_dq.run_intermediate_checks"
_PATCH_DQ = "eternalsalesdata.pipeline.step_02_check_dq.dq"


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
        patch(_PATCH_BASIC),
        patch(_PATCH_INTERMEDIATE),
    ):
        run(ctx)

    assert ctx.dq_status == ExecutionStatus.SUCCESS


def test_run_runtime_error_sets_failed_and_reraises() -> None:
    ctx = _make_ctx()

    with (
        patch(_PATCH_GET_SPARK),
        patch(_PATCH_READ_CSV, return_value=MagicMock()),
        patch(_PATCH_BASIC, side_effect=RuntimeError("halt")),
        patch(_PATCH_INTERMEDIATE),
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
        patch(_PATCH_BASIC, side_effect=Exception("boom")),
        patch(_PATCH_INTERMEDIATE),
        pytest.raises(Exception, match="boom"),
    ):
        run(ctx)

    assert ctx.dq_status == ExecutionStatus.FAILED
    assert "boom" in ctx.errors


def test_run_value_error_sets_failed_adds_error_and_reraises() -> None:
    ctx = _make_ctx()

    with (
        patch(_PATCH_GET_SPARK),
        patch(_PATCH_READ_CSV, return_value=MagicMock()),
        patch(_PATCH_BASIC, side_effect=ValueError("bad column")),
        patch(_PATCH_INTERMEDIATE),
        pytest.raises(ValueError, match="bad column"),
    ):
        run(ctx)

    assert ctx.dq_status == ExecutionStatus.FAILED
    assert "bad column" in ctx.errors


def test_run_runtime_error_from_intermediate_checks() -> None:
    ctx = _make_ctx()

    with (
        patch(_PATCH_GET_SPARK),
        patch(_PATCH_READ_CSV, return_value=MagicMock()),
        patch(_PATCH_BASIC),
        patch(_PATCH_INTERMEDIATE, side_effect=RuntimeError("intermediate halt")),
        pytest.raises(RuntimeError),
    ):
        run(ctx)

    assert ctx.dq_status == ExecutionStatus.FAILED


# ---------------------------------------------------------------------------
# run_basic_checks
# ---------------------------------------------------------------------------


def test_basic_checks_no_halt_checks() -> None:
    ctx = _make_ctx(halt_checks=set())
    df1, df2, df3 = MagicMock(), MagicMock(), MagicMock()

    with patch(_PATCH_DQ) as mock_dq:
        run_basic_checks(df1, df2, df3, ctx)

    for c in mock_dq.check_row_count.call_args_list:
        assert c.args[3] is False
    for c in mock_dq.check_col_unique.call_args_list:
        assert c.args[3] is False
    for c in mock_dq.check_col_non_null.call_args_list:
        assert c.args[3] is False
    for c in mock_dq.check_col_non_negative.call_args_list:
        assert c.args[3] is False


def test_basic_checks_row_count_halt() -> None:
    ctx = _make_ctx(halt_checks={"row_count"})
    df1, df2, df3 = MagicMock(), MagicMock(), MagicMock()

    with patch(_PATCH_DQ) as mock_dq:
        run_basic_checks(df1, df2, df3, ctx)

    for c in mock_dq.check_row_count.call_args_list:
        assert c.args[3] is True
    for c in mock_dq.check_col_unique.call_args_list:
        assert c.args[3] is False
    for c in mock_dq.check_col_non_null.call_args_list:
        assert c.args[3] is False
    for c in mock_dq.check_col_non_negative.call_args_list:
        assert c.args[3] is False


def test_basic_checks_col_unique_halt() -> None:
    ctx = _make_ctx(halt_checks={"col_unique"})
    df1, df2, df3 = MagicMock(), MagicMock(), MagicMock()

    with patch(_PATCH_DQ) as mock_dq:
        run_basic_checks(df1, df2, df3, ctx)

    for c in mock_dq.check_col_unique.call_args_list:
        assert c.args[3] is True
    for c in mock_dq.check_row_count.call_args_list:
        assert c.args[3] is False
    for c in mock_dq.check_col_non_null.call_args_list:
        assert c.args[3] is False
    for c in mock_dq.check_col_non_negative.call_args_list:
        assert c.args[3] is False


def test_basic_checks_col_non_null_halt() -> None:
    ctx = _make_ctx(halt_checks={"col_non_null"})
    df1, df2, df3 = MagicMock(), MagicMock(), MagicMock()

    with patch(_PATCH_DQ) as mock_dq:
        run_basic_checks(df1, df2, df3, ctx)

    for c in mock_dq.check_col_non_null.call_args_list:
        assert c.args[3] is True
    for c in mock_dq.check_row_count.call_args_list:
        assert c.args[3] is False
    for c in mock_dq.check_col_unique.call_args_list:
        assert c.args[3] is False
    for c in mock_dq.check_col_non_negative.call_args_list:
        assert c.args[3] is False


def test_basic_checks_col_non_negative_halt() -> None:
    ctx = _make_ctx(halt_checks={"col_non_negative"})
    df1, df2, df3 = MagicMock(), MagicMock(), MagicMock()

    with patch(_PATCH_DQ) as mock_dq:
        run_basic_checks(df1, df2, df3, ctx)

    for c in mock_dq.check_col_non_negative.call_args_list:
        assert c.args[3] is True
    for c in mock_dq.check_row_count.call_args_list:
        assert c.args[3] is False
    for c in mock_dq.check_col_unique.call_args_list:
        assert c.args[3] is False
    for c in mock_dq.check_col_non_null.call_args_list:
        assert c.args[3] is False


def test_basic_checks_correct_dataset_names_and_row_counts() -> None:
    ctx = _make_ctx()
    df1, df2, df3 = MagicMock(), MagicMock(), MagicMock()

    with patch(_PATCH_DQ) as mock_dq:
        run_basic_checks(df1, df2, df3, ctx)

    calls = mock_dq.check_row_count.call_args_list
    assert len(calls) == 3
    # args: (df, expected_count, dataset_name, should_halt)
    df_to_name = {c.args[0]: c.args[2] for c in calls}
    assert df_to_name[df1] == "Employee_calls"
    assert df_to_name[df2] == "Employee_details"
    assert df_to_name[df3] == "Sales_details"

    name_to_count = {c.args[2]: c.args[1] for c in calls}
    assert name_to_count == {
        "Employee_calls": 1000,
        "Employee_details": 1000,
        "Sales_details": 10000,
    }


def test_basic_checks_non_negative_columns_per_dataset() -> None:
    ctx = _make_ctx()
    df1, df2, df3 = MagicMock(), MagicMock(), MagicMock()

    with patch(_PATCH_DQ) as mock_dq:
        run_basic_checks(df1, df2, df3, ctx)

    assert mock_dq.check_col_non_negative.call_count == 3
    # args: (df, dataset_name, columns, halt)
    df_to_cols = {c.args[0]: c.args[2] for c in mock_dq.check_col_non_negative.call_args_list}
    assert df_to_cols[df1] == ["calls_made", "calls_successful"]
    assert df_to_cols[df2] == ["sales_amount"]
    assert df_to_cols[df3] == ["quantity", "age"]


# ---------------------------------------------------------------------------
# run_intermediate_checks
# ---------------------------------------------------------------------------


def test_intermediate_checks_no_halt_checks() -> None:
    ctx = _make_ctx(halt_checks=set())
    df1, df2, df3 = MagicMock(), MagicMock(), MagicMock()

    with patch(_PATCH_DQ) as mock_dq:
        run_intermediate_checks(df1, df2, df3, ctx)

    # args: (df, dataset_name, should_halt)
    assert mock_dq.check_calls_successful_gt_made.call_args.args[2] is False
    assert mock_dq.check_address_format.call_args.args[2] is False
    # args: (parent_table, parent_keys, child_table, child_keys, dataset_name, halt)
    assert mock_dq.check_referential_integrity.call_args.args[5] is False


def test_intermediate_checks_calls_successful_gt_made_halt() -> None:
    ctx = _make_ctx(halt_checks={"calls_successful_gt_made"})
    df1, df2, df3 = MagicMock(), MagicMock(), MagicMock()

    with patch(_PATCH_DQ) as mock_dq:
        run_intermediate_checks(df1, df2, df3, ctx)

    assert mock_dq.check_calls_successful_gt_made.call_args.args[2] is True
    assert mock_dq.check_address_format.call_args.args[2] is False
    assert mock_dq.check_referential_integrity.call_args.args[5] is False


def test_intermediate_checks_address_format_halt() -> None:
    ctx = _make_ctx(halt_checks={"address_format"})
    df1, df2, df3 = MagicMock(), MagicMock(), MagicMock()

    with patch(_PATCH_DQ) as mock_dq:
        run_intermediate_checks(df1, df2, df3, ctx)

    assert mock_dq.check_calls_successful_gt_made.call_args.args[2] is False
    assert mock_dq.check_address_format.call_args.args[2] is True
    assert mock_dq.check_referential_integrity.call_args.args[5] is False


def test_intermediate_checks_referential_integrity_halt() -> None:
    ctx = _make_ctx(halt_checks={"referential_integrity"})
    df1, df2, df3 = MagicMock(), MagicMock(), MagicMock()

    with patch(_PATCH_DQ) as mock_dq:
        run_intermediate_checks(df1, df2, df3, ctx)

    assert mock_dq.check_referential_integrity.call_count == 1
    assert mock_dq.check_referential_integrity.call_args.args[5] is True
    assert mock_dq.check_calls_successful_gt_made.call_args.args[2] is False
    assert mock_dq.check_address_format.call_args.args[2] is False


def test_intermediate_checks_all_halt() -> None:
    ctx = _make_ctx(
        halt_checks={"calls_successful_gt_made", "address_format", "referential_integrity"}
    )
    df1, df2, df3 = MagicMock(), MagicMock(), MagicMock()

    with patch(_PATCH_DQ) as mock_dq:
        run_intermediate_checks(df1, df2, df3, ctx)

    assert mock_dq.check_calls_successful_gt_made.call_args.args[2] is True
    assert mock_dq.check_address_format.call_args.args[2] is True
    assert mock_dq.check_referential_integrity.call_args.args[5] is True


def test_intermediate_checks_correct_dataset_labels_and_dataframe_args() -> None:
    ctx = _make_ctx()
    df1, df2, df3 = MagicMock(), MagicMock(), MagicMock()

    with patch(_PATCH_DQ) as mock_dq:
        run_intermediate_checks(df1, df2, df3, ctx)

    calls_args = mock_dq.check_calls_successful_gt_made.call_args.args
    assert calls_args[0] is df1
    assert calls_args[1] == "Employee_calls"

    addr_args = mock_dq.check_address_format.call_args.args
    assert addr_args[0] is df2
    assert addr_args[1] == "Employee_details"


def test_intermediate_checks_referential_integrity_caller_id_against_dataset_one() -> None:
    ctx = _make_ctx()
    df1, df2, df3 = MagicMock(), MagicMock(), MagicMock()

    with patch(_PATCH_DQ) as mock_dq:
        run_intermediate_checks(df1, df2, df3, ctx)

    assert mock_dq.check_referential_integrity.call_count == 1
    call = mock_dq.check_referential_integrity.call_args
    # args: (parent_table, parent_keys, child_table, child_keys, dataset_name, halt)
    assert call.args[0] is df1  # parent = Employee_calls
    assert call.args[1] == ["id"]  # parent key
    assert call.args[2] is df3  # child = Sales_details
    assert call.args[3] == ["caller_id"]  # child key
    assert call.args[4] == "Sales_details"
