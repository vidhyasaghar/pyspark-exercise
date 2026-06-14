"""Tests for eternalsalesdata.pipeline.step_01_initialize."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from eternalsalesdata.pipeline.context import ExecutionStatus, PipelineContext
from eternalsalesdata.pipeline.step_01_initialize import run


def _csv(tmp_path: Path, name: str) -> Path:
    p = tmp_path / name
    p.write_text("id\n1\n")
    return p


def _valid_ctx(tmp_path: Path) -> PipelineContext:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    return PipelineContext(
        dataset_one=_csv(tmp_path, "one.csv"),
        dataset_two=_csv(tmp_path, "two.csv"),
        dataset_three=_csv(tmp_path, "three.csv"),
        output_dir=output_dir,
    )


# ---------------------------------------------------------------------------
# Happy-path cases
# ---------------------------------------------------------------------------


def test_run_success_all_datasets_valid_output_dir_exists(tmp_path: Path) -> None:
    ctx = _valid_ctx(tmp_path)

    run(ctx)

    assert ctx.init_status == ExecutionStatus.SUCCESS
    assert ctx.errors == []


def test_run_success_creates_output_dir_when_missing(tmp_path: Path) -> None:
    output_dir = tmp_path / "output"
    ctx = PipelineContext(
        dataset_one=_csv(tmp_path, "one.csv"),
        dataset_two=_csv(tmp_path, "two.csv"),
        dataset_three=_csv(tmp_path, "three.csv"),
        output_dir=output_dir,
    )
    assert not output_dir.exists()

    run(ctx)

    assert output_dir.exists()
    assert output_dir.is_dir()
    assert ctx.init_status == ExecutionStatus.SUCCESS


# ---------------------------------------------------------------------------
# Dataset validation — missing paths
# ---------------------------------------------------------------------------


def test_run_fails_when_dataset_one_missing(tmp_path: Path) -> None:
    ctx = PipelineContext(
        dataset_one=tmp_path / "missing.csv",
        dataset_two=_csv(tmp_path, "two.csv"),
        dataset_three=_csv(tmp_path, "three.csv"),
        output_dir=tmp_path / "output",
    )

    with pytest.raises(FileNotFoundError):
        run(ctx)

    assert ctx.init_status == ExecutionStatus.FAILED
    assert any("dataset_one" in e for e in ctx.errors)


def test_run_fails_when_dataset_two_missing(tmp_path: Path) -> None:
    ctx = PipelineContext(
        dataset_one=_csv(tmp_path, "one.csv"),
        dataset_two=tmp_path / "missing.csv",
        dataset_three=_csv(tmp_path, "three.csv"),
        output_dir=tmp_path / "output",
    )

    with pytest.raises(FileNotFoundError):
        run(ctx)

    assert ctx.init_status == ExecutionStatus.FAILED
    assert any("dataset_two" in e for e in ctx.errors)


def test_run_fails_when_dataset_three_missing(tmp_path: Path) -> None:
    ctx = PipelineContext(
        dataset_one=_csv(tmp_path, "one.csv"),
        dataset_two=_csv(tmp_path, "two.csv"),
        dataset_three=tmp_path / "missing.csv",
        output_dir=tmp_path / "output",
    )

    with pytest.raises(FileNotFoundError):
        run(ctx)

    assert ctx.init_status == ExecutionStatus.FAILED
    assert any("dataset_three" in e for e in ctx.errors)


# ---------------------------------------------------------------------------
# Dataset validation — wrong type or extension
# ---------------------------------------------------------------------------


def test_run_fails_when_dataset_is_a_directory(tmp_path: Path) -> None:
    ds1_as_dir = tmp_path / "one.csv"
    ds1_as_dir.mkdir()
    ctx = PipelineContext(
        dataset_one=ds1_as_dir,
        dataset_two=_csv(tmp_path, "two.csv"),
        dataset_three=_csv(tmp_path, "three.csv"),
        output_dir=tmp_path / "output",
    )

    with pytest.raises(FileNotFoundError):
        run(ctx)

    assert ctx.init_status == ExecutionStatus.FAILED
    assert len(ctx.errors) > 0


def test_run_fails_when_dataset_has_wrong_extension(tmp_path: Path) -> None:
    ds1_txt = tmp_path / "one.txt"
    ds1_txt.write_text("id\n1\n")
    ctx = PipelineContext(
        dataset_one=ds1_txt,
        dataset_two=_csv(tmp_path, "two.csv"),
        dataset_three=_csv(tmp_path, "three.csv"),
        output_dir=tmp_path / "output",
    )

    with pytest.raises(FileNotFoundError):
        run(ctx)

    assert ctx.init_status == ExecutionStatus.FAILED
    assert len(ctx.errors) > 0


def test_run_accepts_csv_extension_case_insensitive(tmp_path: Path) -> None:
    ds1 = tmp_path / "one.CSV"
    ds1.write_text("id\n1\n")
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    ctx = PipelineContext(
        dataset_one=ds1,
        dataset_two=_csv(tmp_path, "two.csv"),
        dataset_three=_csv(tmp_path, "three.csv"),
        output_dir=output_dir,
    )

    run(ctx)

    assert ctx.init_status == ExecutionStatus.SUCCESS


# ---------------------------------------------------------------------------
# Output directory validation
# ---------------------------------------------------------------------------


def test_run_fails_when_output_dir_is_a_file(tmp_path: Path) -> None:
    output_as_file = tmp_path / "output"
    output_as_file.write_text("not a directory")
    ctx = PipelineContext(
        dataset_one=_csv(tmp_path, "one.csv"),
        dataset_two=_csv(tmp_path, "two.csv"),
        dataset_three=_csv(tmp_path, "three.csv"),
        output_dir=output_as_file,
    )

    with pytest.raises(FileNotFoundError):
        run(ctx)

    assert ctx.init_status == ExecutionStatus.FAILED
    assert len(ctx.errors) > 0


def test_run_oserror_on_mkdir_sets_failed_and_reraises(tmp_path: Path) -> None:
    mock_output_dir = MagicMock(spec=Path)
    mock_output_dir.exists.return_value = False
    mock_output_dir.mkdir.side_effect = OSError("permission denied")

    ctx = PipelineContext(
        dataset_one=_csv(tmp_path, "one.csv"),
        dataset_two=_csv(tmp_path, "two.csv"),
        dataset_three=_csv(tmp_path, "three.csv"),
        output_dir=mock_output_dir,  # type: ignore[arg-type]
    )

    with pytest.raises(OSError, match="permission denied"):
        run(ctx)

    assert ctx.init_status == ExecutionStatus.FAILED
    assert any("permission denied" in e for e in ctx.errors)


# ---------------------------------------------------------------------------
# Error-handling contract
# ---------------------------------------------------------------------------


def test_run_failure_reraises_and_does_not_swallow(tmp_path: Path) -> None:
    ctx = PipelineContext(
        dataset_one=tmp_path / "missing.csv",
        dataset_two=_csv(tmp_path, "two.csv"),
        dataset_three=_csv(tmp_path, "three.csv"),
        output_dir=tmp_path / "output",
    )

    with pytest.raises(FileNotFoundError):
        run(ctx)
