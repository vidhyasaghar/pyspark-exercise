# `eternalsalesdata` — source overview

This is the Python package for the EternalSalesData pipeline. It is installed as
`eternalsalesdata` and exposes the `sales-data` console command.

## Module map

| Path             | Responsibility |
|------------------|----------------|
| `main.py`        | CLI entry point — parses arguments, builds the halt-check set, runs the orchestrator, returns the process exit code. |
| `pipeline/`      | The pipeline itself: orchestrator, shared context object, and the three ordered steps (initialize → DQ checks → report generation). See [pipeline/README](pipeline/README.md). |
| `dq_checks/`     | Stateless data-quality check functions used by the DQ step. See [dq_checks/README](dq_checks/README.md). |
| `utils/`         | Cross-cutting helpers: Spark session factory, CSV read/write, and logging. See [utils/README](utils/README.md). |

## How a run flows

```
main.main()
  └─ Orchestrator(args).run()          # pipeline/orchestrator.py
       ├─ step_01_initialize.run(ctx)  # validate inputs, prepare output dir
       ├─ step_02_check_dq.run(ctx)    # basic + intermediate DQ checks
       └─ step_03_generate_report.run(ctx)  # run transforms, write CSVs
```

A single [`PipelineContext`](pipeline/context.py) dataclass is threaded through every
step. It carries the dataset paths, the output directory, the set of checks to halt on,
and accumulates per-step `ExecutionStatus` values plus any error messages.

## Entry point

`main()` is registered as a console script in [pyproject.toml](../../pyproject.toml):

```toml
[project.scripts]
sales-data = "eternalsalesdata.main:main"
```

So after installation, `sales-data ...` and `python -m eternalsalesdata.main ...` are
equivalent.

## Applied Principles

These apply throughout `src/`:

- **Logging, not printing.** Never use `print()`. Obtain a logger with
  `get_logger(__name__)` from `eternalsalesdata.utils`.
- **Docstrings** use reStructuredText (`:param:`, `:type:`, `:returns:`, `:rtype:`,
  `:raises:`).
- **DataFrame comparisons in tests** use `chispa.dataframe_comparer.assert_df_equality`,
  never `==`.
