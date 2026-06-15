# `dq_checks` — data-quality checks

Stateless, reusable validation functions run by the DQ step
([`step_02_check_dq.py`](../pipeline/step_02_check_dq.py)). Each function inspects a
DataFrame, logs the outcome, and reports failures.

## Common contract

Every check follows the same convention:

- It **logs a warning** for each issue found and returns `False` on failure / `True` on pass.
- It accepts `halt_on_failure: bool = False`. When `True`, a failure raises
  `RuntimeError` instead of just warning — this is how `--halt`/`--halt-all` stops the run.
- It accepts a `dataset_name` label used in log messages.
- Checks that reference specific columns raise `ValueError` if a required column is missing.

This "warn by default, halt on request" design lets a run surface every data problem in
one pass unless the caller explicitly opts into hard failures.

## Modules

### `dq_basic.py` — structural checks

| Function | Checks |
|----------|--------|
| `check_row_count(df, expected, dataset_name, halt_on_failure=False)` | Row count equals `expected`. |
| `check_col_non_null(df, dataset_name, column_names, halt_on_failure=False)` | Given columns contain no nulls. |
| `check_col_unique(df, dataset_name, column_names, halt_on_failure=False)` | Given columns have no duplicates. |
| `check_col_non_negative(df, dataset_name, columns, halt_on_failure=False)` | Given numeric columns have no negative values. |

### `dq_intermediate.py` — business-rule checks

| Function | Checks |
|----------|--------|
| `check_referential_integrity(parent_table, parent_keys, child_table, child_keys, dataset_name, halt_on_failure=False)` | Every child-table row has a matching parent row on the join keys (no orphans). |
| `check_calls_successful_gt_made(df, dataset_name, halt_on_failure=False)` | `calls_successful` is never greater than `calls_made`. |
| `check_address_format(df, dataset_name, halt_on_failure=False)` | Addresses match `[street], [number], [DDDD XX]` (4 digits, space, 2 capitals). |

## Public API

All seven checks are re-exported from the package, so import from `dq_checks` directly:

```python
import eternalsalesdata.dq_checks as dq

dq.check_row_count(df, 1000, "Employee_calls", halt_on_failure=False)
dq.check_referential_integrity(df1, ["id"], df3, ["caller_id"], "Sales_details")
```

## How the DQ step applies them

`step_02_check_dq.py` reads the three datasets and runs:

- **Basic checks** on all datasets: row counts (1000 / 1000 / 10000), `id` uniqueness,
  `id` non-null, and non-negative numeric columns (calls, `sales_amount`, `quantity`, `age`).
- **Intermediate checks**: `calls_successful <= calls_made` on `Employee_calls`, address
  format on `Employee_details`, and referential integrity of `Sales_details.caller_id`
  against `Employee_calls.id`.

Each check's halt behaviour is driven by whether its name is in `ctx.halt_checks` (set
from the `--halt`/`--halt-all` CLI flags). Check names: `row_count`, `col_unique`,
`col_non_null`, `col_non_negative`, `referential_integrity`, `calls_successful_gt_made`,
`address_format`.
