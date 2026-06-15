# `pipeline` — orchestration and steps

This package wires the pipeline together. An `Orchestrator` runs three steps in order,
passing a shared `PipelineContext` through each one.

## Files

| File                          | Role |
|-------------------------------|------|
| `orchestrator.py`             | `Orchestrator` — builds the context from CLI args and runs the three steps, aborting early if a step raises. Logs a final summary. |
| `context.py`                  | `PipelineContext` dataclass and the `ExecutionStatus` enum. Carries config and accumulates status/errors. |
| `step_01_initialize.py`       | Validates the three input CSVs exist and prepares the output directory. |
| `step_02_check_dq.py`         | Reads the datasets and runs basic + intermediate data-quality checks. |
| `step_03_generate_report.py`  | Runs every registered transform and writes each result to a CSV folder. |

## The context object

Every step takes a `PipelineContext` and returns the (mutated) context:

```python
@dataclass
class PipelineContext:
    dataset_one: Path
    dataset_two: Path
    dataset_three: Path
    output_dir: Path
    halt_checks: set[str]                       # which DQ checks should halt the run
    init_status: ExecutionStatus                # PENDING / SUCCESS / FAILED
    dq_status: ExecutionStatus
    report_statuses: dict[str, ExecutionStatus] # one entry per report folder
    errors: list[str]                           # human-readable error messages
```

`ExecutionStatus` is `PENDING`, `SUCCESS`, or `FAILED`.

## Execution model

`Orchestrator.run()` runs the steps in sequence and stops early on a fatal error:

1. **initialize** — if it raises `FileNotFoundError` (a missing/invalid input), the run aborts.
2. **dq_check** — if a *halt-enabled* check fails it raises `RuntimeError` and the run aborts.
   Non-halting failures are logged as warnings and the pipeline continues.
3. **generate_report** — per-report failures are recorded in `report_statuses` and do not
   stop the other reports; a fatal Spark/read error aborts the step.

After the steps, `_summarise()` logs the status of each step and lists any accumulated
errors. `main()` then returns exit code `1` if `ctx.errors` is non-empty, else `0`.

## Step contract

Each step module exposes a single function:

```python
def run(ctx: PipelineContext) -> PipelineContext: ...
```

It updates the relevant status field(s) on `ctx`, appends to `ctx.errors` on failure,
and returns the context. Steps obtain Spark and I/O helpers from
[`eternalsalesdata.utils`](../utils/README.md).

## Report transforms

`step_03_generate_report.py` uses a small registry. A transformation is a function
`(df1, df2, df3) -> DataFrame` decorated with `@transform("<output_folder>")`:

```python
@transform("it_data")
def transform_it_data(df1, df2, df3) -> DataFrame:
    ...
```

The decorator records the function and its output-folder name in the module-level
`TRANSFORMS` dict. `run()` iterates that registry, executes each transform against the
three datasets, and writes the result with `write_df_to_csv` (coalesced to a single CSV).

To add a report, write a new transform function and decorate it with the desired output
folder name — `run()` picks it up automatically.

| Transform function                              | Output folder | Description |
|-------------------------------------------------|---------------|-------------|
| `transform_it_data`                             | `it_data` | Top 100 IT employees by sales amount |
| `transform_marketing_address_info`              | `marketing_address_info` | Marketing name, address, and zip code |
| `transform_department_breakdown`                | `department_breakdown` | Sales total and call success rate per department |
| `transform_best_performer_per_department`       | `top_3` | Top 3 performers per department (success rate > 75%) |
| `transform_top_3_sold_products_nl`              | `top_3_most_sold_per_department_netherlands` | Top 3 products per department in the Netherlands |
| `transform_best_salesperson_per_country`        | `best_salesperson` | Best salesperson per country by quantity sold |

## Ranking rationale (Outputs #4 and #6)

The exercise asks, for the bonus report (`top_3`) and to indentify *who should get the bonus and why?*
and leaves the "best salesperson" ranking (`best_salesperson`) open. The two reports
use fundamentally **different metrics**:

- `caller_id` in `dataset_three` (Sales_details) spans only **100 distinct employees
  (ids 1–100)** out of the 1,000 in the other datasets. So any metric derived from
  `dataset_three` — `quantity`, `product_sold` — is missing for ~900 employees.
- `sales_amount` lives in `dataset_two` (Employee_details) and is populated for **all
  1,000 employees**.

**`top_3` (Output #4) ranks by `sales_amount` first.** After filtering on call success
rate > 75% (per the brief), `sales_amount` is the deciding metric. It is the only
revenue measure with full employee coverage, and revenue — not units sold — is what
"best deserves the bonus" means. Ranking by quantity here would cut-off 90% of staff
and reward cheap-but-high-volume sellers over high earners.

**`best_salesperson` (Output #6) ranks by `total_quantity` first**, with `sales_amount`
only as a tiebreaker. In dataset_three you've got 100 people each working all 3 countries,
so "best per country" only makes sense if you score them on something that's recorded per country.
Quantity is; sales_amount (a single lifetime total per person) isn't.

In short: **measure by sold amount where it is attributable (employee-level, `top_3`);
fall back to quantity where amount is not attributable (country-level,
`best_salesperson`).**
