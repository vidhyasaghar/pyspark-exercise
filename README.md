# EternalSalesData

A PySpark batch pipeline that validates and analyses **EternalTeleSales** call-centre
and sales data. It runs a set of data-quality (DQ) checks across three input datasets
and then generates six business reports as CSV files.

- **Package:** `eternalsalesdata`
- **CLI command:** `sales-data`
- **Python:** 3.10+
- **Engine:** Apache Spark 3.5.0 (via PySpark)

---

## What it does

The pipeline runs three steps in order:

1. **Initialize** — validate that the three input CSVs exist and prepare the output directory.
2. **Data-quality checks** — run basic and intermediate DQ checks on every dataset. By
   default failures are logged as warnings; with `--halt`/`--halt-all` a failure stops the run.
3. **Generate reports** — run six transforms and write each result to its own CSV folder.

### Input datasets

| Argument   | File                | Logical name       | Columns |
|------------|---------------------|--------------------|---------|
| `dataset1` | `dataset_one.csv`   | `Employee_calls`   | `id, area, calls_made, calls_successful` |
| `dataset2` | `dataset_two.csv`   | `Employee_details` | `id, name, address, sales_amount` |
| `dataset3` | `dataset_three.csv` | `Sales_details`    | `id, caller_id, company, recipient, age, country, product_sold, quantity` |

Sample datasets are provided in [data/](data/).

### Generated reports

Each report is written to a subfolder of the output directory:

| Output folder                                  | Contents |
|------------------------------------------------|----------|
| `it_data`                                      | Top 100 IT-department employees by sales amount |
| `marketing_address_info`                       | Marketing employees with address split from zip code |
| `department_breakdown`                         | Total sales and call success rate per department |
| `top_3`                                         | Top 3 performers per department (success rate > 75%) |
| `top_3_most_sold_per_department_netherlands`   | Top 3 most-sold products per department in the Netherlands |
| `best_salesperson`                             | Best salesperson per country by quantity sold |

---

## Installation

There are two ways to get the code: install a **published release** (recommended for
running the pipeline) or clone the **repository** (recommended for development).

### Option A — Install a published release (any OS)

Each GitHub release ships a built wheel (`.whl`) and source archive (`.tar.gz`).

1. Go to the [Releases page](https://github.com/vidhyasaghar/pyspark-exercise/releases) and download the latest
   `eternalsalesdata-<version>-py3-none-any.whl`.
2. Install it into a fresh virtual environment:

   ```bash
   # macOS / Linux
   python3 -m venv .venv
   source .venv/bin/activate
   pip install eternalsalesdata-<version>-py3-none-any.whl
   ```

   ```powershell
   # Windows (PowerShell)
   py -m venv .venv
   .venv\Scripts\Activate.ps1
   pip install eternalsalesdata-<version>-py3-none-any.whl
   ```

You can also install directly from the release URL without downloading first:

```bash
pip install https://github.com/vidhyasaghar/pyspark-exercise/releases/download/v<version>/eternalsalesdata-<version>-py3-none-any.whl
```

> **Java is required.** PySpark needs a Java runtime (JDK 8/11/17). Install a JDK
> (e.g. Temurin 17) and make sure `java -version` works before running the pipeline.

### Option B — Clone the repository (development)

This project uses [uv](https://docs.astral.sh/uv/) for dependency management.

```bash
git clone https://github.com/vidhyasaghar/pyspark-exercise.git
cd pyspark-exercise
uv sync --extra dev
```

`uv sync --extra dev` installs the runtime dependency (PySpark) plus the dev tools
(pytest, chispa, black, pylint, mypy) into `.venv`.

---

## Usage

The CLI takes the three dataset paths and an output directory:

```
sales-data <dataset1> <dataset2> <dataset3> <output_dir> [--halt-all] [--halt CHECKS]
```

### Run from an installed release

With the wheel installed and the virtual environment active, the `sales-data` command
is on your PATH:

```bash
sales-data data/dataset_one.csv data/dataset_two.csv data/dataset_three.csv output/
```

### Run from a clone

```bash
uv run sales-data data/dataset_one.csv data/dataset_two.csv data/dataset_three.csv output/
```

### Halting on data-quality failures

By default, DQ failures are logged as warnings and the pipeline continues. To make
specific checks stop the run:

```bash
# Halt on every DQ check
sales-data data/dataset_one.csv data/dataset_two.csv data/dataset_three.csv output/ --halt-all

# Halt on selected checks only (comma-separated)
sales-data data/dataset_one.csv data/dataset_two.csv data/dataset_three.csv output/ \
  --halt row_count,referential_integrity
```

Available check names for `--halt`: `row_count`, `col_unique`, `col_non_null`,
`col_non_negative`, `referential_integrity`, `calls_successful_gt_made`, `address_format`.

The command exits `0` on success and `1` if any step failed or errors were recorded.

### Output and logs

- **Reports** are written under the output directory, one folder per report (see table above).
- **Logs** are printed to the console and written to `logs/eternalsalesdata.log`
  (rotating, 5 MB × 3 backups).

---

## Project layout

```
src/eternalsalesdata/
├── main.py            # CLI entry point (the `sales-data` command)
├── pipeline/          # Orchestrator, context, and the three pipeline steps
├── dq_checks/         # Reusable data-quality check functions
└── utils/             # Spark session, CSV I/O, and logging helpers
```

Each package has its own README with details:

- [src/eternalsalesdata/](src/eternalsalesdata/README.md) — source overview
- [pipeline/](src/eternalsalesdata/pipeline/README.md) — orchestration and steps
- [dq_checks/](src/eternalsalesdata/dq_checks/README.md) — data-quality checks
- [utils/](src/eternalsalesdata/utils/README.md) — shared utilities

---

## Development

Install dev dependencies and run the code quality checks locally (these are the same checks CI runs):

```bash
uv sync --extra dev

uv run black --check src tests      # formatting (line length 100)
uv run pylint src/eternalsalesdata  # linting
uv run mypy src/eternalsalesdata    # type checking
uv run pytest                       # tests (pytest + chispa)
```

Tests live in [tests/](tests/) and compare DataFrames with
`chispa.dataframe_comparer.assert_df_equality`.

### Releasing

Releases are automated. Pushing a tag of the form `vMAJOR.MINOR.PATCH` triggers
[.github/workflows/publish_release.yml](.github/workflows/publish_release.yml), which:

1. runs the clean-code gate (black, pylint, mypy, pytest),
2. builds the package with `uv build`, and
3. publishes a GitHub Release with the wheel and source archive attached.

```bash
git tag v0.1.0
git push origin v0.1.0
```
