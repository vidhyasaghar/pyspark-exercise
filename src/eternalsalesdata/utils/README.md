# `utils` — shared utilities

Cross-cutting helpers used across the pipeline: a Spark session factory, CSV I/O, and
logging. All three are re-exported from the package for convenient import:

```python
from eternalsalesdata.utils import (
    get_logger,
    get_spark_session,
    read_csv_with_header,
    write_df_to_csv,
)
```

## Files

| File                | Provides |
|---------------------|----------|
| `logger_config.py`  | `get_logger` — the project's single logging entry point. |
| `spark_session.py`  | `get_spark_session` — creates/retrieves a configured `SparkSession`. |
| `spark_utils.py`    | `read_csv_with_header`, `write_df_to_csv` — CSV read/write helpers. |

## Logging — `get_logger`

```python
get_logger(name, level=logging.INFO, spark_session=None) -> logging.Logger
```

Use this everywhere instead of `print()`:

```python
from eternalsalesdata.utils import get_logger
logger = get_logger(__name__)
```

- Always attaches a **console handler** and a **rotating file handler**
  (`logs/eternalsalesdata.log`, 5 MB × 3 backups).
- Handlers are added only once per logger name, so repeated calls are safe.
- When a `spark_session` is passed, it also bridges Python logs into the Spark JVM's
  **Log4j** logger (via the internal `_SparkLog4jHandler`), so messages appear in the
  Spark UI / cluster logs and lines are tagged with the Spark app name. If the JVM is
  unreachable, this is skipped silently — logging never breaks because of Spark.
- Raises `ValueError` if `name` is empty.

## Spark session — `get_spark_session`

```python
get_spark_session(app_name, logger=None) -> SparkSession
```

Returns an active session (creating one if needed) with adaptive query execution tuned:

- `spark.sql.adaptive.enabled = true`
- `spark.sql.adaptive.advisoryPartitionSizeInBytes = 100MB`
- `spark.sql.shuffle.partitions = 10`

Pass an optional `logger` to emit init/error messages; on success the logger is
re-bound to the new session so subsequent logs are bridged to Log4j. Builder errors are
logged (if a logger was given) and re-raised.

> Requires a Java runtime (JDK 8/11/17) on the host — this is a PySpark requirement.

## CSV I/O — `spark_utils`

```python
read_csv_with_header(spark, path) -> DataFrame
```

Reads a CSV with `header=true` and `inferSchema=true`. Raises `ValueError` if `path`
is not an existing `.csv` file; re-raises any Spark read error.

```python
write_df_to_csv(df, path, mode="overwrite") -> None
```

Writes a DataFrame to `path`, `coalesce(1)`-ed to a single file with a header. Raises
`ValueError` if the parent directory does not exist; re-raises any Spark write error.
