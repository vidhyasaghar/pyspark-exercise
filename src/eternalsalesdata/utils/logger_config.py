"""Logger configuration with optional PySpark Log4j bridging."""

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Optional

from pyspark.sql import SparkSession

LOG_DIR = Path("logs")
LOG_FILE = LOG_DIR / "eternalsalesdata.log"
MAX_BYTES = 5 * 1024 * 1024  # 5 MB
BACKUP_COUNT = 3

_PYTHON_TO_LOG4J = {
    logging.DEBUG: "DEBUG",
    logging.INFO: "INFO",
    logging.WARNING: "WARN",
    logging.ERROR: "ERROR",
    logging.CRITICAL: "FATAL",
}


class _SparkLog4jHandler(logging.Handler):
    """
    A Python :class:`logging.Handler` that forwards log records to a
    Log4j logger running inside the PySpark JVM.

    By inheriting from :class:`logging.Handler` this can be added to any
    Python logger via ``logger.addHandler(...)`` exactly like any other
    handler, keeping the caller completely unaware of the JVM underneath.

    :param log4j_logger: A ``org.apache.log4j.Logger`` instance obtained
        from the active SparkContext JVM.
    :type log4j_logger: Any
    """

    def __init__(self, log4j_logger: Any) -> None:
        super().__init__()
        self._log4j_logger = log4j_logger

    def emit(self, record: logging.LogRecord) -> None:
        """
        Format the record and forward it to Log4j at the matching level.

        The ``emit`` method is the single required override — Python's logging
        machinery calls it automatically whenever a record passes the level
        filter.  We format the record with whatever formatter is attached to
        this handler (set externally via ``setFormatter``) and then call the
        appropriate Log4j method.

        :param record: The log record to emit.
        :type record: logging.LogRecord
        """
        try:
            message = self.format(record)
            level = record.levelno

            if level >= logging.CRITICAL:
                self._log4j_logger.fatal(message)
            elif level >= logging.ERROR:
                self._log4j_logger.error(message)
            elif level >= logging.WARNING:
                self._log4j_logger.warn(message)
            elif level >= logging.INFO:
                self._log4j_logger.info(message)
            else:
                self._log4j_logger.debug(message)

        except Exception:  # pylint: disable=broad-except
            self.handleError(record)
            # handleError is inherited from logging.Handler — it writes
            # the traceback to stderr without crashing the application


def _build_log4j_handler(
    name: str,
    level: int,
    spark_session: SparkSession,
    formatter: logging.Formatter,
) -> Optional[_SparkLog4jHandler]:
    """
    Attempt to build a :class:`SparkLog4jHandler` from the JVM.

    Returns ``None`` silently if the JVM is unreachable so the rest of the
    logging setup is never interrupted by Spark availability.

    :param name: Logger name passed to ``LogManager.getLogger``.
    :type name: str
    :param level: Python logging level, converted to a Log4j Level object.
    :type level: int
    :param spark_session: Active SparkSession.
    :type spark_session: pyspark.sql.SparkSession
    :param formatter: Formatter to attach to the handler.
    :type formatter: logging.Formatter
    :returns: Configured handler or ``None``.
    :rtype: SparkLog4jHandler or None
    """
    try:
        jvm = getattr(spark_session.sparkContext, "_jvm", None)
        if jvm is None:
            return None

        log4j_level_name = _PYTHON_TO_LOG4J.get(level, "INFO")
        log4j_level = jvm.org.apache.log4j.Level.toLevel(log4j_level_name)

        log4j_logger = jvm.org.apache.log4j.LogManager.getLogger(name)
        log4j_logger.setLevel(log4j_level)

        handler = _SparkLog4jHandler(log4j_logger)
        handler.setLevel(level)
        handler.setFormatter(formatter)
        return handler

    except Exception:  # pylint: disable=broad-except
        return None


def get_logger(
    name: str,
    level: int = logging.INFO,
    spark_session: Optional[SparkSession] = None,
) -> logging.Logger:
    """
    Return a configured Python logger.

    Always attaches a console handler and a rotating file handler.
    When ``spark_session`` is provided a :class:`SparkLog4jHandler` is
    also attached so every log call is automatically forwarded to the
    Spark UI / cluster log pipeline with no extra effort from the caller.

    The formatter includes the Spark application name when a session is
    given so log lines are clearly tagged in mixed environments.

    :param name: Logger name — pass ``__name__`` from the calling module.
    :type name: str
    :param level: Logging level (default: ``logging.INFO``).
    :type level: int
    :param spark_session: Optional SparkSession for Log4j bridging.
    :type spark_session: pyspark.sql.SparkSession, optional
    :returns: Configured logger with all relevant handlers attached.
    :rtype: logging.Logger
    :raises ValueError: If ``name`` is empty.
    """
    if not name:
        raise ValueError("Logger name must be provided.")

    logger = logging.getLogger(name)
    logger.setLevel(level)

    if logger.hasHandlers() and spark_session is None:
        return logger

    app_name = spark_session.sparkContext.appName if spark_session else None
    fmt = (
        f"%(asctime)s | %(levelname)s | %(name)s | [{app_name}] | %(message)s"
        if app_name
        else "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    formatter = logging.Formatter(fmt, datefmt="%Y-%m-%d %H:%M:%S")

    if not logger.hasHandlers():
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        LOG_DIR.mkdir(exist_ok=True)
        file_handler = RotatingFileHandler(LOG_FILE, maxBytes=MAX_BYTES, backupCount=BACKUP_COUNT)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    if spark_session and not any(isinstance(h, _SparkLog4jHandler) for h in logger.handlers):
        spark_handler = _build_log4j_handler(name, level, spark_session, formatter)
        if spark_handler:
            logger.addHandler(spark_handler)

    return logger
