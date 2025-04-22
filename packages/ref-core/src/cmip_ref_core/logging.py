"""
Logging utilities

The REF uses [loguru](https://loguru.readthedocs.io/en/stable/), a simple logging framework
"""

import contextlib
import inspect
import logging
import sys
from collections.abc import Generator
from typing import Any

import pooch
from loguru import logger
from rich.pretty import pretty_repr

from cmip_ref_core.executor import EXECUTION_LOG_FILENAME
from cmip_ref_core.metrics import MetricExecutionDefinition


class _InterceptHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        # Get corresponding Loguru level if it exists.
        level: str | int
        try:
            level = logger.level(record.levelname).name
        except ValueError:  # pragma: no cover
            level = record.levelno

        # Find caller from where originated the logged message.
        frame, depth = inspect.currentframe(), 0
        while frame and (depth == 0 or frame.f_code.co_filename == logging.__file__):
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


def capture_logging() -> None:
    """
    Capture logging from the standard library and redirect it to Loguru

    Note that this replaces the root logger, so any other handlers attached to it will be removed.
    """
    # Pooch adds a handler to its own logger which circumvents the REF logger
    pooch.get_logger().handlers.clear()
    pooch.get_logger().addHandler(_InterceptHandler())

    logging.basicConfig(handlers=[_InterceptHandler()], level=0, force=True)

    # Disable some overly verbose logs
    logger.disable("matplotlib.colorbar")
    logger.disable("matplotlib.ticker")
    logger.disable("matplotlib.font_manager")
    logger.disable("pyproj.transformer")
    logger.disable("pint.facets.plain.registry")


def add_log_handler(**kwargs: Any) -> None:
    """
    Add a log sink to the logger to capture logs.

    This is useful for testing purposes, to ensure that logs are captured correctly.
    """
    if hasattr(logger, "default_handler_id"):
        raise AssertionError("The default log handler has already been created")

    kwargs.setdefault("sink", sys.stderr)

    handled_id = logger.add(**kwargs)

    # Track the current handler via custom attributes on the logger
    # This is a bit of a workaround because of loguru's super slim API that doesn't allow for
    # modificiation of existing handlers.
    logger.default_handler_id = handled_id  # type: ignore[attr-defined]
    logger.default_handler_kwargs = kwargs  # type: ignore[attr-defined]

    capture_logging()


def remove_log_handler() -> None:
    """
    Remove the default log handler from the logger.

    This is useful for cleaning up after tests or when changing logging configurations.
    The previously used logger kwargs are kept in `logger.default_handler_kwargs` if the
    logger should be readded later
    """
    if hasattr(logger, "default_handler_id"):
        logger.remove(logger.default_handler_id)
        del logger.default_handler_id
    else:
        raise AssertionError("No default log handler to remove.")


@contextlib.contextmanager
def redirect_logs(definition: MetricExecutionDefinition, log_level: str) -> Generator[None, None, None]:
    """
    Temporarily redirect log output to a file.

    This also writes some common log messages

    Parameters
    ----------
    definition
        Metric definition to capture logging for

    log_level
        Log level as a string e.g. INFO, WARNING, DEBUG.
        This log level will dictate what logs will be sent to disk
        The logger will also be reset to this level after leaving the context manager.

    """
    app_logger_configured = hasattr(logger, "default_handler_id")

    # Remove existing default log handler
    # This swallows the logs from the app logger
    # If the app logger hasn't been configured yet, we don't need to remove it,
    # as logs will also be written to the console as loguru adds a stderr handler by default
    if app_logger_configured:
        remove_log_handler()

    # Add a new log handler for the execution log
    output_file = definition.output_directory / EXECUTION_LOG_FILENAME
    file_handler_id = logger.add(output_file, level=log_level, colorize=False)
    capture_logging()

    logger.info(f"Running definition {pretty_repr(definition)}")
    try:
        yield
    except:
        logger.exception("Execution failed")
        raise
    finally:
        logger.info(f"Metric execution complete. Results available in {definition.output_fragment()}")

        # Reset the logger to the default
        logger.remove(file_handler_id)

        # We only re-add the app handler if it was configured before
        if app_logger_configured:
            add_log_handler(**logger.default_handler_kwargs)  # type: ignore[attr-defined]


__all__ = ["add_log_handler", "capture_logging", "logger", "redirect_logs"]
