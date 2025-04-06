"""
Logging utilities

The REF uses [loguru](https://loguru.readthedocs.io/en/stable/), a simple logging framework
"""

import contextlib
import inspect
import logging
import sys
from collections.abc import Generator

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
    output_file = definition.output_directory / EXECUTION_LOG_FILENAME

    logger.remove()
    logger.add(output_file, level=log_level, colorize=False)
    capture_logging()

    logger.info(f"Running definition {pretty_repr(definition)}")

    try:
        yield
    except:
        logger.exception("Execution failed")
        raise
    finally:
        logger.info(f"Metric execution complete. Results available in {definition.output_fragment()}")

        # Reset the logger to stderr
        logger.remove()
        logger.add(sys.stderr, level=log_level)
        capture_logging()


__all__ = ["capture_logging", "logger", "redirect_logs"]
