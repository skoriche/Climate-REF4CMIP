import inspect
import logging

from loguru import logger


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
    # logger.debug("Capturing logging from the standard library")
    logging.basicConfig(handlers=[_InterceptHandler()], level=0, force=True)


__all__ = ["capture_logging", "logger"]
