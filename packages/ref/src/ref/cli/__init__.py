"""Entrypoint for the CLI"""

import inspect
import logging
import sys
from typing import Annotated, Optional

import typer
from loguru import logger

from ref import __core_version__, __version__
from ref.cli import config, datasets, ingest, solve


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


app = typer.Typer(name="ref", no_args_is_help=True)

app.command(name="ingest")(ingest.ingest)
app.command(name="solve")(solve.solve)
app.add_typer(config.app, name="config")
app.add_typer(datasets.app, name="datasets")


def _version_callback(value: bool) -> None:
    if value:
        print(f"ref: {__version__}")
        print(f"ref-core: {__core_version__}")
        raise typer.Exit()


@app.callback()
def main(
    verbose: bool = typer.Option(False, "--verbose", "-v"),
    version: Annotated[
        Optional[bool],
        typer.Option("--version", callback=_version_callback, is_eager=True),
    ] = None,
) -> None:
    """
    ref: A CLI for the CMIP Rapid Evaluation Framework
    """
    capture_logging()

    lvl = logging.INFO
    if verbose:
        lvl = logging.DEBUG

    logger.remove()
    logger.add(sys.stderr, level=lvl)
