"""Entrypoint for the CLI"""

import logging
import sys
from pathlib import Path
from typing import Annotated, Optional

import typer
from attrs import define
from loguru import logger

from ref import __core_version__, __version__
from ref.cli import config, datasets, solve
from ref.cli._logging import capture_logging
from ref.config import Config
from ref.constants import config_filename
from ref.database import Database


@define
class CLIContext:
    """
    Context object that can be passed to commands
    """

    config: Config
    database: Database


def _version_callback(value: bool) -> None:
    if value:
        print(f"ref: {__version__}")
        print(f"ref-core: {__core_version__}")
        raise typer.Exit()


def load_config(configuration_directory: Path | None = None) -> Config:
    """
    Load the configuration from the specified directory

    Parameters
    ----------
    configuration_directory
        The directory to load the configuration from

        If the specified directory is not found, the process will exit with an exit code of 1

        If None, the default configuration will be loaded

    Returns
    -------
    :
        The configuration loaded from the specified directory
    """
    try:
        if configuration_directory:
            config = Config.load(configuration_directory / config_filename, allow_missing=False)
        else:
            config = Config.default()
    except FileNotFoundError:
        typer.secho("Configuration file not found", fg=typer.colors.RED)
        raise typer.Exit(1)
    return config


app = typer.Typer(name="ref", no_args_is_help=True)

app.command(name="solve")(solve.solve)
app.add_typer(config.app, name="config")
app.add_typer(datasets.app, name="datasets")


@app.callback()
def main(
    ctx: typer.Context,
    configuration_directory: Path | None = typer.Option(None, help="Configuration directory"),
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

    config = load_config(configuration_directory)
    ctx.obj = CLIContext(config=config, database=Database.from_config(config))


if __name__ == "__main__":
    app()
