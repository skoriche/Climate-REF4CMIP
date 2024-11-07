"""
View and update the REF configuration
"""

from pathlib import Path

import typer

from ref.config import Config
from ref.constants import config_filename

app = typer.Typer(help=__doc__)


@app.command()
def list(configuration_directory: Path | None = typer.Option(None, help="Configuration directory")) -> None:
    """
    Print the current ref configuration

    If a configuration directory is provided,
    the configuration will attempt to load from the specified directory.
    """
    try:
        if configuration_directory:
            config = Config.load(configuration_directory / config_filename, allow_missing=False)
        else:
            config = Config.default()
    except FileNotFoundError:
        typer.secho("Configuration file not found", fg=typer.colors.RED)
        raise typer.Exit(1)

    print(config.dumps(defaults=True))


@app.command()
def update() -> None:
    """
    Update a configuration value
    """
    print("config")
