"""Entrypoint for the CLI"""

import typer

from ref.cli import config, sync

app = typer.Typer(name="ref")

app.command(name="sync")(sync.sync)
app.add_typer(config.app, name="config")
