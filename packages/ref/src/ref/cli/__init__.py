"""Entrypoint for the CLI"""

import typer

from ref.cli import config, sync

app = typer.Typer()

app.command(name="sync")(sync.sync)
app.add_typer(config.app, name="config")
