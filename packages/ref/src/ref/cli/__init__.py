"""Entrypoint for the CLI"""

import typer

from ref.cli import config, ingest

app = typer.Typer(name="ref")

app.command(name="ingest")(ingest.ingest)
app.add_typer(config.app, name="config")
