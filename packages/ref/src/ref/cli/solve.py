from pathlib import Path

import typer

from ref.cli.config import load_config
from ref.database import Database
from ref.solver import solve_metrics

app = typer.Typer()


@app.command()
def solve(
    configuration_directory: Path | None = typer.Option(None, help="Configuration directory"),
    dry_run: bool = typer.Option(False, help="Do not execute any metrics"),
) -> None:
    """
    Solve for metrics that require recalculation

    This may trigger a number of additional calculations depending on what data has been ingested
    since the last solve.
    """
    config = load_config(configuration_directory)
    db = Database(config.db.database_url)

    solve_metrics(db, dry_run=dry_run)
