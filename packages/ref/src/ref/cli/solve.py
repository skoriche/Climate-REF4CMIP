from pathlib import Path

import typer
from loguru import logger

from ref.cli.config import load_config
from ref.database import Database
from ref.solver import MetricSolver

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

    solver = MetricSolver.build_from_db(db)

    logger.info("Solving for metrics that require recalculation...")
    metric_runs = solver.solve()

    logger.info(f"Found {len(metric_runs)} new calculations to be made")

    if not dry_run:
        logger.info(f"Found {len(metric_runs)} new calculations to be made")
        for metric_run in metric_runs:
            logger.info(f"Registering metric run: {metric_run}")
