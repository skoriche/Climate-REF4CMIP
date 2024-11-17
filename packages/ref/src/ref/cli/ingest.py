from pathlib import Path

import pandas as pd
import typer

from ref.cli.config import load_config
from ref.cli.solve import solve as solve_cli
from ref.database import Database

app = typer.Typer()


def parse_datasets(file_or_directory: Path) -> pd.DataFrame:
    """
    Find the datasets in the specified file or directory

    Parameters
    ----------
    file_or_directory

    Returns
    -------
    :
        A DataFrame containing the datasets found in the specified file or directory
    """
    return pd.DataFrame()


@app.command()
def ingest(
    file_or_directory: Path,
    configuration_directory: Path | None = typer.Option(None, help="Configuration directory"),
    solve: bool = typer.Option(False, help="Run metrics after ingestion"),
    dry_run: bool = typer.Option(False, help="Do not execute any metrics"),
) -> None:
    """
    Ingest a source dataset

    This will register a dataset in the database and trigger any metric calculations that rely on
    this dataset.
    """
    config = load_config(configuration_directory)
    db = Database(config.db.database_url)

    typer.echo(f"ingesting {file_or_directory}")

    datasets = parse_datasets(file_or_directory)

    typer.echo(f"Found {len(datasets)} datasets")

    if not dry_run:
        for dataset in datasets.itertuples():
            db.register_dataset(dataset)

    if solve:
        solve_cli(file_or_directory, configuration_directory, dry_run=dry_run)
