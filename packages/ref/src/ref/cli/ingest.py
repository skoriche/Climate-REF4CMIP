from pathlib import Path

import ecgtools.parsers
import pandas as pd
import typer
from ecgtools import Builder

from ref.cli.config import load_config
from ref.cli.solve import solve as solve_cli
from ref.database import Database
from ref.models.dataset import SourceDatasetType

app = typer.Typer()


def parse_datasets(file_or_directory: Path, source_type: SourceDatasetType) -> pd.DataFrame:
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
    if source_type.CMIP6:
        builder = Builder(
            paths=[str(file_or_directory)],
            depth=10,
            include_patterns=["*.nc"],
            joblib_parallel_kwargs={"n_jobs": 1},
        ).build(parsing_func=ecgtools.parsers.parse_cmip6)

        datasets = builder.df

    elif source_type.CMIP7:
        # TODO: Assuming that the same fields will be used for CMIP7
        # Update as needed.

        builder = Builder(
            paths=[str(file_or_directory)],
            depth=10,
            include_patterns=["*.nc"],
            joblib_parallel_kwargs={"n_jobs": 1},
        ).build(parsing_func=ecgtools.parsers.parse_cmip6)

        datasets = builder.df

    return datasets


@app.command()
def ingest(
    file_or_directory: Path,
    configuration_directory: Path | None = typer.Option(None, help="Configuration directory"),
    source_type: SourceDatasetType = typer.Option(help="Type of source dataset"),
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

    datasets = parse_datasets(file_or_directory, source_type)

    typer.echo(f"Found {len(datasets)} datasets")

    if not dry_run:
        for dataset in datasets.itertuples():
            db.register_dataset(dataset)

    if solve:
        solve_cli(file_or_directory, configuration_directory, dry_run=dry_run)
