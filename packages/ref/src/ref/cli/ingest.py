import errno
import os
from pathlib import Path

import pandas as pd
import typer
from loguru import logger
from ref_core.datasets import SourceDatasetType
from ref_core.exceptions import OutOfTreeDatasetException
from rich import box
from rich.console import Console
from rich.table import Table

from ref.cli.config import load_config
from ref.config import Config
from ref.database import Database
from ref.datasets import get_dataset_adapter
from ref.models.dataset import Dataset
from ref.solver import solve_metrics

app = typer.Typer()
console = Console()


def validate_path(config: Config, raw_path: str) -> Path:
    """
    Validate the prefix of a dataset against the data directory
    """
    prefix = Path(raw_path)

    # Check if the prefix is relative to the data directory
    if prefix.is_relative_to(config.paths.data):
        prefix = prefix.relative_to(config.paths.data)
    elif config.paths.allow_out_of_tree_datasets:
        logger.warning(f"Dataset {prefix} is not relative to {config.paths.data}")
    else:
        raise OutOfTreeDatasetException(prefix, config.paths.data)

    return prefix


def pretty_print_df(df: pd.DataFrame) -> None:
    """
    Pretty print a DataFrame

    Parameters
    ----------
    df
        DataFrame to print
    """
    # Initiate a Table instance to be modified
    table = Table(*[str(column) for column in df.columns])

    for index, value_list in enumerate(df.values.tolist()):
        row = [str(x) for x in value_list]
        table.add_row(*row)

    # Update the style of the table
    table.row_styles = ["none", "dim"]
    table.box = box.SIMPLE_HEAD

    console.print(table)


@app.command()
def ingest(
    file_or_directory: Path,
    configuration_directory: Path | None = typer.Option(None, help="Configuration directory"),
    source_type: SourceDatasetType = typer.Option(help="Type of source dataset"),
    solve: bool = typer.Option(False, help="Run metrics after ingestion"),
    dry_run: bool = typer.Option(False, help="Do not execute any metrics"),
) -> None:
    """
    Ingest a dataset

    This will register a dataset in the database to be used for metrics calculations.
    """
    config = load_config(configuration_directory)
    db = Database(config.db.database_url)

    logger.info(f"ingesting {file_or_directory}")

    adapter = get_dataset_adapter(source_type.value)

    # Create a data catalog from the specified file or directory
    if not file_or_directory.exists():
        logger.error(f"File or directory {file_or_directory} does not exist")
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), file_or_directory)

    data_catalog = adapter.find_local_datasets(file_or_directory)
    adapter.validate_data_catalog(data_catalog)

    logger.info(f"Found {len(data_catalog)} files for {len(data_catalog.index.unique())} datasets")
    pretty_print_df(adapter.pretty_subset(data_catalog))

    for instance_id, data_catalog_dataset in data_catalog.groupby(adapter.slug_column):
        logger.info(f"Processing dataset {instance_id}")

        if dry_run:
            dataset = db.session.query(Dataset).filter_by(slug=instance_id, dataset_type=source_type).first()
            if not dataset:
                logger.info(f"Would save dataset {instance_id} to the database")
                continue
        else:
            with db.session.begin():
                adapter.register_dataset(config, db, data_catalog_dataset)

    if solve:
        solve_metrics(
            db=db,
            dry_run=dry_run,
        )
