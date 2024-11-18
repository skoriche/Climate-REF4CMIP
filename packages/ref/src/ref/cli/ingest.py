import logging
from datetime import datetime
from pathlib import Path

import ecgtools.parsers
import pandas as pd
import typer
from ecgtools import Builder
from ref_core.datasets import SourceDatasetType
from ref_core.exceptions import OutOfTreeDatasetException
from rich import box
from rich.console import Console
from rich.table import Table

from ref.cli.config import load_config
from ref.cli.solve import solve as solve_cli
from ref.config import Config
from ref.database import Database
from ref.models.dataset import CMIP6File, Dataset

app = typer.Typer()
console = Console()


def _parse_datetimes(dt_str: pd.Series) -> pd.Series:
    """
    Pandas tries to coerce everything to their own datetime format, which is not what we want here.
    """
    return pd.Series(
        [datetime.strptime(dt, "%Y-%m-%d %H:%M:%S") if dt else None for dt in dt_str],
        index=dt_str.index,
        dtype="object",
    )


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

        # Convert the start_time and end_time columns to datetime objects
        # We don't know the calendar used in the dataset (TODO: Check what ecgtools does)
        datasets["start_time"] = _parse_datetimes(datasets["start_time"])
        datasets["end_time"] = _parse_datetimes(datasets["end_time"])

        drs_items = [
            "activity_id",
            "institution_id",
            "source_id",
            "experiment_id",
            "member_id",
            "table_id",
            "variable_id",
            "grid_label",
        ]
        datasets["instance_id"] = datasets.apply(
            lambda row: "CMIP6." + ".".join([row[item] for item in drs_items]), axis=1
        )
        datasets = datasets.set_index("instance_id")

    elif source_type.CMIP7:
        # TODO: Assuming that the same fields will be used for CMIP7
        # Update as needed.

        raise NotImplementedError("CMIP7 is not yet supported")
    else:
        raise ValueError(f"Unsupported source dataset type: {source_type}")

    return datasets


def pretty_print_df(df: pd.DataFrame, source_dataset_type: SourceDatasetType) -> None:
    """
    Pretty print a DataFrame

    Parameters
    ----------
    df
        DataFrame to print
    """
    # TODO: Should this live here or alongside a defintion of a source dataset type?
    if source_dataset_type.CMIP6:
        df = df[
            [
                "activity_id",
                "institution_id",
                "source_id",
                "experiment_id",
                "member_id",
                "table_id",
                "variable_id",
                "grid_label",
                "version",
            ]
        ]
    elif source_dataset_type.CMIP7:
        df = df[
            [
                "activity_id",
                "institution_id",
                "source_id",
                "experiment_id",
                "member_id",
                "table_id",
                "variable_id",
                "grid_label",
                "version",
            ]
        ]
    else:
        raise ValueError(f"Unsupported source dataset type: {source_dataset_type}")

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
    Ingest a source dataset

    This will register a dataset in the database and trigger any metric calculations that rely on
    this dataset.
    """
    config = load_config(configuration_directory)
    db = Database(config.db.database_url)

    typer.echo(f"ingesting {file_or_directory}")

    incoming_dataset_files = parse_datasets(file_or_directory, source_type)

    typer.echo(
        f"Found {len(incoming_dataset_files)} files for {len(incoming_dataset_files.index.unique())} datasets"
    )
    pretty_print_df(incoming_dataset_files, source_type)

    for instance_id in incoming_dataset_files.index:
        dataset_files = incoming_dataset_files.loc[[instance_id]]

        logging.info(f"Processing dataset {instance_id}")

        with db.session.begin():
            if dry_run:
                dataset = (
                    db.session.query(Dataset).filter_by(slug=instance_id, dataset_type=source_type).first()
                )
                if not dataset:
                    logging.info(f"Would save dataset {instance_id} to the database")
                    continue
            else:
                dataset, created = db.get_or_create(Dataset, slug=instance_id, dataset_type=source_type)

                if not created:
                    logging.warning(f"{dataset} already exists in the database. Skipping")
                    continue

                db.session.flush()

            for dataset_file in dataset_files.to_dict(orient="records"):
                dataset_file["dataset_id"] = dataset.id
                dataset_file["instance_id"] = instance_id

                raw_path = dataset_file.pop("path")
                prefix = _validate_prefix(config, raw_path)

                if dry_run:
                    logging.info(f"Would save file {raw_path} to the database")
                else:
                    db.session.add(CMIP6File.build(prefix=str(prefix), **dataset_file))

    if solve:
        solve_cli(
            configuration_directory=configuration_directory,
            dry_run=dry_run,
        )


def _validate_prefix(config: Config, raw_path: str) -> Path:
    """
    Validate the prefix of a dataset against the data directory
    """
    prefix = Path(raw_path)

    # Check if the prefix is relative to the data directory
    if prefix.is_relative_to(config.paths.data):
        prefix = prefix.relative_to(config.paths.data)
    elif config.paths.allow_out_of_tree_datasets:
        logging.warning(f"Dataset {prefix} is not relative to {config.paths.data}")
    else:
        raise OutOfTreeDatasetException(prefix, config.paths.data)

    return prefix
