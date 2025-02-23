"""
View metric executions
"""

from typing import Annotated

import pandas as pd
import typer
from loguru import logger
from rich.console import Console

from cmip_ref.cli._utils import pretty_print_df
from cmip_ref.models import MetricExecution
from cmip_ref_core.datasets import SourceDatasetType

app = typer.Typer(help=__doc__)
console = Console()


@app.command(name="list")
def list_(
    ctx: typer.Context,
    source_type: Annotated[
        SourceDatasetType, typer.Option(help="Type of source dataset")
    ] = SourceDatasetType.CMIP6.value,  # type: ignore
    column: Annotated[list[str] | None, typer.Option()] = None,
    include_files: bool = typer.Option(False, help="Include files in the output"),
    limit: int = typer.Option(100, help="Limit the number of rows to display"),
) -> None:
    """
    List the datasets that have been ingested

    The data catalog is sorted by the date that the dataset was ingested (first = newest).
    """
    database = ctx.obj.database

    results: list[MetricExecution] = database.session.query(MetricExecution).all()

    results_df = pd.DataFrame(
        [
            {
                "key": result.key,
                "provider": result.metric.provider.slug,
                "metric": result.metric.slug,
            }
            for result in results
        ]
    )

    if column:
        if not all(col in results_df.columns for col in column):
            logger.error(f"Column not found in data catalog: {column}")
            raise typer.Exit(code=1)
        results_df = results_df[column]

    pretty_print_df(results_df, console=console)
