"""
Manage the REF providers.
"""

from typing import Annotated

import pandas as pd
import typer
from loguru import logger
from rich.console import Console

from cmip_ref.cli._utils import pretty_print_df
from cmip_ref.provider_registry import ProviderRegistry
from cmip_ref_core.providers import CondaMetricsProvider, MetricsProvider

app = typer.Typer(help=__doc__)
console = Console()


@app.command(name="list")
def list_(ctx: typer.Context) -> None:
    """
    Print the available providers.
    """
    config = ctx.obj.config
    db = ctx.obj.database
    with db.session.begin():
        provider_registry = ProviderRegistry.build_from_config(config, db)

    def get_env(provider: MetricsProvider) -> str:
        env = ""
        if isinstance(provider, CondaMetricsProvider):
            env = f"{provider.env_path}"
            if not provider.env_path.exists():
                env += " (not installed)"
        return env

    results_df = pd.DataFrame(
        [
            {
                "provider": provider.slug,
                "version": provider.version,
                "conda environment": get_env(provider),
            }
            for provider in provider_registry.providers
        ]
    )
    pretty_print_df(results_df, console=console)


@app.command()
def create_env(
    ctx: typer.Context,
    provider: Annotated[
        str | None,
        typer.Option(help="Only install the environment for the named provider."),
    ] = None,
    dev: Annotated[
        str | None,
        typer.Option(
            help=(
                "Install the development version of the metrics package using pip. "
                "Any archive url/path accepted by pip can be provided. "
                "If no url/path is provided, the default path for the metrics "
                "provider will be used."
            ),
        ),
    ] = None,
) -> None:
    """
    Create a virtual environment containing the provider software.
    """
    config = ctx.obj.config
    db = ctx.obj.database
    with db.session.begin():
        providers = ProviderRegistry.build_from_config(config, db).providers

    if provider is None:
        if dev is not None:
            msg = f'Please use the --provider argument to select the environment to install "{dev}" in.'
            logger.error(msg)
            raise typer.Exit(code=1)
    else:
        available = ", ".join([f'"{p.slug}"' for p in providers])
        providers = [p for p in providers if p.slug == provider]
        if not providers:
            msg = f'Provider "{provider}" not available. Choose from: {available}'
            logger.error(msg)
            raise typer.Exit(code=1)

    for provider_ in providers:
        txt = f"virtual environment for provider {provider_.slug}"
        if isinstance(provider_, CondaMetricsProvider):
            logger.info(f"Creating {txt} in {provider_.env_path}")
            provider_.create_env(True if dev is None else dev)
            logger.info(f"Finished creating {txt}")
        else:
            logger.info(f"Skipping creating {txt} because it does use virtual environments.")

    list_(ctx)
