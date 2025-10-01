"""
Managing remote celery workers

This module is used to manage remote execution workers for the Climate REF project.
It is added to the `ref` command line interface if the `climate-ref-celery` package is installed.

A celery worker should be run for each diagnostic provider.
"""

import importlib.metadata
from warnings import warn

import typer
from loguru import logger

from climate_ref_celery.app import create_celery_app
from climate_ref_celery.tasks import register_celery_tasks
from climate_ref_core.providers import DiagnosticProvider

app = typer.Typer(help=__doc__)


def import_provider(provider_name: str) -> DiagnosticProvider:
    """
    Import the provider using the name of a registered provider.

    Parameters
    ----------
    provider_name:
        The name of a registered provider.

        Packages can register a provider by defining an
        [entry point](https://packaging.python.org/en/latest/specifications/entry-points/)
         in its `pyproject.toml` file under the group `"climate-ref.providers"`.

        Example: 'climate_ref_esmvaltool:provider' would require a section in the `pyproject.toml` for the
        `climate_ref_esmvaltool` package like this:

        ```
        [project.entry-points."climate-ref.providers"]
        esmvaltool = "climate_ref_esmvaltool:provider"
        ```

        `"esmvaltool"` or ("climate_ref_esmvaltool:provider")
        can then be used as the `provider_name` argument.

        If the entry point is not found, an error will be raised
        and the list of available providers will be shown.

    Raises
    ------
    typer.Abort
        If the provider_package does not define a 'provider' variable

        If the provider_package is not found

    Returns
    -------
    :
        The provider instance
    """
    provider_entry_points = importlib.metadata.entry_points(group="climate-ref.providers")
    for entry_point in provider_entry_points:
        logger.debug(f"found entry point: {entry_point}")

        # Also support the case where the entrypoint definition ('name:provider') is supplied
        if entry_point.name == provider_name or entry_point.value == provider_name:  # noqa: PLR1714
            break
    else:
        found_entry_points = ", ".join(f"{ep.name} ({ep.value})" for ep in provider_entry_points)
        if len(found_entry_points) == 0:
            found_entry_points = "[]"

        typer.echo(
            f"No entry point named {provider_name!r} was found. Found entry points: {found_entry_points}."
        )
        raise typer.Abort()

    # Get the provider from the provider_package
    try:
        provider = entry_point.load()
    except ModuleNotFoundError:
        _split = entry_point.value.split(":", 1)
        typer.echo(f"Invalid entrypoint {entry_point}: Package {_split[0]!r} not found.")
        raise typer.Abort()
    except AttributeError:
        _split = entry_point.value.split(":", 1)
        typer.echo(
            f"Invalid entrypoint {entry_point}: {_split[0]!r} does not define a {_split[1]!r} attribute."
        )
        raise typer.Abort()

    if not isinstance(provider, DiagnosticProvider):
        typer.echo(f"Expected DiagnosticProvider, got {type(provider)}")
        raise typer.Abort()
    return provider


@app.command()
def start_worker(
    ctx: typer.Context,
    loglevel: str = typer.Option("info", help="Log level for the worker"),
    provider: list[str] | None = typer.Option(
        help="Name of the provider to start a worker for. This argument may be supplied multiple times. "
        "If no provider is given, the worker will consume the default queue.",
        default=None,
    ),
    package: str | None = typer.Option(help="Deprecated. Use provider instead", default=None),
    extra_args: list[str] = typer.Argument(None, help="Additional arguments for the worker"),
) -> None:
    """
    Start a Celery worker for the given provider.

    A celery worker enables the execution of tasks in the background on multiple different nodes.
    This worker will register a celery task for each diagnostic in the provider.
    The worker tasks can be executed by sending a celery task with the name
    '{package_slug}_{diagnostic_slug}'.

    Providers must be registered as entry points in the `pyproject.toml` file of the package.
    The entry point should be defined under the group `climate-ref.providers`
    (See `import_provider` for details).
    """
    # Create a new celery app
    celery_app = create_celery_app("climate_ref_celery")

    if package:
        msg = "The '--package' argument is deprecated. Use '--provider' instead."
        # Deprecation warning for package argument
        warn(
            msg,
            DeprecationWarning,
            stacklevel=2,
        )
        typer.echo(msg)
        # Assume the package is the provider
        provider = [package + ":provider"]

    queues = []
    if provider:
        for p in provider:
            # Attempt to import the provider
            provider_instance = import_provider(p)

            if hasattr(ctx.obj, "config"):
                # Configure the provider so that it knows where the conda environments are
                provider_instance.configure(ctx.obj.config)

            # Wrap each diagnostics in the provider with a celery tasks
            register_celery_tasks(celery_app, provider_instance)
            queues.append(provider_instance.slug)
    else:
        # This might need some tweaking in later PRs to pull in the appropriate tasks
        import climate_ref_celery.worker_tasks  # noqa: F401, PLC0415

        queues.append("celery")

    argv = ["worker", "-E", f"--loglevel={loglevel}", f"--queues={','.join(queues)}", *(extra_args or [])]
    celery_app.worker_main(argv=argv)


@app.command()
def list_config() -> None:
    """
    List the celery configuration
    """
    celery_app = create_celery_app("climate_ref_celery")

    print(celery_app.conf.humanize())


if __name__ == "__main__":  # pragma: no cover
    app()
