"""
View and update the REF configuration
"""

import typer

app = typer.Typer(help=__doc__)


@app.command(name="list")
def list_(ctx: typer.Context) -> None:
    """
    Print the current cmip_ref configuration

    If a configuration directory is provided,
    the configuration will attempt to load from the specified directory.
    """
    config = ctx.obj.config

    print(config.dumps(defaults=True))


@app.command()
def update() -> None:
    """
    Update a configuration value
    """
    print("config")
