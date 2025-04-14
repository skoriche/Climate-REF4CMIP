"""
Database maintenance commands
"""

from typing import Annotated

import alembic.command
import typer
from loguru import logger
from rich.console import Console

app = typer.Typer(help=__doc__)
console = Console()


@app.command()
def upgrade(
    ctx: typer.Context,
    revision: str = typer.Option("heads", help="Revision to migrate to"),
) -> None:
    """
    Upgrade the database to a given revision

    Defaults to "heads" which applies all the available migrations
    """
    database = ctx.obj.database

    alembic.command.upgrade(database.alembic_config(), revision)


@app.command()
def downgrade(
    ctx: typer.Context,
    revision: str = typer.Option(help="Revision to migrate to"),
) -> None:
    """
    Downgrade the database to a given revision

    Running any CLI commands after this will automatically migrate to the lastest revision
    """
    database = ctx.obj.database

    alembic.command.upgrade(database.alembic_config(), revision)


@app.command()
def check(
    ctx: typer.Context,
) -> None:
    """
    Check if the migrations are up to date with the codebase
    """
    database = ctx.obj.database

    alembic.command.check(database.alembic_config())


@app.command()
def revision(
    ctx: typer.Context,
    message: Annotated[str, typer.Option(help="Revision message")],
) -> None:
    """
    Generate a new revision from the current state of the codebase
    """
    database = ctx.obj.database

    logger.warning("Autogenerating a new revision")
    alembic.command.revision(database.alembic_config(), autogenerate=True, message=message)
