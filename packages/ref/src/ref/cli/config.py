"""
View and update the REF configuration
"""

import typer

app = typer.Typer(help=__doc__)


@app.command()
def list():
    """
    Print the current ref configuration
    """
    print("config")


@app.command()
def update():
    """
    Print the current ref configuration
    """
    print("config")
