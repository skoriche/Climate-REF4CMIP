import typer

app = typer.Typer()


@app.command()
def sync() -> None:
    """
    Placeholder command for syncing data
    """  # noqa: D401
    print("syncing data")
