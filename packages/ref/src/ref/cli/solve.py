import typer

from ref.solver import solve_metrics

app = typer.Typer()


@app.command()
def solve(
    ctx: typer.Context,
    dry_run: bool = typer.Option(False, help="Do not execute any metrics"),
) -> None:
    """
    Solve for metrics that require recalculation

    This may trigger a number of additional calculations depending on what data has been ingested
    since the last solve.
    """
    with ctx.obj.database.session.begin():
        solve_metrics(ctx.obj.database, dry_run=dry_run)
