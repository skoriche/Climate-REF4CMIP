import typer

from climate_ref.solver import solve_required_executions

app = typer.Typer()


@app.command()
def solve(
    ctx: typer.Context,
    dry_run: bool = typer.Option(False, help="Do not execute any diagnostics"),
    timeout: int = typer.Option(60, help="Timeout in seconds for the solve operation"),
) -> None:
    """
    Solve for executions that require recalculation

    This may trigger a number of additional calculations depending on what data has been ingested
    since the last solve.
    """
    config = ctx.obj.config
    db = ctx.obj.database
    solve_required_executions(config=config, db=db, dry_run=dry_run, timeout=timeout)
