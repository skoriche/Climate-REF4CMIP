import typer

from climate_ref.solver import solve_required_executions

app = typer.Typer()


@app.command()
def solve(
    ctx: typer.Context,
    dry_run: bool = typer.Option(False, help="Do not execute any diagnostics"),
    timeout: int = typer.Option(60, help="Timeout in seconds for the solve operation"),
    one_per_provider: bool = typer.Option(
        False, help="Limit to one execution per provider. This is useful for testing"
    ),
    one_per_diagnostic: bool = typer.Option(
        False, help="Limit to one execution per diagnostic. This is useful for testing"
    ),
) -> None:
    """
    Solve for executions that require recalculation

    This may trigger a number of additional calculations depending on what data has been ingested
    since the last solve.
    """
    config = ctx.obj.config
    db = ctx.obj.database
    solve_required_executions(
        config=config,
        db=db,
        dry_run=dry_run,
        timeout=timeout,
        one_per_provider=one_per_provider,
        one_per_diagnostic=one_per_diagnostic,
    )
