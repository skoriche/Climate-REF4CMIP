from typer.testing import CliRunner

from ref.cli import app

runner = CliRunner()


def test_solve_help():
    result = runner.invoke(app, ["solve", "--help"])
    assert result.exit_code == 0

    assert "Solve for metrics that require recalculation" in result.output


class TestSolve:
    def test_solve_without_datasets(self, esgf_data_dir, db):
        result = runner.invoke(app, ["solve"])
        assert result.exit_code == 0, result.output
