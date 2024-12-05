from typer.testing import CliRunner

from ref.cli import app
from ref.database import Database
from ref.models import Dataset, MetricExecution

runner = CliRunner()


def test_solve(esgf_data_dir, config):
    db = Database.from_config(config)

    result = runner.invoke(app, ["datasets", "ingest", "--source-type", "cmip6", str(esgf_data_dir)])
    assert result.exit_code == 0, result.stdout
    assert db.session.query(Dataset).count() == 5

    result = runner.invoke(app, ["--verbose", "solve"])
    assert result.exit_code == 0, result.stdout
    assert "Created metric execution ACCESS-ESM1-5_rsut_ssp126_r1i1p1f1" in result.stdout
    assert "Running metric" in result.stdout
    assert db.session.query(MetricExecution).count() == 2

    # Running solve again should not trigger any new metric executions
    result = runner.invoke(app, ["--verbose", "solve"])
    assert result.exit_code == 0, result.stdout
    assert "Created metric execution ACCESS-ESM1-5_rsut_ssp126_r1i1p1f1" not in result.stdout
    assert db.session.query(MetricExecution).count() == 2
