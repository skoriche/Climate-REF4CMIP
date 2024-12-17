from typer.testing import CliRunner

from ref.cli import app
from ref.database import Database
from ref.models import Dataset, MetricExecution

runner = CliRunner()


def test_solve(esgf_data_dir, config, invoke_cli):
    db = Database.from_config(config)

    result = invoke_cli(app, ["datasets", "ingest", "--source-type", "cmip6", str(esgf_data_dir)])
    assert db.session.query(Dataset).count() == 5

    result = invoke_cli(app, ["--verbose", "solve"])
    assert "Created metric execution ACCESS-ESM1-5_rsut_ssp126_r1i1p1f1" in result.stdout
    assert "Running metric" in result.stdout
    assert db.session.query(MetricExecution).count() == 2

    # Running solve again should not trigger any new metric executions
    result = invoke_cli(app, ["--verbose", "solve"])
    assert "Created metric execution ACCESS-ESM1-5_rsut_ssp126_r1i1p1f1" not in result.stdout
    assert db.session.query(MetricExecution).count() == 2
    execution = db.session.query(MetricExecution).filter_by(key="ACCESS-ESM1-5_rsut_ssp126_r1i1p1f1").one()

    assert len(execution.results[0].datasets) == 1
    assert (
        execution.results[0].datasets[0].instance_id
        == "CMIP6.ScenarioMIP.CSIRO.ACCESS-ESM1-5.ssp126.r1i1p1f1.Amon.rsut.gn.v20210318"
    )
