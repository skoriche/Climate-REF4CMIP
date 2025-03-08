import platform
from pathlib import Path

import pandas as pd
import pytest

from cmip_ref.config import default_metric_providers
from cmip_ref.database import Database
from cmip_ref.models import Dataset, MetricExecution


def test_solve(sample_data_dir, cmip6_data_catalog, config, invoke_cli, monkeypatch):
    num_expected_datasets = cmip6_data_catalog["instance_id"].nunique()
    num_expected_metrics = 9

    db = Database.from_config(config)
    invoke_cli(["datasets", "ingest", "--source-type", "cmip6", str(sample_data_dir / "CMIP6")])
    assert db.session.query(Dataset).count() == num_expected_datasets

    result = invoke_cli(["--verbose", "solve"])
    expected_metric_execution_name = "_".join(
        ["dataset1_ACCESS-ESM1-5_rsut_ssp126_r1i1p1f1", "dataset2_ACCESS-ESM1-5_areacella_ssp126_r1i1p1f1"]
    )
    assert f"Created metric execution {expected_metric_execution_name}" in result.stderr
    assert "Running metric" in result.stderr
    assert db.session.query(MetricExecution).count() == num_expected_metrics

    # Running solve again should not trigger any new metric executions
    result = invoke_cli(["--verbose", "solve"])
    assert f"Created metric execution {expected_metric_execution_name}" not in result.stderr
    assert db.session.query(MetricExecution).count() == num_expected_metrics
    execution = db.session.query(MetricExecution).filter_by(key=expected_metric_execution_name).one()

    assert len(execution.results[0].datasets) == 2
    assert (
        execution.results[0].datasets[0].instance_id
        == "CMIP6.ScenarioMIP.CSIRO.ACCESS-ESM1-5.ssp126.r1i1p1f1.Amon.rsut.gn.v20210318"
    )
    assert (
        execution.results[0].datasets[1].instance_id
        == "CMIP6.ScenarioMIP.CSIRO.ACCESS-ESM1-5.ssp126.r1i1p1f1.fx.areacella.gn.v20210318"
    )


def create_execution_dataframe(executions):
    data = []
    for execution in executions:
        assert len(execution.results) == 1
        result = execution.results[0]

        data.append(
            {
                "metric": execution.metric_id,
                "provider": execution.metric.provider.slug,
                "execution_id": execution.id,
                "result_id": result.id,
                "execution_key": execution.key,
                "successful": result.successful,
            }
        )

    df = pd.DataFrame(data)
    return df


@pytest.mark.slow
def test_solve_ar7_ft(
    sample_data_dir,
    config,
    invoke_cli,
    monkeypatch,
):
    # Arm-based MacOS users will need to set the environment variable `MAMBA_PLATFORM=osx-64`
    if platform.system() == "Darwin" and platform.machine() == "arm64":
        monkeypatch.setenv("MAMBA_PLATFORM", "osx-64")

    # Put the conda environments in a shared location
    # ROOT / .ref / software
    monkeypatch.setenv("REF_SOFTWARE_ROOT", str(Path().parents[3] / ".ref" / "software"))

    config.metric_providers = default_metric_providers()
    config.save()

    assert len(config.metric_providers) == 3

    db = Database.from_config(config)

    # Ingest the sample data
    invoke_cli(["datasets", "ingest", "--source-type", "cmip6", str(sample_data_dir / "CMIP6")])
    invoke_cli(["datasets", "ingest", "--source-type", "obs4mips", str(sample_data_dir / "Obs4MIPs")])

    # Solve
    # This will also create conda environments for the metric providers
    invoke_cli(["--verbose", "solve"])

    executions = db.session.query(MetricExecution).all()
    df = create_execution_dataframe(executions)

    print(df)

    assert len(df["provider"].unique()) == 3
    assert df["successful"].all()
