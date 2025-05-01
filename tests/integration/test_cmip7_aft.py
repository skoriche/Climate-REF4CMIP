import platform
from collections.abc import Iterable

import pandas as pd
import pytest

from climate_ref.config import default_providers
from climate_ref.database import Database
from climate_ref.models import ExecutionGroup


def create_execution_dataframe(executions: Iterable[ExecutionGroup]) -> pd.DataFrame:
    data = []

    for execution in executions:
        assert len(execution.executions) == 1
        result = execution.executions[0]

        data.append(
            {
                "diagnostic": execution.metric.slug,
                "provider": execution.metric.provider.slug,
                "execution_id": execution.id,
                "result_id": result.id,
                "execution_key": execution.key,
                "successful": result.successful,
            }
        )

    return pd.DataFrame(data)


@pytest.fixture
def config_cmip7_aft(config):
    """
    Overwrite the default test config to use the diagnostic providers for CMIP7 Assessment Fast Track
    """
    # Force the default diagnostic providers
    config.diagnostic_providers = default_providers()

    # Write the config to disk so it is used by the CLI
    # This overwrites the default config
    config.save()

    return config


@pytest.mark.slow
def test_solve_cmip7_aft(
    sample_data_dir,
    config_cmip7_aft,
    invoke_cli,
    monkeypatch,
):
    # Arm-based MacOS users will need to set the environment variable `MAMBA_PLATFORM=osx-64`
    if platform.system() == "Darwin" and platform.machine() == "arm64":
        monkeypatch.setenv("MAMBA_PLATFORM", "osx-64")

    assert len(config_cmip7_aft.diagnostic_providers) == 3

    db = Database.from_config(config_cmip7_aft)

    # Ingest the sample data
    invoke_cli(["datasets", "ingest", "--source-type", "cmip6", str(sample_data_dir / "CMIP6")])
    invoke_cli(["datasets", "ingest", "--source-type", "obs4mips", str(sample_data_dir / "obs4MIPs")])
    invoke_cli(["datasets", "ingest", "--source-type", "obs4mips", str(sample_data_dir / "obs4REF")])

    # Solve
    # This will also create conda environments for the diagnostic providers
    # We always log the std out and stderr from the command as it is useful for debugging
    invoke_cli(["--verbose", "solve", "--timeout", f"{60 * 60}"], always_log=True)

    execution_groups = db.session.query(ExecutionGroup).all()
    df = create_execution_dataframe(execution_groups)
    print(df)

    # Check that all 3 diagnostic providers have been used
    # TODO: Update once the PMP diagnostics are solving
    assert set(df["provider"].unique()) == {"esmvaltool", "ilamb", "pmp"}

    # TODO: Ignore the PMP diagnostics for now
    df = df[df["provider"] != "pmp"]

    # Check that all diagnostics have been successful
    assert df["successful"].all(), df[["diagnostic", "successful"]]
