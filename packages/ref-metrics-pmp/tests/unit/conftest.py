from pathlib import Path

import cmip_ref_metrics_pmp
import pytest


@pytest.fixture(scope="module")
def pdo_example_dir() -> Path:
    return Path(__file__).parent / "test-data" / "pdo-example"


@pytest.fixture
def provider(tmp_path):
    provider = cmip_ref_metrics_pmp.provider
    provider.prefix = tmp_path / "conda"
    provider.prefix.mkdir()
    provider._conda_exe = provider.prefix / "mock_micromamba"
    provider._conda_exe.touch()
    provider.env_path.mkdir()

    return provider
