from pathlib import Path

import climate_ref_pmp
import pytest


@pytest.fixture(scope="module")
def pdo_example_dir(regression_data_dir) -> Path:
    return (
        regression_data_dir
        / "pmp"
        / "extratropical-modes-of-variability-pdo"
        / "cmip6_hist-GHG_r1i1p1f1_ACCESS-ESM1-5_r1i1p1f1__obs4mips_HadISST-1-1_ts"
    )


@pytest.fixture
def provider(tmp_path):
    provider = climate_ref_pmp.provider
    provider.prefix = tmp_path / "conda"
    provider.prefix.mkdir()
    provider._conda_exe = provider.prefix / "mock_micromamba"
    provider._conda_exe.touch()
    provider.env_path.mkdir()

    return provider
