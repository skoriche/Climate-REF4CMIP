import shutil
import unittest
from pathlib import Path
from subprocess import CalledProcessError, CompletedProcess
from unittest.mock import MagicMock

import cmip_ref_metrics_pmp
import pandas as pd
import pytest
from cmip_ref_metrics_pmp.pmp_driver import _get_resource
from cmip_ref_metrics_pmp.variability_modes import ExtratropicalModesOfVariability

import cmip_ref_core.providers
from cmip_ref.solver import extract_covered_datasets
from cmip_ref_core.datasets import DatasetCollection, SourceDatasetType
from cmip_ref_core.metrics import Metric


def get_first_metric_match(data_catalog: pd.DataFrame, metric: Metric) -> pd.DataFrame:
    # obs4mips requirement is first
    datasets = extract_covered_datasets(data_catalog, metric.data_requirements[1])
    assert len(datasets) > 0
    return datasets[0]


@pytest.fixture
def provider(tmp_path):
    provider = cmip_ref_metrics_pmp.provider
    provider.prefix = tmp_path / "conda"
    provider.prefix.mkdir()
    provider._conda_exe = provider.prefix / "mock_micromamba"
    provider._conda_exe.touch()
    provider.env_path.mkdir()

    return provider


def test_pdo_metric(  # noqa: PLR0913
    cmip6_data_catalog, obs4mips_data_catalog, mocker, definition_factory, pdo_example_dir, provider
):
    metric = ExtratropicalModesOfVariability("PDO")
    metric._provider = provider
    metric_dataset = get_first_metric_match(cmip6_data_catalog, metric)

    if isinstance(obs4mips_data_catalog["path"], str):
        expected_reference_filename = obs4mips_data_catalog["path"]
    if isinstance(obs4mips_data_catalog["path"], list):
        expected_reference_filename = obs4mips_data_catalog["path"][0]
    elif isinstance(obs4mips_data_catalog["path"], pd.DataFrame):
        expected_reference_filename = obs4mips_data_catalog["path"].iloc[0]
    else:
        expected_reference_filename = "mock_test_ref_file.nc"

    definition = definition_factory(
        cmip6=DatasetCollection(metric_dataset, "instance_id"),
        obs4mips=DatasetCollection(
            pd.Series(
                {
                    "instance_id": "HadISST",
                    "source_id": "HadISST-1-1",
                    "variable_id": "ts",
                    "path": expected_reference_filename,
                }
            )
            .to_frame()
            .T,
            "instance_id",
        ),
    )

    def mock_run_fn(cmd, *args, **kwargs):
        # Copy the output from the test-data directory to the output directory
        output_path = definition.output_directory
        shutil.copytree(pdo_example_dir, output_path)
        return CompletedProcess(cmd, 0, "stdout", "stderr")

    # Mock the subprocess.run call to avoid running PMP
    # Instead the mock_run_call function will be called
    mock_run = mocker.patch.object(
        cmip_ref_core.providers.subprocess,
        "run",
        autospec=True,
        spec_set=True,
        side_effect=mock_run_fn,
    )
    result = metric.run(definition)

    mock_run.assert_called_with(
        [
            f"{provider._conda_exe}",
            "run",
            "--prefix",
            f"{provider.env_path}",
            "python",
            _get_resource("pcmdi_metrics", "variability_mode/variability_modes_driver.py", False),
            "-p",
            _get_resource("cmip_ref_metrics_pmp.params", "pmp_param_MoV-PDO.py", True),
            "--modnames",
            "ACCESS-ESM1-5",
            "--exp",
            "hist-GHG",
            "--realization",
            "r1i1p1f1",
            "--modpath",
            metric_dataset.path[0],
            "--reference_data_path",
            expected_reference_filename,
            "--reference_data_name",
            "HadISST-1-1",
            "--results_dir",
            str(definition.output_directory),
            "--cmec",
            "--no_provenance",
            "--variability_mode",
            "PDO",
        ],
        check=True,
    )

    assert result.successful

    assert str(result.output_bundle_filename) == "output.json"

    output_bundle_path = definition.output_directory / result.output_bundle_filename

    assert output_bundle_path.exists()
    assert output_bundle_path.is_file()

    assert str(result.metric_bundle_filename) == "metric.json"

    metric_bundle_path = definition.output_directory / result.metric_bundle_filename

    assert result.successful
    assert metric_bundle_path.exists()
    assert metric_bundle_path.is_file()


def test_pdo_metric_failed(cmip6_data_catalog, mocker, definition_factory, pdo_example_dir, provider):
    metric = ExtratropicalModesOfVariability("PDO")
    metric._provider = provider
    metric_dataset = get_first_metric_match(cmip6_data_catalog, metric)

    definition = definition_factory(
        cmip6=DatasetCollection(metric_dataset, "instance_id"),
        obs4mips=DatasetCollection(
            pd.Series(
                {
                    "instance_id": "HadISST",
                    "source_id": "HadISST-1-1",
                    "variable_id": "ts",
                    "path": "not_a_file",
                }
            )
            .to_frame()
            .T,
            "instance_id",
        ),
    )

    # Mock the subprocess.run call to avoid running PMP
    # Instead the mock_run_call function will be called
    mocker.patch.object(
        cmip_ref_core.providers.subprocess,
        "run",
        autospec=True,
        spec_set=True,
        side_effect=CalledProcessError(1, ["cmd"], "output", "stderr"),
    )

    with pytest.raises(CalledProcessError):
        metric.run(definition)


class TestExtratropicalModesOfVariability(unittest.TestCase):
    def test_invalid_mode_id(self):
        with self.assertRaises(ValueError) as context:
            ExtratropicalModesOfVariability("INVALID")
        self.assertIn("Unknown mode_id 'INVALID'", str(context.exception))

    def test_build_cmd_with_valid_mode_id(self):
        definition = MagicMock()
        definition.metric_dataset = {
            SourceDatasetType.CMIP6: MagicMock(
                path=["/path/to/model/file.nc"],
                source_id=["ModelA"],
                experiment_id=["historical"],
                member_id=["r1i1p1f1"],
            ),
            SourceDatasetType.obs4MIPs: MagicMock(
                datasets=[{"path": "/path/to/obs/file.nc"}],
                source_id=["HadISST-1-1"],
            ),
        }
        definition.output_directory = Path("/output/directory")

        metric = ExtratropicalModesOfVariability("PDO")
        cmd = metric.build_cmd(definition)
        self.assertIn("variability_mode/variability_modes_driver.py", cmd)

    def test_build_metric_result_no_json(self):
        definition = MagicMock()
        definition.output_directory.glob = MagicMock(return_value=[])
        metric = ExtratropicalModesOfVariability("PDO")
        result = metric.build_metric_result(definition)
        self.assertFalse(result.successful)
