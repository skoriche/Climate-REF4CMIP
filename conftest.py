"""
Re-useable fixtures etc. for tests that are shared across the whole project

See https://docs.pytest.org/en/7.1.x/reference/fixtures.html#conftest-py-sharing-fixtures-across-multiple-files
"""

import os
import re
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pandas as pd
import pytest
from _pytest.logging import LogCaptureFixture
from click.testing import Result
from loguru import logger
from typer.testing import CliRunner

from climate_ref import cli
from climate_ref.config import Config, DiagnosticProviderConfig
from climate_ref.datasets.cmip6 import CMIP6DatasetAdapter
from climate_ref.datasets.obs4mips import Obs4MIPsDatasetAdapter
from climate_ref.testing import TEST_DATA_DIR, fetch_sample_data
from climate_ref_core.datasets import DatasetCollection, ExecutionDatasetCollection, SourceDatasetType
from climate_ref_core.diagnostics import (
    DataRequirement,
    Diagnostic,
    ExecutionDefinition,
    ExecutionResult,
)
from climate_ref_core.logging import add_log_handler, remove_log_handler
from climate_ref_core.providers import DiagnosticProvider

pytest_plugins = ("celery.contrib.pytest",)


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow to run")


def pytest_addoption(parser):
    parser.addoption("--slow", action="store_true", help="include tests marked slow")


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--slow"):
        skip_slow = pytest.mark.skip(reason="need --slow option to run")
        for item in items:
            if item.get_closest_marker("slow"):
                item.add_marker(skip_slow)


@pytest.fixture(scope="session")
def tmp_path_session():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def caplog(caplog: LogCaptureFixture) -> Iterator[LogCaptureFixture]:
    """
    Capture logs from the default logger
    """

    def filter_(record):
        return record["level"].no >= caplog.handler.level

    add_log_handler(sink=caplog.handler, level=0, format="{message}", filter=filter_)
    yield caplog
    remove_log_handler()


@pytest.fixture(autouse=True)
def cleanup_log_handlers(request: pytest.FixtureRequest) -> Iterator[None]:
    yield
    if hasattr(logger, "default_handler_id"):
        logger.warning("Logger handler not removed, removing it now")
        remove_log_handler()


@pytest.fixture(scope="session")
def test_data_dir() -> Path:
    if TEST_DATA_DIR is None:
        raise ValueError("Test data should exist when running tests")
    return TEST_DATA_DIR


@pytest.fixture(scope="session")
def sample_data_dir(test_data_dir) -> Path:
    return test_data_dir / "sample-data"


@pytest.fixture(autouse=True, scope="session")
def sample_data() -> None:
    # Downloads the sample data if it doesn't exist
    logger.disable("climate_ref_core.dataset_registry")
    fetch_sample_data(force_cleanup=False, symlink=False)
    logger.enable("climate_ref_core.dataset_registry")


@pytest.fixture(scope="session")
def cmip6_data_catalog(sample_data_dir) -> pd.DataFrame:
    adapter = CMIP6DatasetAdapter()
    return adapter.find_local_datasets(sample_data_dir / "CMIP6")


@pytest.fixture(scope="session")
def obs4mips_data_catalog(sample_data_dir) -> pd.DataFrame:
    adapter = Obs4MIPsDatasetAdapter()
    return pd.concat(
        [
            adapter.find_local_datasets(sample_data_dir / "obs4MIPs"),
            adapter.find_local_datasets(sample_data_dir / "obs4REF"),
        ]
    )


@pytest.fixture(scope="session")
def data_catalog(cmip6_data_catalog, obs4mips_data_catalog):
    return {
        SourceDatasetType.CMIP6: cmip6_data_catalog,
        SourceDatasetType.obs4MIPs: obs4mips_data_catalog,
    }


@pytest.fixture(autouse=True)
def config(tmp_path, monkeypatch, request) -> Config:
    # Optionally use the `REF_TEST_OUTPUT` env variable as the root output directory
    # This is useful in the CI to capture any executions for later analysis
    root_output_dir = Path(os.environ.get("REF_TEST_OUTPUT", tmp_path / "climate_ref"))
    # Each test gets its own directory (based on the test filename and the test name)
    # Sanitize the directory name to remove invalid characters
    dir_name = re.sub(r"[^a-zA-Z0-9_.-]", "_", request.node.name)
    ref_config_dir = root_output_dir / request.module.__name__ / dir_name

    monkeypatch.setenv("REF_CONFIGURATION", str(ref_config_dir))

    # Uses the default configuration
    cfg = Config.load(ref_config_dir / "ref.toml")

    # Put the conda environments in a shared location
    # ROOT / .ref / software
    cfg.paths.software = Path(__file__).parent / ".ref" / "software"

    # Allow adding datasets from outside the tree for testing
    cfg.diagnostic_providers = [DiagnosticProviderConfig(provider="climate_ref_example")]

    cfg.save()

    return cfg


@pytest.fixture
def invoke_cli():
    """
    Invoke the CLI with the given arguments and verify the exit code
    """

    # We want to split stderr and stdout
    # stderr == logging
    # stdout == output from commands
    runner = CliRunner(mix_stderr=False)

    def _invoke_cli(args: list[str], expected_exit_code: int = 0, always_log: bool = False) -> Result:
        result = runner.invoke(
            app=cli.app,
            args=args,
        )

        # Clean up the log handler the is added by the CLI
        if hasattr(logger, "default_handler_id"):
            remove_log_handler()

        if always_log or result.exit_code != expected_exit_code:
            print("## Command: ", " ".join(args))
            print("Exit code: ", result.exit_code)
            print("Command stdout")
            print(result.stdout)
            print("Command stderr")
            print(result.stderr)
            print("## Command end")

        if result.exit_code != expected_exit_code:
            if result.exception:
                raise result.exception
            raise ValueError(f"Expected exit code {expected_exit_code}, got {result.exit_code}")
        return result

    return _invoke_cli


class MockDiagnostic(Diagnostic):
    name = "mock"
    slug = "mock"

    # This runs on every dataset
    data_requirements = (DataRequirement(source_type=SourceDatasetType.CMIP6, filters=(), group_by=None),)

    def run(self, definition: ExecutionDefinition) -> ExecutionResult:
        # TODO: This doesn't write output.json, use build function?
        return ExecutionResult(
            output_bundle_filename=definition.output_directory / "output.json",
            metric_bundle_filename=definition.output_directory / "diagnostic.json",
            successful=True,
            definition=definition,
        )


class FailedDiagnostic(Diagnostic):
    name = "failed"
    slug = "failed"

    data_requirements = (DataRequirement(source_type=SourceDatasetType.CMIP6, filters=(), group_by=None),)

    def run(self, definition: ExecutionDefinition) -> ExecutionResult:
        return ExecutionResult.build_from_failure(definition)


@pytest.fixture
def provider(tmp_path, mock_diagnostic) -> DiagnosticProvider:
    provider = DiagnosticProvider("mock_provider", "v0.1.0")
    provider.register(mock_diagnostic)
    provider.register(FailedDiagnostic())

    return provider


@pytest.fixture
def mock_diagnostic() -> MockDiagnostic:
    return MockDiagnostic()


@pytest.fixture
def definition_factory(tmp_path: Path, config):
    def _create_definition(
        *,
        execution_dataset_collection: ExecutionDatasetCollection | None = None,
        cmip6: DatasetCollection | None = None,
        obs4mips: DatasetCollection | None = None,
        pmp_climatology: DatasetCollection | None = None,
    ) -> ExecutionDefinition:
        if execution_dataset_collection is None:
            datasets = {}
            if cmip6:
                datasets[SourceDatasetType.CMIP6] = cmip6
            if obs4mips:
                datasets[SourceDatasetType.obs4MIPs] = obs4mips
            if pmp_climatology:
                datasets[SourceDatasetType.PMPClimatology] = pmp_climatology
            execution_dataset_collection = ExecutionDatasetCollection(datasets)

        return ExecutionDefinition(
            key="key",
            datasets=execution_dataset_collection,
            root_directory=config.paths.scratch,
            output_directory=config.paths.scratch / "output_fragment",
        )

    return _create_definition


@pytest.fixture
def metric_definition(definition_factory, cmip6_data_catalog) -> ExecutionDefinition:
    selected_dataset = cmip6_data_catalog[
        cmip6_data_catalog["instance_id"].isin(
            {
                "CMIP6.ScenarioMIP.CSIRO.ACCESS-ESM1-5.ssp126.r1i1p1f1.Amon.tas.gn.v20210318",
                "CMIP6.ScenarioMIP.CSIRO.ACCESS-ESM1-5.ssp126.r1i1p1f1.fx.areacella.gn.v20210318",
            }
        )
    ]
    collection = ExecutionDatasetCollection(
        {
            SourceDatasetType.CMIP6: DatasetCollection(
                selected_dataset,
                "instance_id",
            )
        }
    )
    return definition_factory(execution_dataset_collection=collection)
