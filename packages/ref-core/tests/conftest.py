import pathlib

import pytest
from ref_core.datasets import MetricDataset, SourceDatasetType
from ref_core.metrics import DataRequirement, MetricExecutionDefinition, MetricResult
from ref_core.providers import MetricsProvider


class MockMetric:
    name = "mock"
    slug = "mock"

    def __init__(self, temp_dir: pathlib.Path) -> None:
        self.temp_dir = temp_dir

    # This runs on every dataset
    data_requirements = (DataRequirement(source_type=SourceDatasetType.CMIP6, filters=(), group_by=None),)

    def run(self, definition: MetricExecutionDefinition) -> MetricResult:
        return MetricResult(
            output_fragment=self.temp_dir / definition.output_fragment / "output.json",
            successful=True,
        )


class FailedMetric:
    name = "failed"
    slug = "failed"

    data_requirements = (DataRequirement(source_type=SourceDatasetType.CMIP6, filters=(), group_by=None),)

    def run(self, definition: MetricExecutionDefinition) -> MetricResult:
        return MetricResult(
            successful=False,
        )


@pytest.fixture
def provider(tmp_path) -> MetricsProvider:
    provider = MetricsProvider("mock_provider", "v0.1.0")
    provider.register(MockMetric(tmp_path))
    provider.register(FailedMetric())

    return provider


@pytest.fixture
def mock_metric(tmp_path) -> MockMetric:
    return MockMetric(tmp_path)


@pytest.fixture
def metric_definition(tmp_path) -> MetricExecutionDefinition:
    return MetricExecutionDefinition(
        output_fragment=tmp_path, key="mocked-metric-slug", metric_dataset=MetricDataset({})
    )
