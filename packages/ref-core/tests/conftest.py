import pytest
from ref_core.metrics import MetricManager, MetricResult
from ref_core.providers import Configuration


class MockMetric:
    name = "mock"

    def run(self, configuration: Configuration) -> MetricResult:
        return MetricResult(
            output_bundle=configuration.output_directory / "output.json",
            successful=True,
        )


class FailedMetric:
    name = "failed"

    def run(self, configuration: Configuration) -> MetricResult:
        return MetricResult(
            successful=False,
        )


@pytest.fixture
def metric_manager() -> MetricManager:
    manager = MetricManager()
    manager.register(MockMetric())
    manager.register(FailedMetric())

    return manager


@pytest.fixture
def mock_metric() -> MockMetric:
    return MockMetric()


@pytest.fixture
def configuration(tmp_path) -> Configuration:
    return Configuration(
        output_directory=tmp_path,
    )
