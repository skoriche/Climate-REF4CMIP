import pytest

from cmip_ref.executor import ExecutorManager, run_metric
from cmip_ref.executor.local import LocalExecutor
from cmip_ref_core.datasets import MetricDataset
from cmip_ref_core.executor import Executor
from cmip_ref_core.metrics import MetricExecutionDefinition


@pytest.fixture
def metric_definition(tmp_path) -> MetricExecutionDefinition:
    return MetricExecutionDefinition(
        output_fragment=tmp_path, key="mocked-metric-slug", metric_dataset=MetricDataset({})
    )


class TestExecutorManager:
    def test_executor_register(self):
        manager = ExecutorManager()
        manager.register(LocalExecutor())

        assert len(manager._executors) == 1
        assert "local" in manager._executors
        assert isinstance(manager.get("local"), LocalExecutor)


class TestLocalExecutor:
    def test_is_executor(self):
        executor = LocalExecutor()

        assert executor.name == "local"
        assert isinstance(executor, Executor)

    def test_run_metric(self, metric_definition, mock_metric):
        executor = LocalExecutor()

        result = executor.run_metric(mock_metric, metric_definition)
        assert result.successful
        assert result.bundle_filename == metric_definition.output_fragment / "output.json"

    def test_raises_exception(self, metric_definition, mock_metric):
        executor = LocalExecutor()

        mock_metric.run = lambda definition: 1 / 0

        result = executor.run_metric(mock_metric, metric_definition)
        assert result.successful is False
        assert result.bundle_filename is None


@pytest.mark.parametrize("executor_name", ["local", None])
def test_run_metric_local(monkeypatch, executor_name, mock_metric, provider, metric_definition):
    if executor_name:
        monkeypatch.setenv("REF_EXECUTOR", executor_name)
    result = run_metric("mock", provider, definition=metric_definition)
    assert result.successful


def test_run_metric_unknown_executor(monkeypatch, provider):
    monkeypatch.setenv("REF_EXECUTOR", "missing")
    with pytest.raises(KeyError):
        run_metric("mock", metrics_provider=provider, definition=None)


def test_run_metric_unknown_metric(monkeypatch, provider):
    with pytest.raises(KeyError):
        run_metric("missing", metrics_provider=provider, definition=None)
