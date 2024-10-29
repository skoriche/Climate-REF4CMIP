import pytest
from ref_core.executor import Executor, ExecutorManager, run_metric
from ref_core.executor.local import LocalExecutor


class MockMetric:
    name = "mock"

    def run(self, *args, **kwargs):
        result = {
            "args": args,
            "kwargs": kwargs,
        }

        return result


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

    def test_run_metric(self):
        executor = LocalExecutor()

        metric = MockMetric()
        result = executor.run_metric(metric, "test", kwarg="test")

        assert result == {
            "args": ("test",),
            "kwargs": {"kwarg": "test"},
        }


@pytest.mark.parametrize("executor_name", ["local", None])
def test_run_metric_local(monkeypatch, executor_name):
    if executor_name:
        monkeypatch.setenv("CMIP_REF_EXECUTOR", executor_name)
    result = run_metric(MockMetric(), "test", kwarg="test")
    assert result == {
        "args": ("test",),
        "kwargs": {"kwarg": "test"},
    }


def test_run_metric_unknown(monkeypatch):
    monkeypatch.setenv("CMIP_REF_EXECUTOR", "missing")
    with pytest.raises(KeyError):
        run_metric(MockMetric(), "test", kwarg="test")
