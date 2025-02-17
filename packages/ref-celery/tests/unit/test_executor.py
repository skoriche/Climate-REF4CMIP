import pytest
from cmip_ref_celery.executor import CeleryExecutor


def test_run_metric():
    pass


def test_join_empty():
    executor = CeleryExecutor()

    executor.join(1)


def test_join_returns_on_completion(mocker):
    executor = CeleryExecutor()
    result = mocker.Mock()
    result.ready.return_value = True
    executor._results = [result]

    executor.join(2)


def test_join_raises(mocker):
    executor = CeleryExecutor()
    result = mocker.Mock()
    result.ready.return_value = False
    executor._results = [result]

    with pytest.raises(TimeoutError):
        executor.join(0.1)
