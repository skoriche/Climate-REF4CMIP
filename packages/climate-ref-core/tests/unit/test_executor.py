import pytest

from climate_ref.executor import LocalExecutor
from climate_ref_core.exceptions import InvalidExecutorException
from climate_ref_core.executor import Executor, import_executor_cls


@pytest.mark.parametrize(
    "import_str", ["climate_ref.executor.local.LocalExecutor", "climate_ref.executor.LocalExecutor"]
)
def test_import_executor(import_str):
    executor = import_executor_cls(import_str)

    assert isinstance(executor, Executor)
    assert executor == LocalExecutor


def test_import_executor_missing():
    fqn = "climate_ref.executor.local.WrongExecutor"
    match = f"Invalid executor: '{fqn}'\n Executor 'WrongExecutor' not found in climate_ref.executor.local"
    with pytest.raises(InvalidExecutorException, match=match):
        import_executor_cls(fqn)

    fqn = "missing.executor.local.WrongExecutor"
    match = f"Invalid executor: '{fqn}'\n Module 'missing.executor.local' not found"
    with pytest.raises(InvalidExecutorException, match=match):
        import_executor_cls(fqn)
