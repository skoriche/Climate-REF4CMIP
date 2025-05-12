import concurrent.futures
import re
from concurrent.futures import Future

import pytest

from climate_ref.executor.local import ExecutionFuture, LocalExecutor
from climate_ref_core.diagnostics import ExecutionResult
from climate_ref_core.exceptions import ExecutionError
from climate_ref_core.executor import Executor


class TestLocalExecutor:
    def test_is_executor(self):
        executor = LocalExecutor()

        assert executor.name == "local"
        assert isinstance(executor, Executor)

    def test_takes_process_pool(self):
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        executor = LocalExecutor(pool=pool)

        assert executor.pool == pool

    def test_run_metric(self, metric_definition, provider, mock_diagnostic, mocker, caplog):
        process_pool = mocker.MagicMock(spec=concurrent.futures.ProcessPoolExecutor)
        executor = LocalExecutor(pool=process_pool)

        executor.run(metric_definition, None)
        assert len(executor._results) == 1
        assert executor._results[0].definition == metric_definition
        assert executor._results[0].execution is None

        # This directory is created by the executor
        assert process_pool.submit.call_count == 1

    def test_join(self, metric_definition):
        executor = LocalExecutor(n=1)
        future = Future()
        executor._results = [ExecutionFuture(future, definition=metric_definition, execution=None)]

        # Future isn't done yet
        with pytest.raises(TimeoutError):
            executor.join(0.1)

        # The executor should still have the future
        assert len(executor._results) == 1

        future.set_result(
            ExecutionResult(
                definition=metric_definition,
                successful=False,
                output_bundle_filename=None,
                metric_bundle_filename=None,
            )
        )

        executor.join(0.1)

        assert len(executor._results) == 0

    def test_join_exception(self, metric_definition):
        executor = LocalExecutor(n=1)
        future = Future()
        executor._results = [ExecutionFuture(future, definition=metric_definition, execution=None)]

        future.set_exception(ValueError("Some thing bad went wrong"))

        with pytest.raises(ExecutionError, match=re.escape("Failed to execute 'mock_provider/mock/key'")):
            executor.join(0.1)
