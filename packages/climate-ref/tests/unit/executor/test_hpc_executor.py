import re
from unittest.mock import MagicMock, patch

import parsl
import pytest
from parsl.dataflow import futures

from climate_ref.executor.hpc import HPCExecutor, execute_locally
from climate_ref.executor.local import ExecutionFuture
from climate_ref_core.diagnostics import ExecutionResult
from climate_ref_core.exceptions import ExecutionError
from climate_ref_core.executor import Executor


def test_execute_locally(definition_factory, mock_diagnostic):
    definition = definition_factory(diagnostic=mock_diagnostic)
    result = execute_locally(
        definition,
        log_level="DEBUG",
    )
    assert result.successful is True
    assert definition.output_directory.exists()


def test_execute_locally_failed(definition_factory, mock_diagnostic):
    mock_diagnostic.run = lambda definition: 1 / 0

    # execution raises an exception
    result = execute_locally(
        definition_factory(diagnostic=mock_diagnostic),
        log_level="DEBUG",
    )

    assert result.successful is False


class TestHPCExecutor:
    def test_is_executor(self, tmp_path):
        executor = HPCExecutor(log_dir=tmp_path / "parsl_runinfo")

        assert executor.name == "hpc"
        assert isinstance(executor, Executor)
        parsl.dfk().cleanup()

    def test_run_metric(self, metric_definition, provider, mock_diagnostic, mocker, caplog, tmp_path):
        with patch.object(HPCExecutor, "run", autospec=True) as mock_run:
            # Configure the mock to behave similarly to the original
            mock_run.side_effect = lambda self, definition, execution=None: (
                self.parsl_results.append(
                    ExecutionFuture(
                        future=MagicMock(),  # Mock the future object
                        definition=definition,
                        execution_id=execution.id if execution else None,
                    )
                )
            )

            executor = HPCExecutor(log_dir=tmp_path / "parsl_runinfo")

            # shall have the SerializationError, but not raised
            executor.run(metric_definition, None)
            assert len(executor.parsl_results) == 1
            assert executor.parsl_results[0].definition == metric_definition
            assert executor.parsl_results[0].execution_id is None

        parsl.dfk().cleanup()

    def test_join(self, metric_definition, tmp_path):
        executor = HPCExecutor(log_dir=tmp_path / "parsl_runinfo")
        future = futures.AppFuture(1)
        executor.parsl_results = [ExecutionFuture(future, definition=metric_definition, execution_id=None)]

        future.set_result(
            ExecutionResult(
                definition=metric_definition,
                successful=False,
                output_bundle_filename=None,
                metric_bundle_filename=None,
            )
        )
        executor.join(0.1)

        assert len(executor.parsl_results) == 0

    def test_join_exception(self, metric_definition, tmp_path):
        executor = HPCExecutor(log_dir=tmp_path / "parsl_runinfo")
        future = futures.AppFuture(1)
        executor.parsl_results = [ExecutionFuture(future, definition=metric_definition, execution_id=None)]

        future.set_exception(ValueError("Some thing bad went wrong"))

        with pytest.raises(ExecutionError, match=re.escape("Failed to execute 'mock_provider/mock/key'")):
            executor.join(0.1)
