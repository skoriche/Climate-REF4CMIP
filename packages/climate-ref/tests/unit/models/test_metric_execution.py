from climate_ref.models import Execution, ExecutionGroup


class TestMetricExecution:
    def test_should_run_no_results(self, mocker):
        execution = mocker.Mock(spec=ExecutionGroup)
        execution.executions = []

        assert ExecutionGroup.should_run(execution, "dataset_hash")

    def test_should_run_invalid_hash(self, mocker):
        execution = mocker.Mock(spec=ExecutionGroup)
        execution_result = mocker.Mock(spec=Execution)

        execution_result.dataset_hash = "dataset_hash_old"
        execution.executions = [execution_result]

        assert ExecutionGroup.should_run(execution, "dataset_hash")

    def test_should_run_dirty(self, mocker):
        execution = mocker.Mock(spec=ExecutionGroup)
        execution_result = mocker.Mock(spec=Execution)

        execution_result.dataset_hash = "dataset_hash"
        execution.executions = [execution_result]
        execution.dirty = True

        assert ExecutionGroup.should_run(execution, "dataset_hash")

    def test_shouldnt_run(self, mocker):
        execution = mocker.Mock(spec=ExecutionGroup)
        execution_result = mocker.Mock(spec=Execution)

        execution_result.dataset_hash = "dataset_hash"
        execution.executions = [execution_result]
        execution.dirty = False

        assert not ExecutionGroup.should_run(execution, "dataset_hash")
