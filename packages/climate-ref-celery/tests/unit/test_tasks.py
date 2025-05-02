from unittest.mock import Mock

from celery import Celery
from climate_ref_celery.tasks import _diagnostic_task_factory, register_celery_tasks

from climate_ref_core.diagnostics import ExecutionDefinition
from climate_ref_core.providers import DiagnosticProvider


def test_diagnostic_task_factory(tmp_path, caplog):
    # Mock Diagnostic and ExecutionDefinition
    mock_metric = Mock()

    definition = ExecutionDefinition(
        key="test", datasets=None, output_directory=tmp_path / "output", root_directory=tmp_path
    )

    # Create task using factory
    task = _diagnostic_task_factory(mock_metric)

    # Run task and check result
    result = task(definition, "INFO")
    assert result == mock_metric.run.return_value
    mock_metric.run.assert_called_once_with(definition)


def test_register_celery_tasks(mocker):
    mock_task_factory = mocker.patch("climate_ref_celery.tasks._diagnostic_task_factory")
    # Mock Celery app and DiagnosticProvider
    mock_app = Mock(spec=Celery)
    mock_provider = Mock(spec=DiagnosticProvider)
    mock_provider.slug = "test_provider"
    mock_provider.diagnostics.return_value = [Mock(), Mock()]
    mock_provider.diagnostics.return_value[0].slug = "metric1"
    mock_provider.diagnostics.return_value[1].slug = "metric2"

    # Register tasks
    register_celery_tasks(mock_app, mock_provider)

    # Check that tasks are registered
    assert mock_app.task.call_count == 2
    mock_app.task.assert_any_call(
        mock_task_factory(mock_provider.diagnostics()[0]), name="test_provider.metric1", queue="test_provider"
    )
    mock_app.task.assert_any_call(
        mock_task_factory(mock_provider.diagnostics()[1]), name="test_provider.metric2", queue="test_provider"
    )
