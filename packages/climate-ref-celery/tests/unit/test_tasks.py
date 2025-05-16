from celery import Celery
from climate_ref_celery.tasks import generate_task_name, register_celery_tasks


def test_generate_task_name(mock_diagnostic):
    assert mock_diagnostic.slug == "mock"
    assert mock_diagnostic.provider.slug == "mock_provider"
    assert generate_task_name(mock_diagnostic.provider, mock_diagnostic) == "mock_provider.mock"


def test_registry_celery_tasks(provider, mocker):
    """
    Test that the tasks are registered correctly
    """
    assert len(provider) == 2

    mock_app = mocker.MagicMock(spec=Celery)
    register_celery_tasks(mock_app, provider)

    assert mock_app.task.call_count == 2
    assert mock_app.task.call_args_list[0].kwargs["name"] == "mock_provider.mock"
    assert mock_app.task.call_args_list[1].kwargs["name"] == "mock_provider.failed"
    assert mock_app.task.call_args_list[0].kwargs["queue"] == "mock_provider"
