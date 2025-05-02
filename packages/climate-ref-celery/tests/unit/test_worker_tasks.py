from climate_ref_celery.worker_tasks import handle_result
from climate_ref_example import provider

from climate_ref.database import Database
from climate_ref.models import MetricExecutionGroup, MetricExecutionResult
from climate_ref.provider_registry import _register_provider


def test_worker_task(mocker, config):
    mock_handle_result = mocker.patch("climate_ref_celery.worker_tasks.handle_execution_result")
    db = Database.from_config(config, run_migrations=True)
    with db.session.begin():
        result = mocker.Mock()

        _register_provider(db, provider)
        metric_execution_group = MetricExecutionGroup(
            metric_id=1,
            dataset_key="key",
            dirty=True,
        )
        db.session.add(metric_execution_group)

        metric_execution_result = MetricExecutionResult(
            output_fragment="output_fragment",
            dataset_hash="hash",
            metric_execution_group=metric_execution_group,
        )
        db.session.add(metric_execution_result)

    handle_result(result, metric_execution_result.id)

    mock_handle_result.assert_called_once()


def test_worker_task_missing(mocker, config):
    result = mocker.Mock()
    Database.from_config(config, run_migrations=True)

    assert handle_result(result, 1) is None
