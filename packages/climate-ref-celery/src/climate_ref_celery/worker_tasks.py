"""
Celery worker tasks for handling diagnostic execution executions.
"""

from celery import current_app
from loguru import logger

from climate_ref.config import Config
from climate_ref.database import Database
from climate_ref.executor import handle_execution_result
from climate_ref.models import Execution as MetricExecutionResultModel
from climate_ref_core.diagnostics import ExecutionResult


@current_app.task
def handle_result(result: ExecutionResult, metric_execution_result_id: int) -> None:
    """
    Handle the result of a diagnostic execution

    This function is called when a diagnostic execution is completed.

    Parameters
    ----------
    metric_execution_result_id
        The unique identifier for the diagnostic execution
    result
        The result of the diagnostic execution
    """
    logger.info(f"Handling result for diagnostic execution {metric_execution_result_id} + {result}")

    config = Config.default()
    db = Database.from_config(config, run_migrations=False)

    with db.session.begin():
        metric_execution_result = db.session.get(MetricExecutionResultModel, metric_execution_result_id)

        if metric_execution_result is None:
            logger.error(f"Metric execution result {metric_execution_result_id} not found")
            return

        handle_execution_result(config, db, metric_execution_result, result)
