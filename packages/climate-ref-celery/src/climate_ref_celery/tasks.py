"""
Task generation and registration for Celery

This module provides a factory function to create Celery tasks for diagnostics.
These celery tasks are then registered with the Celery app to enable them to be run asynchronously.

Since the diagnostic definition may be in a different virtual environment it is not possible to directly
import the provider and create the tasks in both the worker and the main process.

Instead, the tasks are registered only in the worker process.
The main process can then send tasks to the worker using the task name.
The main process is responsible for tracking what diagnostics have been registered
and to respond to new workers coming online.
"""

from collections.abc import Callable

from celery import Celery
from loguru import logger

from climate_ref_core.diagnostics import Diagnostic, ExecutionDefinition, ExecutionResult
from climate_ref_core.logging import redirect_logs
from climate_ref_core.providers import DiagnosticProvider


def generate_task_name(provider: DiagnosticProvider, diagnostic: Diagnostic) -> str:
    """
    Generate the name of the task for the given provider and diagnostic
    """
    return f"{provider.slug}.{diagnostic.slug}"


def _diagnostic_task_factory(
    diagnostic: Diagnostic,
) -> Callable[
    [ExecutionDefinition, str],
    ExecutionResult,
]:
    """
    Create a new task for the given diagnostic
    """

    def task(definition: ExecutionDefinition, log_level: str) -> ExecutionResult:
        """
        Task to run the diagnostic
        """
        logger.info(f"Running diagnostic {diagnostic.name} with definition {definition}")
        try:
            with redirect_logs(definition, log_level):
                return diagnostic.run(definition)
        except Exception:
            logger.exception(f"Error running diagnostic {diagnostic.slug}:{definition.key}")
            # TODO: This exception should be caught and a unsuccessful result returned.
            raise

    return task


def register_celery_tasks(app: Celery, provider: DiagnosticProvider) -> None:
    """
    Register all tasks for the given provider

    This is run on worker startup to register all tasks a given provider

    Parameters
    ----------
    app
        The Celery app to register the tasks with
    provider
        The provider to register tasks for
    """
    for metric in provider.diagnostics():
        print(f"Registering task for diagnostic {metric.name}")
        app.task(  # type: ignore
            _diagnostic_task_factory(metric),
            name=generate_task_name(provider, metric),
            queue=provider.slug,
        )
