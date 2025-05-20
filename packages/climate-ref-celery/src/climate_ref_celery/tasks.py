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

from celery import Celery

from climate_ref_core.diagnostics import Diagnostic
from climate_ref_core.executor import execute_locally
from climate_ref_core.providers import DiagnosticProvider


def generate_task_name(provider: DiagnosticProvider, diagnostic: Diagnostic) -> str:
    """
    Generate the name of the task for the given provider and diagnostic
    """
    return f"{provider.slug}.{diagnostic.slug}"


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
    for diagnostic in provider.diagnostics():
        print(f"Registering task for diagnostic {diagnostic.name}")

        # The task function is the same for all diagnostics
        # The diagnostic is included in the definition
        # The queue is important to ensure that the task is run in the correct worker
        app.task(  # type: ignore
            execute_locally,
            name=generate_task_name(provider, diagnostic),
            queue=provider.slug,
        )
