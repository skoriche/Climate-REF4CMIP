"""
Execute metrics in different environments

We support running metrics in different environments, such as locally,
in a separate process, or in a container.
These environments are represented by `Executor` classes.
The `REF_EXECUTOR` environment variable determines which executor is used.

The simplest executor is the `LocalExecutor`, which runs the metric in the same process.
This is useful for local testing and debugging.

This is a placeholder implementation and will be expanded in the future.
"""

import os

from cmip_ref.executor.local import LocalExecutor
from cmip_ref_core.executor import Executor
from cmip_ref_core.metrics import MetricExecutionDefinition, MetricResult
from cmip_ref_core.providers import MetricsProvider


class ExecutorManager:
    """
    Enables the registration of executors and retrieval by name.

    This is exposed as a singleton instance `cmip_ref.executor.get_executor`
     and `cmip_ref.executor.register_executor`,
     but for testability, you can create your own instance.
    """

    def __init__(self) -> None:
        self._executors: dict[str, Executor] = {}

    def register(self, executor: Executor) -> None:
        """
        Register an executor with the manager

        Parameters
        ----------
        executor
            The executor to register
        """
        if not isinstance(executor, Executor):  # pragma: no cover
            raise ValueError("Executor must be an instance of Executor")
        self._executors[executor.name.lower()] = executor

    def get(self, name: str) -> Executor:
        """
        Get an executor by name

        Parameters
        ----------
        name
            Name of the executor (case-sensitive)

        Raises
        ------
        KeyError
            If the executor with the given name is not found

        Returns
        -------
        :
            The requested executor
        """
        return self._executors[name.lower()]


_default_manager = ExecutorManager()

register_executor = _default_manager.register
get_executor = _default_manager.get


def run_metric(
    metric_name: str, /, metrics_provider: MetricsProvider, definition: MetricExecutionDefinition
) -> MetricResult:
    """
    Run a metric using the default executor

    The executor is determined by the `REF_EXECUTOR` environment variable.
    The arguments will be updated in the future as the metric execution interface is expanded.

    TODO: migrate to a configuration object rather than relying on environment variables.

    Parameters
    ----------
    metric_name
        Name of the metric to run.
    metrics_provider
        Provider from where to retrieve the metric
    definition
        Information that describes a given metric execution.

        This includes the datasets that are needed to run the metric,
        where the output should be stored, and any other information needed to run the metric.

    Returns
    -------
    :
        The result of the metric execution
    """
    executor_name = os.environ.get("REF_EXECUTOR", "local")

    executor = get_executor(executor_name)
    metric = metrics_provider.get(metric_name)

    result = executor.run_metric(metric, definition)

    # TODO: Validate the result
    # TODO: Log the result

    return result


register_executor(LocalExecutor())
