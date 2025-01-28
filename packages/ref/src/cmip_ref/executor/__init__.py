"""
Execute metrics in different environments

We support running metrics in different environments, such as locally,
in a separate process, or in a container.
These environments are represented by `cmip_ref.executor.Executor` classes.

The simplest executor is the `LocalExecutor`, which runs the metric in the same process.
This is useful for local testing and debugging.
"""

import importlib

from loguru import logger

from cmip_ref_core.exceptions import InvalidExecutorException
from cmip_ref_core.executor import Executor


def import_executor(fqn: str) -> Executor:
    """
    Import an executor using a fully qualified module path

    Parameters
    ----------
    fqn
        Full package and attribute name of the executor to import

        For example: `cmip_ref_metrics_example.executor` will use the `executor` attribute from the
        `cmip_ref_metrics_example` package.

    Raises
    ------
    cmip_ref_core.exceptions.InvalidExecutorException
        If the executor cannot be imported

        If the executor isn't a valid `MetricsProvider`.

    Returns
    -------
    :
        Executor instance
    """
    module, attribute_name = fqn.rsplit(".", 1)

    try:
        imp = importlib.import_module(module)
        executor = getattr(imp, attribute_name)
        if not isinstance(executor, Executor):
            raise InvalidExecutorException(fqn, f"Expected Executor, got {type(executor)}")
        return executor
    except ModuleNotFoundError:
        logger.error(f"Package '{fqn}' not found")
        raise InvalidExecutorException(fqn, f"Module '{module}' not found")
    except AttributeError:
        logger.error(f"Provider '{fqn}' not found")
        raise InvalidExecutorException(fqn, f"Executor '{attribute_name}' not found in {module}")
