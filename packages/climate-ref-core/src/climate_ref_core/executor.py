"""
Executor interface for running diagnostics
"""

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from climate_ref_core.diagnostics import Diagnostic, ExecutionDefinition
from climate_ref_core.providers import DiagnosticProvider

if TYPE_CHECKING:
    from climate_ref.models import Execution

EXECUTION_LOG_FILENAME = "out.log"
"""
Filename for the execution log.

This file is written via [climate_ref_core.logging.redirect_logs][].
"""


@runtime_checkable
class Executor(Protocol):
    """
    An executor is responsible for running a diagnostic asynchronously

    The diagnostic may be run locally in the same process or in a separate process or container.

    Notes
    -----
    This is an extremely basic interface and will be expanded in the future, as we figure out
    our requirements.
    """

    name: str

    def __init__(self, **kwargs: Any) -> None: ...

    def run(
        self,
        provider: DiagnosticProvider,
        diagnostic: Diagnostic,
        definition: ExecutionDefinition,
        execution: "Execution | None" = None,
    ) -> None:
        """
        Execute a diagnostic with a given definition

        No executions are returned from this method,
        as the execution may be performed asynchronously so executions may not be immediately available.

        /// admonition | Note
        In future, we may return a `Future` object that can be used to retrieve the result,
        but that requires some additional work to implement.
        ///

        Parameters
        ----------
        provider
            Provider of the diagnostic
        diagnostic
            Diagnostic to run
        definition
            Definition of the information needed to execute a diagnostic

            This definition describes which datasets are required to run the diagnostic and where
            the output should be stored.
        execution
            The execution object to update with the results of the execution.

            This is a database object that contains the executions of the execution.
            If provided, it will be updated with the executions of the execution.
            This may happen asynchronously, so the executions may not be immediately available.

        Returns
        -------
        :
            Results from running the diagnostic
        """
        ...

    def join(self, timeout: float) -> None:
        """
        Wait for all executions to finish

        If the timeout is reached, the method will return and raise an exception.

        Parameters
        ----------
        timeout
            Maximum time to wait for all executions to finish in seconds

        Raises
        ------
        TimeoutError
            If the timeout is reached
        """
