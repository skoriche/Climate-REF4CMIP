from functools import partial
from typing import Any

from climate_ref.config import Config
from climate_ref.database import Database
from climate_ref.executor.local import execute_locally
from climate_ref.models import Execution
from climate_ref_core.diagnostics import Diagnostic, ExecutionDefinition
from climate_ref_core.providers import DiagnosticProvider


class SynchronousExecutor:
    """
    Run a diagnostic synchronously, in-process.

    This is mainly useful for debugging and testing.
    The production executor will run the diagnostic in a separate process or container,
    the exact manner of which is yet to be determined.
    """

    name = "synchronous"

    def __init__(
        self, *, database: Database | None = None, config: Config | None = None, **kwargs: Any
    ) -> None:
        if config is None:
            config = Config.default()
        if database is None:
            database = Database.from_config(config, run_migrations=False)

        self.database = database
        self.config = config

        self._execution_func = partial(execute_locally, config=config, database=database)

    def run(
        self,
        provider: DiagnosticProvider,
        diagnostic: Diagnostic,
        definition: ExecutionDefinition,
        execution: Execution | None = None,
    ) -> None:
        """
        Run a diagnostic in process

        Parameters
        ----------
        provider
            The provider of the diagnostic
        diagnostic
            Diagnostic to run
        definition
            A description of the information needed for this execution of the diagnostic
        execution
            A database model representing the execution of the diagnostic.
            If provided, the result will be updated in the database when completed.
        """
        self._execution_func(
            diagnostic=diagnostic,
            definition=definition,
            execution=execution,
        )

    def join(self, timeout: float) -> None:
        """
        Wait for all diagnostics to finish

        This returns immediately because the executor runs diagnostics synchronously.

        Parameters
        ----------
        timeout
            Timeout in seconds (Not used)
        """
        pass
