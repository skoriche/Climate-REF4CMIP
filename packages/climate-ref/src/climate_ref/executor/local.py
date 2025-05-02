from typing import Any

from loguru import logger

from climate_ref.config import Config
from climate_ref.database import Database
from climate_ref.executor import handle_execution_result
from climate_ref.models import Execution
from climate_ref_core.diagnostics import Diagnostic, ExecutionDefinition, ExecutionResult
from climate_ref_core.logging import redirect_logs
from climate_ref_core.providers import DiagnosticProvider


class LocalExecutor:
    """
    Run a diagnostic locally, in-process.

    This is mainly useful for debugging and testing.
    The production executor will run the diagnostic in a separate process or container,
    the exact manner of which is yet to be determined.
    """

    name = "local"

    def __init__(
        self, *, database: Database | None = None, config: Config | None = None, **kwargs: Any
    ) -> None:
        if config is None:
            config = Config.default()
        if database is None:
            database = Database.from_config(config, run_migrations=False)

        self.database = database
        self.config = config

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
        definition.output_directory.mkdir(parents=True, exist_ok=True)

        try:
            with redirect_logs(definition, self.config.log_level):
                result = diagnostic.run(definition=definition)
        except Exception:
            if execution is not None:  # pragma: no branch
                info_msg = (
                    f"\nAdditional information about this execution can be viewed using: "
                    f"ref executions inspect {execution.execution_group_id}"
                )
            else:
                info_msg = ""

            logger.exception(f"Error running diagnostic {diagnostic.slug}. {info_msg}")
            result = ExecutionResult.build_from_failure(definition)

        if execution:
            handle_execution_result(self.config, self.database, execution, result)

    def join(self, timeout: float) -> None:
        """
        Wait for all diagnostics to finish

        This returns immediately because the local executor runs diagnostics synchronously.

        Parameters
        ----------
        timeout
            Timeout in seconds (Not used)
        """
        return
