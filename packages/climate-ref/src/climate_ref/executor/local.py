from typing import Any

from loguru import logger

from climate_ref.config import Config
from climate_ref.database import Database
from climate_ref.executor import handle_execution_result
from climate_ref.models import Execution as MetricExecutionResultModel
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

    def run_metric(
        self,
        provider: DiagnosticProvider,
        metric: Diagnostic,
        definition: ExecutionDefinition,
        metric_execution_result: MetricExecutionResultModel | None = None,
    ) -> None:
        """
        Run a diagnostic in process

        Parameters
        ----------
        provider
            The provider of the diagnostic
        metric
            Diagnostic to run
        definition
            A description of the information needed for this execution of the diagnostic
        metric_execution_result
            A database model representing the execution of the diagnostic.
            If provided, the result will be updated in the database when completed.
        """
        definition.output_directory.mkdir(parents=True, exist_ok=True)

        try:
            with redirect_logs(definition, self.config.log_level):
                result = metric.run(definition=definition)
        except Exception:
            if metric_execution_result is not None:  # pragma: no branch
                info_msg = (
                    f"\nAdditional information about this execution can be viewed using: "
                    f"ref executions inspect {metric_execution_result.execution_group_id}"
                )
            else:
                info_msg = ""

            logger.exception(f"Error running diagnostic {metric.slug}. {info_msg}")
            result = ExecutionResult.build_from_failure(definition)

        if metric_execution_result:
            handle_execution_result(self.config, self.database, metric_execution_result, result)

    def join(self, timeout: float) -> None:
        """
        Wait for all metrics to finish

        This returns immediately because the local executor runs metrics synchronously.

        Parameters
        ----------
        timeout
            Timeout in seconds (Not used)
        """
        return
