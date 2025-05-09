import time
from concurrent.futures import Future
from functools import partial
from typing import Any

import loky
from loguru import logger
from tqdm import tqdm

from climate_ref.config import Config
from climate_ref.database import Database
from climate_ref.executor import handle_execution_result
from climate_ref.models import Execution
from climate_ref_core.diagnostics import Diagnostic, ExecutionDefinition, ExecutionResult
from climate_ref_core.logging import redirect_logs
from climate_ref_core.providers import DiagnosticProvider


def execute_locally(
    config: Config,
    database: Database,
    diagnostic: Diagnostic,
    definition: ExecutionDefinition,
    execution: Execution | None = None,
) -> None:
    """
    Run a diagnostic execution locally

    Parameters
    ----------
    config
    database
    diagnostic
    definition
    execution
    """
    definition.output_directory.mkdir(parents=True, exist_ok=True)

    try:
        with redirect_logs(definition, config.log_level):
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
        handle_execution_result(config, database, execution, result)


class LocalExecutor:
    """
    Run a diagnostic locally, in-process.

    This is mainly useful for debugging and testing.
    The production executor will run the diagnostic in a separate process or container,
    the exact manner of which is yet to be determined.
    """

    name = "local"

    def __init__(
        self,
        *,
        database: Database | None = None,
        config: Config | None = None,
        n: int | None = None,
        **kwargs: Any,
    ) -> None:
        if config is None:
            config = Config.default()
        if database is None:
            database = Database.from_config(config, run_migrations=False)
        self.n = n

        self.database = database
        self.config = config

        self._execution_func = partial(execute_locally, config=config, database=database)
        self.process_pool = loky.get_reusable_executor(max_workers=n)
        self._results: list[Future] = []

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
        future = self.process_pool.submit(
            self._execution_func,
            diagnostic=diagnostic,
            definition=definition,
            execution=execution,
        )
        self._results.append(future)

    def join(self, timeout: float) -> None:
        """
        Wait for all diagnostics to finish

        This returns immediately because the local executor runs diagnostics synchronously.

        This doesn't support a timeout.

        Parameters
        ----------
        timeout
            Timeout in seconds (Not used)
        """
        start_time = time.time()
        refresh_time = 0.5  # Time to wait between checking for completed tasks in seconds

        results = self._results
        t = tqdm(total=len(results), desc="Waiting for executions to complete", unit="execution")

        try:
            while results:
                # Wait for a short time before checking for completed executions
                time.sleep(refresh_time)

                elapsed_time = time.time() - start_time

                if elapsed_time > timeout:
                    raise TimeoutError("Not all tasks completed within the specified timeout")

                # Iterate over a copy of the list and remove finished tasks
                for result in results[:]:
                    if result.done():
                        t.update(n=1)
                        results.remove(result)
        finally:
            t.close()
