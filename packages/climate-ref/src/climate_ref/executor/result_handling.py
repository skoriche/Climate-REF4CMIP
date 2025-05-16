"""
Execute diagnostics in different environments

We support running diagnostics in different environments, such as locally,
in a separate process, or in a container.
These environments are represented by `climate_ref.executor.Executor` classes.

The simplest executor is the `LocalExecutor`, which runs the diagnostic in the same process.
This is useful for local testing and debugging.
"""

import pathlib
import shutil
from typing import TYPE_CHECKING

from loguru import logger
from sqlalchemy import insert

from climate_ref.database import Database
from climate_ref.models import ScalarMetricValue
from climate_ref.models.execution import Execution, ExecutionOutput, ResultOutputType
from climate_ref_core.diagnostics import ExecutionResult, ensure_relative_path
from climate_ref_core.exceptions import ResultValidationError
from climate_ref_core.logging import EXECUTION_LOG_FILENAME
from climate_ref_core.pycmec.controlled_vocabulary import CV
from climate_ref_core.pycmec.metric import CMECMetric
from climate_ref_core.pycmec.output import CMECOutput, OutputDict

if TYPE_CHECKING:
    from climate_ref.config import Config


def _copy_file_to_results(
    scratch_directory: pathlib.Path,
    results_directory: pathlib.Path,
    fragment: pathlib.Path | str,
    filename: pathlib.Path | str,
) -> None:
    """
    Copy a file from the scratch directory to the executions directory

    Parameters
    ----------
    scratch_directory
        The directory where the file is currently located
    results_directory
        The directory where the file should be copied to
    fragment
        The fragment of the executions directory where the file should be copied
    filename
        The name of the file to be copied
    """
    assert results_directory != scratch_directory
    input_directory = scratch_directory / fragment
    output_directory = results_directory / fragment

    filename = ensure_relative_path(filename, input_directory)

    if not (input_directory / filename).exists():
        raise FileNotFoundError(f"Could not find {filename} in {input_directory}")

    output_filename = output_directory / filename
    output_filename.parent.mkdir(parents=True, exist_ok=True)

    shutil.copy(input_directory / filename, output_filename)


def handle_execution_result(
    config: "Config",
    database: Database,
    execution: Execution,
    result: "ExecutionResult",
) -> None:
    """
    Handle the result of a diagnostic execution

    This will update the diagnostic execution result with the output of the diagnostic execution.
    The output will be copied from the scratch directory to the executions directory.

    Parameters
    ----------
    config
        The configuration to use
    database
        The active database session to use
    execution
        The diagnostic execution result DB object to update
    result
        The result of the diagnostic execution, either successful or failed
    """
    # Always copy log data
    _copy_file_to_results(
        config.paths.scratch,
        config.paths.results,
        execution.output_fragment,
        EXECUTION_LOG_FILENAME,
    )

    if result.successful and result.metric_bundle_filename is not None:
        logger.info(f"{execution} successful")

        _copy_file_to_results(
            config.paths.scratch,
            config.paths.results,
            execution.output_fragment,
            result.metric_bundle_filename,
        )
        execution.mark_successful(result.as_relative_path(result.metric_bundle_filename))

        if result.output_bundle_filename:
            _copy_file_to_results(
                config.paths.scratch,
                config.paths.results,
                execution.output_fragment,
                result.output_bundle_filename,
            )
            _handle_output_bundle(
                config,
                database,
                execution,
                result.to_output_path(result.output_bundle_filename),
            )

        cmec_metric_bundle = CMECMetric.load_from_json(result.to_output_path(result.metric_bundle_filename))

        # Check that the diagnostic values conform with the controlled vocabulary
        try:
            cv = CV.load_from_file(config.paths.dimensions_cv)
            cv.validate_metrics(cmec_metric_bundle)
        except (ResultValidationError, AssertionError):
            logger.exception("Diagnostic values do not conform with the controlled vocabulary")
            # TODO: Mark the diagnostic execution result as failed once the CV has stabilised
            # execution.mark_failed()

        # Perform a bulk insert of scalar values
        # TODO: The section below will likely fail until we have agreed on a controlled vocabulary
        # The current implementation will swallow the exception, but display a log message
        try:
            # Perform this in a nested transaction to (hopefully) gracefully rollback if something
            # goes wrong
            with database.session.begin_nested():
                database.session.execute(
                    insert(ScalarMetricValue),
                    [
                        {
                            "execution_id": execution.id,
                            "value": result.value,
                            "attributes": result.attributes,
                            **result.dimensions,
                        }
                        for result in cmec_metric_bundle.iter_results()
                    ],
                )
        except Exception:
            # TODO: Remove once we have settled on a controlled vocabulary
            logger.exception("Something went wrong when ingesting diagnostic values")

        # TODO Ingest the series values

        # TODO: This should check if the result is the most recent for the execution,
        # if so then update the dirty fields
        # i.e. if there are outstanding executions don't make as clean
        execution.execution_group.dirty = False
    else:
        logger.error(f"{execution} failed")
        execution.mark_failed()


def _handle_output_bundle(
    config: "Config",
    database: Database,
    execution: Execution,
    cmec_output_bundle_filename: pathlib.Path,
) -> None:
    # Extract the registered outputs
    # Copy the content to the output directory
    # Track in the db
    cmec_output_bundle = CMECOutput.load_from_json(cmec_output_bundle_filename)
    _handle_outputs(
        cmec_output_bundle.plots,
        output_type=ResultOutputType.Plot,
        config=config,
        database=database,
        execution=execution,
    )
    _handle_outputs(
        cmec_output_bundle.data,
        output_type=ResultOutputType.Data,
        config=config,
        database=database,
        execution=execution,
    )
    _handle_outputs(
        cmec_output_bundle.html,
        output_type=ResultOutputType.HTML,
        config=config,
        database=database,
        execution=execution,
    )


def _handle_outputs(
    outputs: dict[str, OutputDict] | None,
    output_type: ResultOutputType,
    config: "Config",
    database: Database,
    execution: Execution,
) -> None:
    outputs = outputs or {}

    for key, output_info in outputs.items():
        filename = ensure_relative_path(
            output_info.filename, config.paths.scratch / execution.output_fragment
        )

        _copy_file_to_results(
            config.paths.scratch,
            config.paths.results,
            execution.output_fragment,
            filename,
        )
        database.session.add(
            ExecutionOutput(
                execution_id=execution.id,
                output_type=output_type,
                filename=str(filename),
                description=output_info.description,
                short_name=key,
                long_name=output_info.long_name,
            )
        )
