import pytest
from climate_ref_esmvaltool import provider

from climate_ref.models import Execution as MetricExecutionResultModel
from climate_ref.solver import solve_executions
from climate_ref.testing import validate_result
from climate_ref_core.diagnostics import Diagnostic

metrics = [pytest.param(metric, id=metric.slug) for metric in provider.metrics()]


@pytest.mark.slow
@pytest.mark.parametrize("diagnostic", metrics)
def test_metrics(metric: Diagnostic, data_catalog, tmp_path, config, mocker):
    mocker.patch.object(MetricExecutionResultModel, "metric_execution_group")
    # Ensure the conda prefix is set
    provider.configure(config)

    # Get the first match from the data catalog
    execution = next(
        solve_executions(
            data_catalog=data_catalog,
            metric=metric,
            provider=provider,
        )
    )

    # Run the diagnostic
    definition = execution.build_execution_definition(output_root=config.paths.scratch)
    definition.output_directory.mkdir(parents=True, exist_ok=True)
    result = metric.run(definition)

    # Check the result
    validate_result(config, result)
