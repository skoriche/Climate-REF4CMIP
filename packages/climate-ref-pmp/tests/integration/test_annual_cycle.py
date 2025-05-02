import pytest
from climate_ref_pmp import provider

from climate_ref.models import MetricExecutionResult as MetricExecutionResultModel
from climate_ref.solver import solve_metric_executions
from climate_ref.testing import validate_result


@pytest.mark.slow
def test_annual_cycle(data_catalog, tmp_path, config, mocker):
    metric = provider.get("annual-cycle")
    mocker.patch.object(MetricExecutionResultModel, "metric_execution_group")

    # Ensure the conda prefix is set
    provider.configure(config)

    # Get the first match from the data catalog
    execution = next(
        solve_metric_executions(
            data_catalog=data_catalog,
            metric=metric,
            provider=provider,
        )
    )

    # Run the metric
    definition = execution.build_metric_execution_info(output_root=config.paths.scratch)
    definition.output_directory.mkdir(parents=True)
    result = metric.run(definition)

    # Check the result
    validate_result(config, result)
