import pytest
from climate_ref_pmp import provider

from climate_ref.models import Execution as MetricExecutionResultModel
from climate_ref.solver import solve_executions
from climate_ref.testing import validate_result


@pytest.mark.xfail("#258")
@pytest.mark.slow
def test_annual_cycle(data_catalog, tmp_path, config, mocker):
    metric = provider.get("pmp-annual-cycle")
    mocker.patch.object(MetricExecutionResultModel, "execution")

    # Ensure the conda prefix is set
    provider.configure(config)

    # Get the first match from the data catalog
    execution = next(
        solve_executions(
            data_catalog=data_catalog,
            diagnostic=metric,
            provider=provider,
        )
    )

    # Run the diagnostic
    definition = execution.build_execution_definition(output_root=config.paths.scratch)
    definition.output_directory.mkdir(parents=True)
    result = metric.run(definition)

    # Check the result
    validate_result(config, result)
