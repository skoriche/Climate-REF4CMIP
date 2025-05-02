import pytest
from climate_ref_esmvaltool import provider

from climate_ref.models import Execution
from climate_ref.solver import solve_executions
from climate_ref.testing import validate_result
from climate_ref_core.diagnostics import Diagnostic

diagnostics = [pytest.param(metric, id=metric.slug) for metric in provider.diagnostics()]


@pytest.mark.slow
@pytest.mark.parametrize("diagnostic", diagnostics)
def test_metrics(diagnostic: Diagnostic, data_catalog, tmp_path, config, mocker):
    mocker.patch.object(Execution, "execution_group")
    # Ensure the conda prefix is set
    provider.configure(config)

    # Get the first match from the data catalog
    execution = next(
        solve_executions(
            data_catalog=data_catalog,
            diagnostic=diagnostic,
            provider=provider,
        )
    )

    # Run the diagnostic
    definition = execution.build_execution_definition(output_root=config.paths.scratch)
    definition.output_directory.mkdir(parents=True, exist_ok=True)
    result = diagnostic.run(definition)

    # Check the result
    validate_result(diagnostic, config, result)
