import pytest
from climate_ref_pmp import provider

from climate_ref.models import Execution
from climate_ref.solver import solve_executions
from climate_ref.testing import validate_result


@pytest.mark.xfail(reason="#258")
@pytest.mark.slow
def test_annual_cycle(data_catalog, tmp_path, config, mocker):
    diagnostic = provider.get("annual-cycle")
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
    definition.output_directory.mkdir(parents=True)
    result = diagnostic.run(definition)

    # Check the result
    validate_result(diagnostic, config, result)
