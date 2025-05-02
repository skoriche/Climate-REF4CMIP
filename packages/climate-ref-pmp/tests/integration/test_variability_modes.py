import pytest
from climate_ref_pmp import provider
from climate_ref_pmp.diagnostics import ExtratropicalModesOfVariability

from climate_ref.models import Execution
from climate_ref.solver import solve_executions
from climate_ref.testing import validate_result

variability_metrics = [
    pytest.param(
        metric,
        id=metric.slug,
        marks=(
            pytest.mark.xfail(reason="https://github.com/Climate-REF/climate-ref/issues/258")
            if metric.slug
            in {
                "extratropical-modes-of-variability-pdo",
                "extratropical-modes-of-variability-npgo",
            }
            else ()
        ),
    )
    for metric in provider.diagnostics()
    if isinstance(metric, ExtratropicalModesOfVariability)
]


@pytest.mark.slow
@pytest.mark.parametrize("diagnostic", variability_metrics)
def test_variability_modes(
    diagnostic: ExtratropicalModesOfVariability, data_catalog, tmp_path, config, mocker
):
    mocker.patch.object(Execution, "execution_group")

    # Ensure the conda prefix is set
    provider.configure(config)

    if diagnostic.mode_id in ExtratropicalModesOfVariability.psl_modes:
        pytest.xfail("Missing PSL sample data")

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
    result = diagnostic.run(definition)

    # Check the result
    validate_result(diagnostic, config, result)
