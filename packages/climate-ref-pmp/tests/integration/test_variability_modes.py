import pytest
from climate_ref_pmp import provider as pmp_provider
from climate_ref_pmp.diagnostics import ExtratropicalModesOfVariability

variability_metrics = [
    pytest.param(
        metric,
        id=metric.slug,
    )
    for metric in pmp_provider.diagnostics()
    if isinstance(metric, ExtratropicalModesOfVariability)
]


@pytest.mark.slow
@pytest.mark.parametrize("diagnostic", variability_metrics)
def test_variability_modes(diagnostic: ExtratropicalModesOfVariability, config, diagnostic_validation):
    diagnostic_validation(diagnostic)
