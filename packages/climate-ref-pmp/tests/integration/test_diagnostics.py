import pytest
from climate_ref_pmp import provider as pmp_provider

from climate_ref_core.diagnostics import Diagnostic

diagnostics = [pytest.param(diagnostic, id=diagnostic.slug) for diagnostic in pmp_provider.diagnostics()]


@pytest.mark.slow
@pytest.mark.parametrize("diagnostic", diagnostics)
def test_diagnostics(diagnostic: Diagnostic, diagnostic_validation):
    validator = diagnostic_validation(diagnostic)

    definition = validator.get_definition()
    validator.execute(definition)


@pytest.mark.parametrize("diagnostic", diagnostics)
def test_build_results(diagnostic: Diagnostic, diagnostic_validation):
    validator = diagnostic_validation(diagnostic)

    definition = validator.get_regression_definition()
    validator.validate(definition)
