import pytest
from climate_ref_pmp import provider as pmp_provider

from climate_ref_core.diagnostics import Diagnostic

diagnostics = [pytest.param(diagnostic, id=diagnostic.slug) for diagnostic in pmp_provider.diagnostics()]


@pytest.mark.slow
@pytest.mark.parametrize("diagnostic", diagnostics)
def test_diagnostics(diagnostic: Diagnostic, diagnostic_validation):
    diagnostic_validation(diagnostic)
