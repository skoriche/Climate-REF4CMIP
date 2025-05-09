import pytest
from climate_ref_esmvaltool import provider

from climate_ref_core.diagnostics import Diagnostic

diagnostics = [pytest.param(diagnostic, id=diagnostic.slug) for diagnostic in provider.diagnostics()]


@pytest.mark.slow
@pytest.mark.parametrize("diagnostic", diagnostics)
def test_diagnostics(diagnostic: Diagnostic, diagnostic_validation):
    diagnostic_validation(diagnostic)
