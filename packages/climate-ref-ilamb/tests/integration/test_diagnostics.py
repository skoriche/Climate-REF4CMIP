import pytest
from climate_ref_ilamb import provider as ilamb_provider

from climate_ref_core.diagnostics import Diagnostic

diagnostics = [pytest.param(diagnostic, id=diagnostic.slug) for diagnostic in ilamb_provider.diagnostics()]


@pytest.mark.slow
@pytest.mark.parametrize("diagnostic", diagnostics)
def test_diagnostics(diagnostic: Diagnostic, diagnostic_validation):
    if diagnostic.slug.startswith("thetao"):
        pytest.xfail("Missing data for thetao diagnostics")

    diagnostic_validation(diagnostic)
