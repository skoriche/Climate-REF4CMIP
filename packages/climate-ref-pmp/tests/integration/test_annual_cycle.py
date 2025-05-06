import pytest
from climate_ref_pmp import provider as pmp_provider


@pytest.mark.slow
def test_annual_cycle(diagnostic_validation):
    diagnostic = pmp_provider.get("annual-cycle")

    diagnostic_validation(diagnostic)
