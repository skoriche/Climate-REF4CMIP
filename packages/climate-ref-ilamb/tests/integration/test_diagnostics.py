import pytest
from climate_ref_ilamb import provider as ilamb_provider

from climate_ref_core.diagnostics import Diagnostic

skipped_diagnostics = [
    "csoil-hwsd2",  # Incorrect time spans
    "nbp-hoffman",  # Incorrect time spans
]

diagnostics = [
    pytest.param(
        diagnostic,
        id=diagnostic.slug,
        marks=[pytest.mark.xfail(reason="Expected failure")]
        if diagnostic.slug in skipped_diagnostics
        else [],
    )
    for diagnostic in ilamb_provider.diagnostics()
]


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
    validator.execution_regression.check(definition.key, definition.output_directory)
