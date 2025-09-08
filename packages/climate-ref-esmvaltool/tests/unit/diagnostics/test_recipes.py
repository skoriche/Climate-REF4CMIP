"""Test that the recipes are updated correctly."""

from pathlib import Path

import pandas as pd
import pytest
from climate_ref_esmvaltool import provider
from climate_ref_esmvaltool.diagnostics.base import ESMValToolDiagnostic
from pytest_regressions.file_regression import FileRegressionFixture

from climate_ref.solver import solve_executions
from climate_ref_core.datasets import SourceDatasetType


@pytest.mark.parametrize(
    "diagnostic", [pytest.param(diagnostic, id=diagnostic.slug) for diagnostic in provider.diagnostics()]
)
def test_write_recipe(
    tmp_path: Path,
    file_regression: FileRegressionFixture,
    data_catalog: dict[SourceDatasetType, pd.DataFrame],
    diagnostic: ESMValToolDiagnostic,
):
    execution = next(
        solve_executions(
            data_catalog=data_catalog,
            diagnostic=diagnostic,
            provider=diagnostic.provider,
        )
    )
    definition = execution.build_execution_definition(output_root=tmp_path)
    definition.output_directory.mkdir(parents=True, exist_ok=True)
    recipe_path = diagnostic.write_recipe(definition=definition)
    encoding = "utf-8"
    file_regression.check(
        recipe_path.read_text(encoding),
        encoding=encoding,
        fullpath=Path(__file__).parent / "recipes" / f"recipe-{diagnostic.slug}.yml".replace("-", "_"),
    )
