import pandas

from climate_ref_core.constraints import (
    RequireContiguousTimerange,
    RequireFacets,
    RequireOverlappingTimerange,
)
from climate_ref_core.datasets import FacetFilter, SourceDatasetType
from climate_ref_core.diagnostics import DataRequirement
from climate_ref_esmvaltool.diagnostics.base import ESMValToolDiagnostic
from climate_ref_esmvaltool.recipe import dataframe_to_recipe
from climate_ref_esmvaltool.types import Recipe


class ENSOBasicClimatology(ESMValToolDiagnostic):
    """
    Calculate the ENSO CLIVAR metrics - background climatology.
    """

    name = "ENSO Basic Climatology"
    slug = "enso-basic-climatology"
    base_recipe = "ref/recipe_enso_basicclimatology.yml"

    variables = (
        "pr",
        "tos",
        "tauu",
    )
    data_requirements = (
        DataRequirement(
            source_type=SourceDatasetType.CMIP6,
            filters=(
                FacetFilter(
                    facets={
                        "variable_id": variables,
                        "experiment_id": "historical",
                    },
                ),
            ),
            group_by=("source_id", "member_id", "grid_label"),
            constraints=(
                RequireFacets("variable_id", variables),
                RequireContiguousTimerange(group_by=("instance_id",)),
                RequireOverlappingTimerange(group_by=("instance_id",)),
            ),
        ),
    )
    facets = ()

    @staticmethod
    def update_recipe(recipe: Recipe, input_files: pandas.DataFrame) -> None:
        """Update the recipe."""
        recipe_variables = dataframe_to_recipe(input_files)
        recipe.pop("datasets")
        for diagnostic in recipe["diagnostics"].values():
            for variable in diagnostic["variables"].values():
                variable["additional_datasets"].extend(
                    recipe_variables[variable["short_name"]]["additional_datasets"]
                )
