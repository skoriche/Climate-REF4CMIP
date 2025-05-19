from pathlib import Path
from typing import Any

import ilamb3  # type: ignore
import ilamb3.regions as ilr  # type: ignore
import matplotlib.pyplot as plt
import pandas as pd
import pooch
from ilamb3 import run

from climate_ref_core.dataset_registry import dataset_registry_manager
from climate_ref_core.datasets import FacetFilter, SourceDatasetType
from climate_ref_core.diagnostics import (
    DataRequirement,
    Diagnostic,
    ExecutionDefinition,
    ExecutionResult,
)
from climate_ref_core.pycmec.metric import CMECMetric
from climate_ref_core.pycmec.output import CMECOutput
from climate_ref_ilamb.datasets import (
    registry_to_collection,
)


def format_cmec_output_bundle(
    dataset: pd.DataFrame,
    dimensions: list[str],
    metadata_columns: list[str],
    value_column: str = "value",
) -> dict[str, Any]:
    """
    Create a CMEC output bundle for the dataset.

    Parameters
    ----------
    dataset
        Processed dataset
    dimensions
        The dimensions of the dataset (e.g., ["source_id", "member_id", "region"])
    metadata_columns
        The columns to be used as metadata (e.g., ["Description", "LongName"])
    value_column
        The column containing the values

    Returns
    -------
        A CMEC output bundle ready to be written to disk
    """
    # Validate that all required columns exist
    required_columns = set(dimensions) | {value_column} | set(metadata_columns)
    missing_columns = required_columns - set(dataset.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    # Build the dimensions section
    dimensions_dict: dict[str, dict[str, dict[str, str]]] = {}

    # For each dimension, create a dictionary of unique values and their metadata
    for dim in dimensions:
        unique_values = dataset[dim].unique()
        dim_dict: dict[str, dict[str, str]] = {}

        for val in unique_values:
            # Get the row for this dimension value

            dim_dict[str(val)] = {}

            if dim == dimensions[-1]:
                # If this is the last dimension, add the value column to the metadata

                dim_dict[str(val)] = dataset[dataset[dim] == val].iloc[0][metadata_columns].to_dict()

        dimensions_dict[dim] = dim_dict

    # Build the results section - create nested structure based on dimensions
    def nest_results(df: pd.DataFrame, dims: list[str]) -> dict[str, Any] | float:
        if not dims:
            return float(df[value_column].iloc[0].item())

        current_dim = dims[0]
        remaining_dims = dims[1:]

        return {
            str(group_name): nest_results(group_df, remaining_dims)
            for group_name, group_df in df.groupby(current_dim)
        }

    results = nest_results(dataset, list(dimensions))

    return {"DIMENSIONS": {"json_structure": list(dimensions), **dimensions_dict}, "RESULTS": results}


def _build_cmec_bundle(df: pd.DataFrame) -> dict[str, Any]:
    """
    Build a CMEC bundle from information in the dataframe.

    """
    # TODO: Handle the reference data
    # reference_df = df[df["source"] == "Reference"]
    model_df = df[df["source"] != "Reference"]

    # Source is formatted as "ACCESS-ESM1-5-r1i1p1f1-gn"
    # This assumes that the member_id and grid_label are always the last two parts of the source string
    # and don't contain '-'
    extracted_source = model_df.source.str.extract(r"([\w-]+)-([\w\d]+)-([\w\d]+)")
    model_df["source_id"] = extracted_source[0]
    model_df["member_id"] = extracted_source[1]
    model_df["grid_label"] = extracted_source[2]

    # Strip out units from the name
    # These are available in the attributes
    extracted_source = model_df.name.str.extract(r"(.*)\s\[.*\]")
    model_df["name"] = extracted_source[0]

    model_df = model_df.rename(
        columns={
            "analysis": "metric",
            "name": "statistic",
        }
    )

    dimensions = ["experiment_id", "source_id", "member_id", "grid_label", "region", "metric", "statistic"]
    attributes = ["type", "units"]

    bundle = format_cmec_output_bundle(
        model_df,
        dimensions=dimensions,
        metadata_columns=attributes,
        value_column="value",
    )

    ilamb_regions = ilr.Regions()
    for region, region_info in bundle["DIMENSIONS"]["region"].items():
        if region == "None":
            region_info["LongName"] = "None"
            region_info["Description"] = "Reference data extents"
            region_info["Generator"] = "N/A"
        else:
            region_info["LongName"] = ilamb_regions.get_name(region)
            region_info["Description"] = ilamb_regions.get_name(region)
            region_info["Generator"] = ilamb_regions.get_source(region)

    return bundle


def _form_bundles(df: pd.DataFrame) -> tuple[CMECMetric, CMECOutput]:
    """
    Create the output bundles (really a lift to make Ruff happy with the size of run()).
    """
    metric_bundle = _build_cmec_bundle(df)
    output_bundle = CMECOutput.create_template()
    return CMECMetric.model_validate(metric_bundle), CMECOutput.model_validate(output_bundle)


def _set_ilamb3_options(registry: pooch.Pooch, registry_file: str) -> None:
    """
    Set options for ILAMB based on which registry file is being used.
    """
    ilamb3.conf.reset()
    ilamb_regions = ilr.Regions()
    if registry_file == "ilamb":
        ilamb_regions.add_netcdf(registry.fetch("ilamb/regions/GlobalLand.nc"))
        ilamb_regions.add_netcdf(registry.fetch("ilamb/regions/Koppen_coarse.nc"))
        ilamb3.conf.set(regions=["global", "tropical"])


def _measure_facets(registry_file: str) -> list[str]:
    """
    Set options for ILAMB based on which registry file is being used.
    """
    if registry_file == "ilamb":
        return ["areacella", "sftlf"]
    return []


def _load_csv_and_merge(output_directory: Path) -> pd.DataFrame:
    """
    Load individual csv scalar data and merge into a dataframe.
    """
    df = pd.concat(
        [pd.read_csv(f, keep_default_na=False, na_values=["NaN"]) for f in output_directory.glob("*.csv")]
    ).drop_duplicates(subset=["source", "region", "analysis", "name"])
    return df


class ILAMBStandard(Diagnostic):
    """
    Apply the standard ILAMB analysis with respect to a given reference dataset.
    """

    def __init__(
        self,
        registry_file: str,
        metric_name: str,
        sources: dict[str, str],
        **ilamb_kwargs: Any,
    ):
        # Setup the diagnostic
        if len(sources) != 1:
            raise ValueError("Only single source ILAMB diagnostics have been implemented.")
        self.variable_id = next(iter(sources.keys()))
        if "sources" not in ilamb_kwargs:  # pragma: no cover
            ilamb_kwargs["sources"] = sources
        if "relationships" not in ilamb_kwargs:
            ilamb_kwargs["relationships"] = {}
        self.ilamb_kwargs = ilamb_kwargs

        # REF stuff
        self.name = metric_name
        self.slug = self.name.lower().replace(" ", "-")
        self.data_requirements = (
            DataRequirement(
                source_type=SourceDatasetType.CMIP6,
                filters=(
                    FacetFilter(
                        facets={
                            "variable_id": (
                                self.variable_id,
                                *ilamb_kwargs.get("relationships", {}).keys(),
                                *ilamb_kwargs.get("alternate_vars", []),
                                *_measure_facets(registry_file),
                            )
                        }
                    ),
                    FacetFilter(facets={"frequency": ("mon", "fx")}),
                    FacetFilter(facets={"experiment_id": ("historical", "land-hist")}),
                ),
                group_by=("experiment_id",),
            ),
        )
        self.facets = (
            "experiment_id",
            "source_id",
            "member_id",
            "grid_label",
            "region",
            "metric",
            "statistic",
        )

        # Setup ILAMB data and options
        self.registry_file = registry_file
        self.registry = dataset_registry_manager[self.registry_file]
        self.ilamb_data = registry_to_collection(
            dataset_registry_manager[self.registry_file],
        )

    def execute(self, definition: ExecutionDefinition) -> None:
        """
        Run the ILAMB standard analysis.
        """
        plt.rcParams.update({"figure.max_open_warning": 0})
        _set_ilamb3_options(self.registry, self.registry_file)
        ref_datasets = self.ilamb_data.datasets.set_index(self.ilamb_data.slug_column)
        run.run_simple(
            ref_datasets,
            self.slug,
            definition.datasets[SourceDatasetType.CMIP6].datasets,
            definition.output_directory,
            **self.ilamb_kwargs,
        )

    def build_execution_result(self, definition: ExecutionDefinition) -> ExecutionResult:
        """
        Build the diagnostic result after running ILAMB.

        Parameters
        ----------
        definition
            The definition of the diagnostic execution

        Returns
        -------
            An execution result object
        """
        selectors = definition.datasets[SourceDatasetType.CMIP6].selector_dict()
        _set_ilamb3_options(self.registry, self.registry_file)

        df = _load_csv_and_merge(definition.output_directory)
        # Add the selectors to the dataframe
        for key, value in selectors.items():
            df[key] = value
        metric_bundle, output_bundle = _form_bundles(df)

        return ExecutionResult.build_from_output_bundle(
            definition, cmec_output_bundle=output_bundle, cmec_metric_bundle=metric_bundle
        )
