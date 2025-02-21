import subprocess
from pathlib import Path
from typing import Any

import xarray as xr

from cmip_ref_core.datasets import FacetFilter, SourceDatasetType
from cmip_ref_core.metrics import DataRequirement, Metric, MetricExecutionDefinition, MetricResult


def calculate_annual_cycle(input_files: list[Path]) -> xr.Dataset:
    """
    Calculate the annual mean timeseries for a dataset.

    While this function is implemented here,
    in most cases the metric calculation will be in the underlying benchmarking package.
    How the metric is calculated is up to the provider.

    Parameters
    ----------
    input_files
        List of input files to calculate the annual mean timeseries.

        This dataset may consist of multiple data files.

    Returns
    -------
    :
        The annual mean timeseries of the dataset
    """
    xr_ds = xr.open_mfdataset(input_files, combine="by_coords", chunks=None, use_cftime=True)

    annual_mean = xr_ds.resample(time="YS").mean()
    return annual_mean.mean(dim=["lat", "lon"], keep_attrs=True)


def format_cmec_output_bundle(dataset: xr.Dataset) -> dict[str, Any]:
    """
    Create a simple CMEC output bundle for the dataset.

    Parameters
    ----------
    dataset
        Processed dataset

    Returns
    -------
        A CMEC output bundle ready to be written to disk
    """
    # TODO: Check how timeseries data are generally serialised
    cmec_output = {
        "DIMENSIONS": {
            "dimensions": {
                "source_id": {dataset.attrs["source_id"]: {}},
                "region": {"global": {}},
                "variable": {"tas": {}},
            },
            "json_structure": [
                "model",
                "region",
                "statistic",
            ],
        },
        # Is the schema tracked?
        "SCHEMA": {
            "name": "CMEC-REF",
            "package": "example",
            "version": "v1",
        },
        "RESULTS": {
            dataset.attrs["source_id"]: {"global": {"tas": 0}},
        },
    }

    return cmec_output


class ExtratropicalModesOfVariability_PDO(Metric):
    """
    Calculate the annual cycle for a dataset
    """

    name = "PMP Extratropical modes of variability PDO"
    slug = "pmp-extratropical-modes-of-variability-pdo"

    data_requirements = (
        DataRequirement(
            source_type=SourceDatasetType.CMIP6,
            filters=(
                FacetFilter(
                    facets={"frequency": "mon", "experiment_id": ("historical"), "variable_id": "ts"}
                ),
                # Ignore some experiments because they are not relevant
                FacetFilter(facets={"experiment_id": ("amip")}, keep=False),
            ),
            # Add cell areas to the groups
            # constraints=(AddCellAreas(),),
            # Run the metric on each unique combination of model, variable, experiment, and variant
            group_by=("source_id", "variable_id", "experiment_id", "variant_label", "member_id"),
        ),
    )

    def run(self, definition: MetricExecutionDefinition) -> MetricResult:
        """
        Run a metric

        Parameters
        ----------
        definition
            A description of the information needed for this execution of the metric

        Returns
        -------
        :
            The result of running the metric.
        """
        # This is where one would hook into however they want to run
        # their benchmarking packages.
        # cmec-driver, python calls, subprocess calls all would work

        input_datasets = definition.metric_dataset[SourceDatasetType.CMIP6]

        # NEED HELP: Retrive info
        # QUESTION: reference data?
        source_id = input_datasets["source_id"].unique()[0]
        member_id = input_datasets["member_id"].unique()[0]
        path_to_model_nc_file = input_datasets.path.to_list()  # this is going to be list of strings
        path_to_ref_nc_file = load_ref_data()
        reference_data_name = load_ref_data_name()
        output_directory_path = definition.output_directory

        cmd = [
            "variability_modes.py",
            "-p",
            "param/pmp_param_MoV-PDO.py",
            "--modnames",
            source_id,
            "--realization",
            member_id,
            "--modpath",
            *path_to_model_nc_file,
            "--reference_data_path",
            path_to_ref_nc_file,
            "--reference_data_name",
            reference_data_name,
            "--results_dir",
            output_directory_path,
        ]

        # Run the command and capture the output
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
        )

        # Print the command output
        print("Output:\n", proc.stdout)

        # Print any errors
        if proc.stderr:
            print("Error:\n", proc.stderr)

        #annual_mean_global_mean_timeseries = calculate_annual_cycle(input_files=input_datasets.path.to_list())
        
        # Expected outcome from the run: a JSON file and some PNG files
        # QUESTION: Do we expect the metric to return the output, or save it somewhere should be fine?

        # Load json as dict and return it
        # About png files --- talk to Min
        
        result_dict = SOMETHING()

        return MetricResult.build_from_output_bundle(
            #definition, format_cmec_output_bundle(annual_mean_global_mean_timeseries)
            definition, result_dict
        )
