import datetime
import json
from collections.abc import Iterable
from typing import Any

from loguru import logger

from climate_ref_core.datasets import FacetFilter, SourceDatasetType
from climate_ref_core.diagnostics import (
    CommandLineDiagnostic,
    DataRequirement,
    ExecutionDefinition,
    ExecutionResult,
)
from climate_ref_pmp.pmp_driver import build_glob_pattern, build_pmp_command, process_json_result


class AnnualCycle(CommandLineDiagnostic):
    """
    Calculate the annual cycle for a dataset
    """

    name = "Annual Cycle"
    slug = "annual-cycle"
    facets = ("model", "realization", "reference", "mode", "season", "method", "statistic")
    data_requirements = (
        # Surface temperature
        (
            DataRequirement(
                source_type=SourceDatasetType.PMPClimatology,
                filters=(FacetFilter(facets={"source_id": ("ERA-5",), "variable_id": ("ts",)}),),
                group_by=("variable_id", "source_id"),
            ),
            DataRequirement(
                source_type=SourceDatasetType.CMIP6,
                filters=(
                    FacetFilter(
                        facets={
                            "frequency": "mon",
                            "experiment_id": ("amip", "historical", "hist-GHG", "piControl"),
                            "variable_id": ("ts",),
                        }
                    ),
                ),
                group_by=("variable_id", "source_id", "experiment_id", "member_id"),
            ),
        ),
        # Precipitation
        (
            DataRequirement(
                source_type=SourceDatasetType.PMPClimatology,
                filters=(FacetFilter(facets={"source_id": ("GPCP-Monthly-3-2",), "variable_id": ("pr",)}),),
                group_by=("variable_id", "source_id"),
            ),
            DataRequirement(
                source_type=SourceDatasetType.CMIP6,
                filters=(
                    FacetFilter(
                        facets={
                            "frequency": "mon",
                            "experiment_id": ("amip", "historical", "hist-GHG", "piControl"),
                            "variable_id": ("pr",),
                        }
                    ),
                ),
                group_by=("variable_id", "source_id", "experiment_id", "member_id"),
            ),
        ),
    )

    def __init__(self) -> None:
        self.parameter_file_1 = "pmp_param_annualcycle_1-clims.py"
        self.parameter_file_2 = "pmp_param_annualcycle_2-metrics.py"

    def build_cmd(self, definition: ExecutionDefinition) -> Iterable[str]:
        """
        Build the command to run the diagnostic

        Parameters
        ----------
        definition
            Definition of the diagnostic execution

        Returns
        -------
            Command arguments to execute in the PMP environment
        """
        raise NotImplementedError("Function not required")

    def build_cmds(self, definition: ExecutionDefinition) -> list[list[str]]:
        """
        Build the command to run the diagnostic

        Parameters
        ----------
        definition
            Definition of the diagnostic execution

        Returns
        -------
            Command arguments to execute in the PMP environment
        """
        input_datasets = definition.datasets[SourceDatasetType.CMIP6]
        reference_datasets = definition.datasets[SourceDatasetType.PMPClimatology]

        selector = input_datasets.selector_dict()
        reference_selector = reference_datasets.selector_dict()

        source_id = selector["source_id"]
        experiment_id = selector["experiment_id"]
        member_id = selector["member_id"]
        variable_id = selector["variable_id"]

        logger.debug(f"selector: {selector}")
        logger.debug(f"reference selector: {reference_selector}")

        model_files_raw = input_datasets.path.to_list()
        if len(model_files_raw) == 1:
            model_files = model_files_raw[0]  # If only one file, use it directly
        elif len(model_files_raw) > 1:
            model_files = build_glob_pattern(model_files_raw)  # If multiple files, build a glob pattern
        else:
            raise ValueError("No model files found")

        logger.debug("build_cmd start")

        logger.debug(f"input_datasets: {input_datasets}")
        logger.debug(f"input_datasets.keys(): {input_datasets.keys()}")

        reference_dataset_name = reference_datasets["source_id"].unique()[0]
        reference_dataset_path = reference_datasets.datasets.iloc[0]["path"]

        logger.debug(f"reference_dataset.datasets: {reference_datasets.datasets}")

        logger.debug(f"reference_dataset_name: {reference_dataset_name}")
        logger.debug(f"reference_dataset_path: {reference_dataset_path}")

        output_directory_path = str(definition.output_directory)

        cmds = []

        # ----------------------------------------------
        # PART 1: Build the command to get climatologies
        # ----------------------------------------------
        # Model
        data_name = f"{source_id}_{experiment_id}_{member_id}"
        data_path = model_files
        params = {
            "driver_file": "mean_climate/pcmdi_compute_climatologies.py",
            "parameter_file": self.parameter_file_1,
            "vars": variable_id,
            "infile": data_path,
            "outfile": f"{output_directory_path}/{variable_id}_{data_name}_clims.nc",
        }

        cmds.append(build_pmp_command(**params))

        # ----------------------------------------------
        # PART 2: Build the command to calculate diagnostics
        # ----------------------------------------------

        # Reference
        obs_dict = {
            variable_id: {
                reference_dataset_name: {
                    "template": reference_dataset_path,
                },
                "default": reference_dataset_name,
            }
        }

        # Generate a JSON file based on the obs_dict
        with open(f"{output_directory_path}/obs_dict.json", "w") as f:
            json.dump(obs_dict, f)

        date = datetime.datetime.now().strftime("%Y%m%d")

        params = {
            "driver_file": "mean_climate/mean_climate_driver.py",
            "parameter_file": self.parameter_file_2,
            "vars": variable_id,
            "custom_observations": f"{output_directory_path}/obs_dict.json",
            "test_data_path": output_directory_path,
            "test_data_set": source_id,
            "realization": member_id,
            "filename_template": f"{variable_id}_{data_name}_clims.198101-200512.AC.v{date}.nc",
            "metrics_output_path": output_directory_path,
            "cmec": "",
        }

        cmds.append(build_pmp_command(**params))

        return cmds

    def build_execution_result(self, definition: ExecutionDefinition) -> ExecutionResult:
        """
        Build a diagnostic result from the output of the PMP driver

        Parameters
        ----------
        definition
            Definition of the diagnostic execution

        Returns
        -------
            Result of the diagnostic execution
        """
        input_datasets = definition.datasets[SourceDatasetType.CMIP6]
        variable_id = input_datasets["variable_id"].unique()[0]

        results_directory = definition.output_directory
        png_directory = results_directory / variable_id
        data_directory = results_directory / variable_id

        logger.debug(f"results_directory: {results_directory}")
        logger.debug(f"png_directory: {png_directory}")
        logger.debug(f"data_directory: {data_directory}")

        # Find the executions file
        results_files = list(results_directory.glob("*_cmec.json"))
        if len(results_files) != 1:  # pragma: no cover
            return ExecutionResult.build_from_failure(definition)
        else:
            results_file = results_files[0]
            logger.debug(f"results_file: {results_file}")

        # Rewrite executions file for compatibility
        with open(results_file) as f:
            results = json.load(f)
            results_transformed = _transform_results(results)

        # Get the stem (filename without extension)
        stem = results_file.stem

        # Create the new filename
        results_file_transformed = results_file.with_name(f"{stem}_transformed.json")

        with open(results_file_transformed, "w") as f:
            # Write the transformed executions back to the file
            json.dump(results_transformed, f, indent=4)
            logger.debug(f"Transformed executions written to {results_file_transformed}")

        # Find the other outputs
        png_files = list(png_directory.glob("*.png"))
        data_files = list(data_directory.glob("*.nc"))

        cmec_output_bundle, cmec_metric_bundle = process_json_result(
            results_file_transformed, png_files, data_files
        )

        # Add missing dimensions to the output
        # TODO: Add reference source id
        selectors = input_datasets.selector
        cmec_metric_bundle = cmec_metric_bundle.prepend_dimensions({key: value for key, value in selectors})

        return ExecutionResult.build_from_output_bundle(
            definition,
            cmec_output_bundle=cmec_output_bundle,
            cmec_metric_bundle=cmec_metric_bundle,
        )

    def run(self, definition: ExecutionDefinition) -> ExecutionResult:
        """
        Run the diagnostic on the given configuration.

        Parameters
        ----------
        definition : ExecutionDefinition
            The configuration to run the diagnostic on.

        Returns
        -------
        :
            The result of running the diagnostic.
        """
        cmds = self.build_cmds(definition)

        runs = [self.provider.run(cmd) for cmd in cmds]
        logger.debug(f"runs: {runs}")

        return self.build_execution_result(definition)


def _transform_results(data: dict[str, Any]) -> dict[str, Any]:
    """
    Transform the executions dictionary to match the expected structure.

    Parameters
    ----------
    data : dict
        The original executions dictionary.

    Returns
    -------
    dict
        The transformed executions dictionary.
    """
    # Remove the model, reference, rip dimensions
    # These are later replaced with a REF-specific naming convention
    models = list(data["DIMENSIONS"]["model"].keys())
    references = list(data["DIMENSIONS"]["reference"].keys())
    realizations = list(data["DIMENSIONS"]["rip"].keys())

    assert len(models) == 1, "Multiple models found in the data"
    assert len(references) == 1, "Multiple references found in the data"
    assert len(realizations) == 1, "Multiple realizations found in the data"

    inner_results = data["RESULTS"][models[0]][references[0]][realizations[0]]

    # TODO: replace this with the ability to capture series
    # Remove the "CalendarMonths" key from the nested structure
    for region, region_values in inner_results.items():
        for stat, stat_values in region_values.items():
            if "CalendarMonths" in stat_values:
                stat_values.pop("CalendarMonths")

    # Remove the "CalendarMonths" key from the nested structure in "DIMENSIONS"
    data["DIMENSIONS"]["season"].pop("CalendarMonths")
    # drop the first 3 elements of the "json_structure" list
    data["DIMENSIONS"]["json_structure"] = data["DIMENSIONS"]["json_structure"][3:]
    return data
