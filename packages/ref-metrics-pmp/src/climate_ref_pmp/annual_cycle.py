import datetime
import json
from collections.abc import Iterable

from loguru import logger

from climate_ref_core.datasets import FacetFilter, SourceDatasetType
from climate_ref_core.metrics import (
    CommandLineMetric,
    DataRequirement,
    MetricExecutionDefinition,
    MetricExecutionResult,
)
from climate_ref_pmp.pmp_driver import build_glob_pattern, build_pmp_command, process_json_result


class AnnualCycle(CommandLineMetric):
    """
    Calculate the annual cycle for a dataset
    """

    def __init__(self) -> None:
        self.name = "Annual Cycle"
        self.slug = "annual-cycle"
        self.data_requirements = (
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
                    filters=(
                        FacetFilter(facets={"source_id": ("GPCP-Monthly-3-2",), "variable_id": ("pr",)}),
                    ),
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

        self.parameter_file_1 = "pmp_param_annualcycle_1-clims.py"
        self.parameter_file_2 = "pmp_param_annualcycle_2-metrics.py"

    def build_cmd(self, definition: MetricExecutionDefinition) -> Iterable[str]:
        """
        Build the command to run the metric

        Parameters
        ----------
        definition
            Definition of the metric execution

        Returns
        -------
            Command arguments to execute in the PMP environment
        """
        raise NotImplementedError("Function not required")

    def build_cmds(self, definition: MetricExecutionDefinition) -> list[list[str]]:
        """
        Build the command to run the metric

        Parameters
        ----------
        definition
            Definition of the metric execution

        Returns
        -------
            Command arguments to execute in the PMP environment
        """
        input_datasets = definition.metric_dataset[SourceDatasetType.CMIP6]
        source_id = input_datasets["source_id"].unique()[0]
        experiment_id = input_datasets["experiment_id"].unique()[0]
        member_id = input_datasets["member_id"].unique()[0]
        variable_id = input_datasets["variable_id"].unique()[0]

        logger.debug(f"input_datasets['source_id'].unique(): {input_datasets['source_id'].unique()}")
        logger.debug(f"input_datasets['experiment_id'].unique(): {input_datasets['experiment_id'].unique()}")
        logger.debug(f"input_datasets['member_id'].unique(): {input_datasets['member_id'].unique()}")
        logger.debug(f"input_datasets['variable_id'].unique(): {input_datasets['variable_id'].unique()}")

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
        logger.debug(f"input_datasets['variable_id']: {input_datasets['variable_id']}")

        logger.debug(f"source_id: {source_id}")
        logger.debug(f"experiment_id: {experiment_id}")
        logger.debug(f"member_id: {member_id}")
        logger.debug(f"variable_id: {variable_id}")

        reference_dataset = definition.metric_dataset[SourceDatasetType.PMPClimatology]
        reference_dataset_name = reference_dataset["source_id"].unique()[0]
        reference_dataset_path = reference_dataset.datasets.iloc[0]["path"]

        logger.debug(f"reference_dataset.datasets: {reference_dataset.datasets}")
        logger.debug(f"reference_dataset['source_id']: {reference_dataset['source_id']}")
        logger.debug(
            f"reference_dataset.datasets.iloc[0]['path']: {reference_dataset.datasets.iloc[0]['path']}"
        )

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
        # PART 2: Build the command to calculate metrics
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

    def build_metric_result(self, definition: MetricExecutionDefinition) -> MetricExecutionResult:
        """
        Build a metric result from the output of the PMP driver

        Parameters
        ----------
        definition
            Definition of the metric execution

        Returns
        -------
            Result of the metric execution
        """
        input_datasets = definition.metric_dataset[SourceDatasetType.CMIP6]
        variable_id = input_datasets["variable_id"].unique()[0]

        results_directory = definition.output_directory
        png_directory = results_directory / variable_id
        data_directory = results_directory / variable_id

        results_files = list(results_directory.glob("*_cmec.json"))
        if len(results_files) != 1:  # pragma: no cover
            return MetricExecutionResult.build_from_failure(definition)

        # Find the other outputs
        png_files = list(png_directory.glob("*.png"))
        data_files = list(data_directory.glob("*.nc"))

        cmec_output, cmec_metric = process_json_result(results_files[0], png_files, data_files)

        return MetricExecutionResult.build_from_output_bundle(
            definition,
            cmec_output_bundle=cmec_output,
            cmec_metric_bundle=cmec_metric,
        )

    def run(self, definition: MetricExecutionDefinition) -> MetricExecutionResult:
        """
        Run the metric on the given configuration.

        Parameters
        ----------
        definition : MetricExecutionDefinition
            The configuration to run the metric on.

        Returns
        -------
        :
            The result of running the metric.
        """
        logger.debug("PMP annual cycle run start")
        cmds = self.build_cmds(definition)

        runs = [self.provider.run(cmd) for cmd in cmds]
        logger.debug(f"runs: {runs}")

        return self.build_metric_result(definition)
