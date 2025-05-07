from EnsoMetrics.EnsoComputeMetricsLib import ComputeCollection  # How can I import this?
from pcmdi_metrics.enso.lib import metrics_to_json
from pcmdi_metrics import resources

from loguru import logger

from climate_ref_core.datasets import FacetFilter, SourceDatasetType
from climate_ref_core.diagnostics import (
    CommandLineDiagnostic,
    DataRequirement,
    ExecutionDefinition,
    ExecutionResult,
)


class ENSO(CommandLineDiagnostic):
    """
    Calculate the ENSO performance metrics for a dataset
    """

    def __init__(self, metrics_collection: str) -> None:
        self.name = metrics_collection
        self.slug = metrics_collection.lower()
        self.metrics_collection = metrics_collection
        self.parameter_file = "pmp_param_enso.py"

        def _get_data_requirements(
            metrics_collection: str,
            extra_experiments: str | tuple[str, ...] | list[str] = (),
            remove_experiments: str | tuple[str, ...] | list[str] = (),
        ) -> tuple[DataRequirement, DataRequirement]:
            if metrics_collection == "ENSO_perf":
                model_variables = ("pr", "ts", "taux")
                obs_sources = ("GPCP", "ERA5")
            elif metrics_collection == "ENSO_tel":
                model_variables = ("pr", "ts")
                obs_sources = ("GPCP", "ERA5")
            elif metrics_collection == "ENSO_proc":
                model_variables = ("ts", "taux", "hfls", "hfss", "rlds", "rlus", "rsds", "rsus")
                obs_sources = ("GPCP", "ERA5", "TropFlux")
            else:
                raise ValueError(
                    f"Unknown metrics collection: {metrics_collection}. Valid options are: ENSO_perf, ENSO_tel, ENSO_proc"
                )

            obs_variables = model_variables

            filters = [
                FacetFilter(
                    facets={
                        "frequency": "mon",
                        "experiment_id": ("historical", *extra_experiments),
                        "variable_id": model_variables,
                    }
                )
            ]

            if remove_experiments:
                filters.append(FacetFilter(facets={"experiment_id": remove_experiments}, keep=False))

            return (
                DataRequirement(
                    source_type=SourceDatasetType.obs4MIPs,
                    filters=(FacetFilter(facets={"source_id": obs_sources, "variable_id": obs_variables}),),
                    group_by=("source_id", "variable_id"),
                ),
                DataRequirement(
                    source_type=SourceDatasetType.CMIP6,
                    filters=tuple(filters),
                    group_by=("source_id", "experiment_id", "member_id"),
                ),
            )

        self.data_requirements = _get_data_requirements(metrics_collection)

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
        mc_name = self.metrics_collection

        # ------------------------------------------------
        # Get the input datasets information for the model
        # ------------------------------------------------
        input_datasets = definition.datasets[SourceDatasetType.CMIP6]
        source_id = input_datasets["source_id"].unique()[0]
        experiment_id = input_datasets["experiment_id"].unique()[0]
        member_id = input_datasets["member_id"].unique()[0]
        variable_ids = input_datasets["variable_id"].unique()
        mod_run = f"{source_id}_{member_id}"

        dict_mod = {}

        for variable in variable_ids:
            dict_mod[mod_run][variable] = {
                "path + filename": list_files,
                "varname": variable,
                "path + filename_area": list_areacell,
                "areaname": list_name_area,
                "path + filename_landmask": list_landmask,
                "landmaskname": list_name_land,
            }

        # -------------------------------------------------------
        # Get the input datasets information for the observations
        # -------------------------------------------------------
        reference_dataset = definition.datasets[SourceDatasetType.obs4MIPs]
        reference_dataset_names = reference_dataset["source_id"].unique()

        dict_obs = {}

        for obs_name in reference_dataset_names:
            for variable in variable_ids:
                dict_obs[obs_name][variable] = {
                    "path + filename": list_files,
                    "varname": variable,
                    "path + filename_area": list_areacell,
                    "areaname": list_name_area,
                    "path + filename_landmask": list_landmask,
                    "landmaskname": list_name_land,
                }

        # Create input directory
        dictDatasets = {"model": dict_mod, "observations": dict_obs}

        # ------------------------------
        # Computes the metric collection
        # ------------------------------
        # https://github.com/CLIVAR-PRP/ENSO_metrics/blob/d50c2613354564a155e0fe0f637eb448dfd7c479/lib/EnsoComputeMetricsLib.py#L59
        
        logger.debug("\n### PMP ENSO: Compute the metric collection ###\n")

        dict_metric = {}
        dict_dive = {}

        dict_metric, dict_dive = ComputeCollection(
            mc_name,
            dictDatasets,
            mod_run,
            netcdf=True,
            netcdf_name=f"{mc_name}_{mod_run}_{experiment_id}",
            debug=True,
            obs_interpreter=True,
        )
        
        egg_pth = resources.resource_path()
        output_directory_path = str(definition.output_directory)
        json_name = f"{mc_name}_{mod_run}_{experiment_id}"

        # OUTPUT METRICS TO JSON FILE (per simulation)
        metrics_to_json(
            mc_name,
            dict_obs,
            dict_metric,
            dict_dive,
            egg_pth,
            output_directory_path,
            json_name,
            mod=self.source_id,
            run=self.member_id,
        )