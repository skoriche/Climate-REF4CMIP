from collections.abc import Iterable

from cmip_ref_core.datasets import FacetFilter, SourceDatasetType
from cmip_ref_core.metrics import CommandLineMetric, DataRequirement, MetricExecutionDefinition, MetricResult
from cmip_ref_metrics_pmp.pmp_driver import build_pmp_command, process_json_result


class ExtratropicalModesOfVariability(CommandLineMetric):
    """
    Calculate the annual cycle for a dataset
    """

    def __init__(self, mode_id: str):
        self.mode_id = mode_id.upper()
        self.name = f"PMP Extratropical modes of variability {mode_id}"
        self.slug = f"pmp-extratropical-modes-of-variability-{mode_id.lower()}"

        def get_data_requirements(
            obs_source: str,
            obs_variable: str,
            cmip_variable: str,
            extra_experiments: str | tuple[str, ...] | list[str] = (),
            remove_experiments: str | tuple[str, ...] | list[str] = (),
        ) -> tuple[DataRequirement, DataRequirement]:
            filters = [
                FacetFilter(
                    facets={
                        "frequency": "mon",
                        "experiment_id": ("historical", "hist-GHG", "piControl", *extra_experiments),
                        "variable_id": cmip_variable,
                    }
                )
            ]

            if remove_experiments:
                filters.append(FacetFilter(facets={"experiment_id": remove_experiments}, keep=False))

            return (
                DataRequirement(
                    source_type=SourceDatasetType.obs4MIPs,
                    filters=(
                        FacetFilter(facets={"source_id": (obs_source,), "variable_id": (obs_variable,)}),
                    ),
                    group_by=("source_id", "variable_id"),
                ),
                DataRequirement(
                    source_type=SourceDatasetType.CMIP6,
                    filters=tuple(filters),
                    group_by=("source_id", "experiment_id", "variant_label", "member_id"),
                ),
            )

        if self.mode_id in ["PDO", "NPGO", "AMO"]:
            self.parameter_file = "pmp_param_MoV-ts.py"
            self.data_requirements = get_data_requirements(
                "HadISST-1-1", "ts", "ts", remove_experiments=("amip",)
            )
        elif self.mode_id in ["NAO", "NAM", "PNA", "NPO", "SAM"]:
            self.parameter_file = "pmp_param_MoV-psl.py"
            self.data_requirements = get_data_requirements("20CR", "psl", "psl", extra_experiments=("amip",))
        else:
            raise ValueError(
                f"Unknown mode_id '{self.mode_id}'. Must be one of PDO, NPGO, AMO, NAO, NAM, PNA, NPO, SAM"
            )

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
        input_datasets = definition.metric_dataset[SourceDatasetType.CMIP6]
        source_id = input_datasets["source_id"].unique()[0]
        experiment_id = input_datasets["experiment_id"].unique()[0]
        member_id = input_datasets["member_id"].unique()[0]

        print("input_datasets:", input_datasets)
        print("source_id:", source_id)
        print("experiment_id:", experiment_id)
        print("member_id:", member_id)

        reference_dataset = definition.metric_dataset[SourceDatasetType.obs4MIPs]
        reference_dataset_name = reference_dataset["source_id"].unique()[0]
        # reference_dataset_path = reference_dataset.datasets[0]["path"]
        reference_dataset_path = reference_dataset.datasets.iloc[0]["path"]

        print("reference_dataset:", reference_dataset)
        print("reference_dataset_name:", reference_dataset_name)
        print("reference_dataset_path:", reference_dataset_path)

        model_files = input_datasets.path.to_list()

        if len(model_files) != 1:
            # Have some logic to replace the dates in the filename with a wildcard
            raise NotImplementedError("Only one model file is supported at this time.")

        if isinstance(model_files, list):
            modpath = " ".join([str(p) for p in model_files])
        else:
            modpath = model_files

        if isinstance(reference_dataset_path, list):
            reference_data_path = " ".join([str(p) for p in reference_dataset_path])
        else:
            reference_data_path = reference_dataset_path

        # Build the command to run the PMP driver script
        params = {
            "driver_file": "variability_mode/variability_modes_driver.py",
            "parameter_file": self.parameter_file,
            "variability_mode": self.mode_id,
            "modpath": modpath,
            "modpath_lf": "none",
            "exp": experiment_id,
            "realization": member_id,
            "modnames": source_id,
            "reference_data_name": reference_dataset_name,
            "reference_data_path": reference_data_path,
            "results_dir": str(definition.output_directory),
            "cmec": None,
            "no_provenance": None,
        }

        # Add conditional parameters
        if self.mode_id in ["SAM"]:  # pragma: no cover
            params["osyear"] = 1950
            params["oeyear"] = 2005

        print("jwlee123_test-1 params:", params)

        development_mode = True

        if development_mode:  # pragma: no cover
            # Get current time in 'yyyymmdd-hhmm' format
            from datetime import datetime

            current_time = datetime.now().strftime("%Y%m%d")
            output_directory_path = f"/Users/lee1043/Documents/Research/REF/output/{current_time}/{self.slug}"
            params.update(
                {
                    "msyear": 2000,
                    "meyear": 2005,
                    "osyear": 2000,
                    "oeyear": 2005,
                    "results_dir": output_directory_path,
                }
            )

            print("jwlee123_test-2 params:", params)

            # definition.output_directory = output_directory_path

        print("jwlee123_test-3 done")

        # Pass the parameters using **kwargs
        return build_pmp_command(**params)

    def build_metric_result(self, definition: MetricExecutionDefinition) -> MetricResult:
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
        print("build_metric_result start")
        results_files = list(definition.output_directory.glob("*_cmec.json"))
        if len(results_files) != 1:  # pragma: no cover
            return MetricResult.build_from_failure(definition)

        # Find the other outputs
        png_files = list(definition.output_directory.glob("*.png"))
        data_files = list(definition.output_directory.glob("*.nc"))

        cmec_output, cmec_metric = process_json_result(results_files[0], png_files, data_files)

        return MetricResult.build_from_output_bundle(
            definition,
            cmec_output_bundle=cmec_output,
            cmec_metric_bundle=cmec_metric,
        )
