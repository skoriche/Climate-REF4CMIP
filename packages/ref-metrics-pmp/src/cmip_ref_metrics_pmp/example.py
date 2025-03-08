import json
import pathlib
from typing import Any

from cmip_ref_core.datasets import FacetFilter, SourceDatasetType
from cmip_ref_core.metrics import DataRequirement, Metric, MetricExecutionDefinition, MetricResult
from cmip_ref_core.pycmec.metric import CMECMetric
from cmip_ref_core.pycmec.output import CMECOutput
from cmip_ref_metrics_pmp.pmp_driver import execute_pmp_driver
from cmip_ref_metrics_pmp.registry import fetch_reference_data


def _remove_nested_key(data: dict[str, Any], key: str) -> dict[str, Any]:
    """
    Remove a nested key from a dictionary

    Parameters
    ----------
    data
        Dictionary to remove the key from
    key
        Key to remove

    Returns
    -------
        The dictionary with the key removed
    """
    if key in data:
        data.pop(key)
    for k, v in data.items():
        if isinstance(v, dict):
            data[k] = _remove_nested_key(v, key)
    return data


def process_json_result(
    json_filename: pathlib.Path, png_files: list[pathlib.Path], data_files: list[pathlib.Path]
) -> tuple[CMECOutput, CMECMetric]:
    """
    Process a PMP JSON result into the appropriate CMEC bundles

    Parameters
    ----------
    json_filename
        Filename of the JSON file that is written out by PMP
    png_files
        List of PNG files to be included in the output
    data_files
        List of data files to be included in the output

    Returns
    -------
        tuple of CMEC output and metric bundles
    """
    with open(json_filename) as fh:
        json_result = json.load(fh)

    cmec_output = CMECOutput.create_template()
    cmec_output["provenance"] = {**cmec_output["provenance"], **json_result["provenance"]}

    # Add the plots and data files
    for fname in png_files:
        cmec_output["plots"][fname.name] = {
            "filename": str(fname),
            "long_name": "Plot",
            "description": "Plot produced by the metric",
        }
    for fname in data_files:
        cmec_output["data"][fname.name] = {
            "filename": str(fname),
            "long_name": "Output data",
            "description": "Data produced by the metric",
        }

    cmec_metric = CMECMetric.create_template()
    cmec_metric["DIMENSIONS"] = {}
    dimensions = json_result["DIMENSIONS"]

    if "dimensions" in dimensions:
        # Merge the contents of inner "dimensions" into the parent "DIMENSIONS"
        dimensions.update(dimensions["dimensions"])
        del dimensions["dimensions"]

    if "statistic" in dimensions["json_structure"]:
        dimensions["json_structure"].remove("statistic")
        dimensions.pop("statistic")

    # Remove the "attributes" key from the RESULTS
    # This isn't standard CMEC output, but it is what PMP produces
    results = _remove_nested_key(json_result["RESULTS"], "attributes")

    cmec_metric["RESULTS"] = results
    cmec_metric["DIMENSIONS"] = dimensions

    if "provenance" in json_result:
        cmec_metric["provenance"] = json_result["provenance"]

    return CMECOutput(**cmec_output), CMECMetric(**cmec_metric)


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
                    facets={
                        "frequency": "mon",
                        "experiment_id": ("historical", "hist-GHG"),
                        "variable_id": "ts",
                    }
                ),
                # Ignore some experiments because they are not relevant
                # JL: This won't ever be triggered because the experiment_id filter more specific
                # Should this be ignoring all AerChemMIP experiments?
                FacetFilter(facets={"experiment_id": ("amip",)}, keep=False),
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
        input_datasets = definition.metric_dataset[SourceDatasetType.CMIP6]

        reference_dataset_name = "HadISST-1-1"

        source_id = input_datasets["source_id"].unique()[0]
        member_id = input_datasets["member_id"].unique()[0]

        try:
            execute_pmp_driver(
                driver_file="variability_mode/variability_modes_driver.py",
                parameter_file="pmp_param_MoV-PDO.py",
                model_files=input_datasets.path.to_list(),
                reference_name=reference_dataset_name,
                reference_paths=[fetch_reference_data(reference_dataset_name)],
                source_id=source_id,
                member_id=member_id,
                output_directory_path=str(definition.output_directory),
            )
        except Exception:
            return MetricResult.build_from_failure(definition)

        # Find the appropriate JSON bundle
        results_files = list(definition.output_directory.glob("*_cmec.json"))
        if len(results_files) != 1:
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
