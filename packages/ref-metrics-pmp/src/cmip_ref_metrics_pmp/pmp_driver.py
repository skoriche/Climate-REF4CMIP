import importlib.metadata
import importlib.resources
import json
import pathlib
from typing import Any

from loguru import logger

from cmip_ref_core.pycmec.metric import CMECMetric
from cmip_ref_core.pycmec.output import CMECOutput


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
    print("process_json_result called with:", json_filename, png_files, data_files)

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

    if "dimensions" in dimensions:  # pragma: no branch
        # Merge the contents of inner "dimensions" into the parent "DIMENSIONS"
        dimensions.update(dimensions["dimensions"])
        del dimensions["dimensions"]

    if "statistic" in dimensions["json_structure"]:  # pragma: no branch
        dimensions["json_structure"].remove("statistic")
        dimensions.pop("statistic")

    # Remove the "attributes" key from the RESULTS
    # This isn't standard CMEC output, but it is what PMP produces
    results = _remove_nested_key(json_result["RESULTS"], "attributes")

    cmec_metric["RESULTS"] = results
    cmec_metric["DIMENSIONS"] = dimensions

    if "provenance" in json_result:  # pragma: no branch
        cmec_metric["provenance"] = json_result["provenance"]

    print("process_json_result returning:", cmec_output, cmec_metric)

    return CMECOutput(**cmec_output), CMECMetric(**cmec_metric)


def _get_resource(package: str, resource_name: str | pathlib.Path, use_resources: bool) -> str:
    """
    Get the path to a resource within the pcmdi_metric package without importing.

    Parameters
    ----------
    package: str
        Python package name if use_resources is True, otherwise the distribution name
        (the pypi package name).
    resource_name : str
        The resource path relative to the package.
    use_resources : bool
        If True, use the importlib.resources API, otherwise use importlib.metadata.

        importlib.resources is the preferred way to access resources because it handles
        packages which have been editably installed,
        but it implictly imports the package.

        Whereas `importlib.metadata` uses the package metadata to resolve the location of a resource.

    Returns
    -------
        The full path to the target resource.
    """
    if use_resources:
        resource_path = str(importlib.resources.files(package) / str(resource_name))
    else:
        distribution = importlib.metadata.distribution(package)
        resource_path = str(distribution.locate_file(pathlib.Path(package) / resource_name))
    if not pathlib.Path(resource_path).exists():
        raise FileNotFoundError(f"Resource {resource_name} not found in {package} package.")
    return str(resource_path)


def build_pmp_command(
    driver_file: str,
    parameter_file: str,
    **kwargs: dict[str, str | int | float | list[str]],
) -> list[str]:
    """
    Run a PMP driver script via a conda environment

    This function runs a PMP driver script using a specific conda environment.
    The driver script is responsible for running the PMP metrics and producing output.
    The output consists of a JSON file that contains the results of the PMP metrics,
    and a set of PNG and data files that are produced by the metrics.

    Parameters
    ----------
    driver_file
        Filename of the PMP driver script to run
    parameter_file
        Filename of the parameter file to use
    kwargs
        Additional arguments to pass to the driver script
    """
    # Note this uses the driver script from the REF env *not* the PMP conda env
    _driver_script = _get_resource("pcmdi_metrics", driver_file, use_resources=False)
    _parameter_file = _get_resource("cmip_ref_metrics_pmp.params", parameter_file, use_resources=True)

    # Run the driver script inside the PMP conda environment
    cmd = [
        "python",
        _driver_script,
        "-p",
        _parameter_file,
    ]

    # Loop through additional arguments if they exist
    if kwargs:  # pragma: no cover
        for key, value in kwargs.items():
            if value:
                cmd.extend([f"--{key}", str(value)])
            else:
                cmd.extend([f"--{key}"])

    logger.info("-- PMP command to run --")
    logger.info("[PMP] Command to run:", " ".join(map(str, cmd)))
    logger.info("[PMP] Command generation for the driver completed.")

    return cmd
