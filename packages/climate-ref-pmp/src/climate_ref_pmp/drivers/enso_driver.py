import argparse
import json
import os

from collections import defaultdict
from loguru import logger

from pcmdi_metrics import resources  # isort:skip
from pcmdi_metrics.enso.lib import metrics_to_json  # isort:skip
from pcmdi_metrics.io import StringConstructor  # isort:skip

from EnsoMetrics.EnsoCollectionsLib import defCollection  # isort:skip
from EnsoMetrics.EnsoComputeMetricsLib import ComputeCollection  # isort:skip
from EnsoPlots.EnsoMetricPlot import main_plotter  # isort:skip


def main():
    """
    Run the ENSO metrics collection and plotting.

    This script is designed to be run from the command line.
    It takes two command line arguments:
    1. input_json_path: Path to the JSON file containing the datasets.
    2. output_directory: Directory where the output files will be saved.
    """
    print("### PMP ENSO: Compute the metric collection ###\n")

    # Create an argument parser
    parser = argparse.ArgumentParser(description="A script that takes two inputs and processes them.")

    # Add arguments
    parser.add_argument("--metrics_collection", type=str, help="metrics collection")
    parser.add_argument("--experiment_id", type=str, help="experiment id")
    parser.add_argument("--input_json_path", type=str, help="JSON file path")
    parser.add_argument("--output_directory", type=str, help="output directory")

    # Parse the arguments
    args = parser.parse_args()

    # Access the inputs
    mc_name = args.metrics_collection
    experiment_id = args.experiment_id
    json_file = args.input_json_path
    output_directory = args.output_directory
    
    # Make sure the output directory exists
    os.makedirs(output_directory, exist_ok=True)

    # Load information from JSON file
    with open(json_file) as f:
        dictDatasets = json.load(f)

    print(f"JSON file loaded: {json_file}")
    print(f"dictDatasets: {dictDatasets}")

    logger.debug(f"JSON file loaded: {json_file}")
    logger.debug(f"dictDatasets: {dictDatasets}")

    mod_run = list(dictDatasets["model"].keys())[0]
    mod = mod_run.split("_")[0]
    run = mod_run.split("_")[1]

    print(f"mod: {mod}")
    print(f"run: {run}")

    # ------------------------------
    # Computes the metric collection
    # ------------------------------
    # https://github.com/CLIVAR-PRP/ENSO_metrics/blob/d50c2613354564a155e0fe0f637eb448dfd7c479/lib/EnsoComputeMetricsLib.py#L59

    logger.debug("\n### PMP ENSO: Compute the metric collection ###\n")

    # Use defaultdict to automatically handle nested dictionary initialization
    dict_metric = defaultdict(dict)
    dict_dive = defaultdict(dict)

    # Compose the pattern string
    pattern = f"{mc_name}_{mod_run}_{experiment_id}"

    # Debug print
    print(f"pattern: {pattern}")
    print(f"mod_run: {mod_run}")
    print(f"experiment_id: {experiment_id}")
    print(f"mc_name: {mc_name}")
    print(f"dictDatasets: {dictDatasets}")
    print(f"output_directory: {output_directory}")

    # Call the function and unpack the results
    metrics, dive_results = ComputeCollection(
        mc_name,
        dictDatasets,
        mod_run,
        netcdf=True,
        netcdf_name=os.path.join(output_directory, pattern),
        obs_interpreter=True,
        debug=True,
    )

    # Store the results in the dictionaries
    dict_metric[mod][run] = metrics
    dict_dive[mod][run] = dive_results

    print(f"dict_metric: {dict_metric}")
    print(f"dict_dive: {dict_dive}")

    logger.debug(f"dict_metric: {dict_metric}")
    logger.debug(f"dict_dive: {dict_dive}")

    egg_pth = resources.resource_path()

    dict_obs = dictDatasets["observations"]

    # OUTPUT METRICS TO JSON FILE (per simulation)
    metrics_to_json(
        mc_name,
        dict_obs,
        dict_metric,
        dict_dive,
        egg_pth,
        StringConstructor(output_directory),
        pattern,
        mod=mod,
        run=run,
    )

    # Plot
    with open(os.path.join(output_directory, f"{pattern}.json")) as ff:
        data_json = json.load(ff)["RESULTS"]["model"][mod][run]

    plot_enso(
        mc_name,
        mod,
        run,
        experiment_id,
        output_directory,
        output_directory,
        data_json,
    )


def plot_enso(mc_name, mod, run, exp, path_in_nc, path_out, data_json):
    """
    Plot the ENSO metrics collection.

    Parameters
    ----------
    mc_name : str
        Name of the metrics collection.
    mod : str
        Model name.
    run : str
        Run name.
    exp : str
        Experiment name.
    path_in_nc : str
        Path to the input NetCDF file.
    path_out : str
        Path to the output directory.
    data_json : dict
        Data loaded from the JSON file.
    """
    metrics = sorted(defCollection(mc_name)["metrics_list"].keys(), key=lambda v: v.upper())
    print("metrics:", metrics)

    pattern = "_".join([mc_name, mod, run, exp])

    for met in metrics:
        print("met:", met)
        # get NetCDF file name
        filename_nc = os.path.join(path_in_nc, pattern + "_" + met + ".nc")
        print("filename_nc:", filename_nc)
        if os.path.exists(filename_nc):
            # get diagnostic values for the given model and observations
            if mc_name == "ENSO_tel" and "Map" in met:
                dict_dia = data_json["value"][met + "Corr"]["diagnostic"]
                diagnostic_values = dict((key1, None) for key1 in dict_dia.keys())
                diagnostic_units = ""
            else:
                dict_dia = data_json["value"][met]["diagnostic"]
                diagnostic_values = dict((key1, dict_dia[key1]["value"]) for key1 in dict_dia.keys())
                diagnostic_units = data_json["metadata"]["metrics"][met]["diagnostic"]["units"]
            # get metric values computed with the given model and observations
            if mc_name == "ENSO_tel" and "Map" in met:
                list1, list2 = (
                    [met + "Corr", met + "Rmse"],
                    [
                        "diagnostic",
                        "metric",
                    ],
                )
                dict_met = data_json["value"]
                metric_values = dict(
                    (
                        key1,
                        {mod: [dict_met[su][ty][key1]["value"] for su, ty in zip(list1, list2)]},
                    )
                    for key1 in dict_met[list1[0]]["metric"].keys()
                )
                metric_units = [data_json["metadata"]["metrics"][su]["metric"]["units"] for su in list1]
            else:
                dict_met = data_json["value"][met]["metric"]
                metric_values = dict((key1, {mod: dict_met[key1]["value"]}) for key1 in dict_met.keys())
                metric_units = data_json["metadata"]["metrics"][met]["metric"]["units"]
            # figure name
            figure_name = "_".join([mc_name, mod, exp, run, met])
            print("figure_name:", figure_name)

            main_plotter(
                mc_name,
                met,
                mod,
                exp,
                filename_nc,
                diagnostic_values,
                diagnostic_units,
                metric_values,
                metric_units,
                member=run,
                path_png=path_out,
                name_png=figure_name,
            )

            print("figure plotting done")

        else:
            print("file not found:", filename_nc)


if __name__ == "__main__":
    main()
