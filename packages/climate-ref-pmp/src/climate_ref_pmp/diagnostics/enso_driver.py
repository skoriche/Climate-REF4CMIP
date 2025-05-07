import argparse
import json
import os

from EnsoMetrics.EnsoCollectionsLib import defCollection
from EnsoMetrics.EnsoComputeMetricsLib import ComputeCollection
from EnsoPlots.EnsoMetricPlot import main_plotter
from loguru import logger
from pcmdi_metrics import resources
from pcmdi_metrics.enso.lib import metrics_to_json


def main():
    # Create an argument parser
    parser = argparse.ArgumentParser(description="A script that takes two inputs and processes them.")

    # Add arguments
    parser.add_argument("--input_json_path", type=str, help="JSON file path")
    parser.add_argument("--output_directory", type=str, help="output directory")

    # Parse the arguments
    args = parser.parse_args()

    # Access the inputs
    json_file = args.input_json_path
    output_directory = args.output_directory

    # Load information from JSON file
    with open(json_file) as f:
        dictDatasets = json.load(f)
    logger.debug(f"JSON file loaded: {json_file}")
    logger.debug(f"dictDatasets: {dictDatasets}")

    mc_name = dictDatasets["metricsCollection"]
    experiment_id = dictDatasets["experiment_id"]
    mod_run = list(dictDatasets["model"].keys())[0]

    # ------------------------------
    # Computes the metric collection
    # ------------------------------
    # https://github.com/CLIVAR-PRP/ENSO_metrics/blob/d50c2613354564a155e0fe0f637eb448dfd7c479/lib/EnsoComputeMetricsLib.py#L59

    logger.debug("\n### PMP ENSO: Compute the metric collection ###\n")

    dict_metric = {}
    dict_dive = {}

    pattern = f"{mc_name}_{mod_run}_{experiment_id}"

    dict_metric, dict_dive = ComputeCollection(
        mc_name,
        dictDatasets,
        mod_run,
        netcdf=True,
        netcdf_name=pattern,
        debug=True,
        obs_interpreter=True,
    )

    egg_pth = resources.resource_path()
    mod = mod_run.split("_")[0]
    run = mod_run.split("_")[1]

    dict_obs = dictDatasets["observations"]

    # OUTPUT METRICS TO JSON FILE (per simulation)
    metrics_to_json(
        mc_name,
        dict_obs,
        dict_metric,
        dict_dive,
        egg_pth,
        output_directory,
        pattern,
        mod=mod,
        run=run,
    )

    # Plot
    with open(os.path.join(output_directory, f"{pattern}.json")) as ff:
        data_json = json.load(ff)["RESULTS"]["model"][mod][run]

    _plot_enso(
        mc_name,
        mod,
        run,
        experiment_id,
        os.path.join(output_directory, f"{pattern}.nc"),
        output_directory,
        data_json,
    )


def _plot_enso(mc_name, mod, run, exp, path_in_nc, path_out, data_json):
    metrics = sorted(defCollection(mc_name)["metrics_list"].keys(), key=lambda v: v.upper())
    print("metrics:", metrics)

    pattern = "_".join([mc_name, mod, exp, run])

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
