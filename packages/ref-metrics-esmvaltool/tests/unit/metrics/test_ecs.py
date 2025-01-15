from pathlib import Path

import numpy as np
import pandas
import xarray as xr
from cmip_ref_metrics_esmvaltool.metrics import EquilibriumClimateSensitivity
from cmip_ref_metrics_esmvaltool.recipe import load_recipe


def test_update_recipe():
    input_files = pandas.read_json(Path(__file__).parent / "input_files_ecs.json")
    print(input_files[["start_time"]])
    recipe = load_recipe("recipe_ecs.yml")
    EquilibriumClimateSensitivity().update_recipe(recipe, input_files)
    assert len(recipe["datasets"]) == 2
    assert len(recipe["diagnostics"]) == 1
    assert set(recipe["diagnostics"]["cmip6"]["variables"]) == {"tas", "rtnt"}


def test_format_output(tmp_path):
    ecs = xr.Dataset(
        data_vars={
            "ecs": (["dim0"], np.array([1.0])),
        },
        coords={
            "dataset": ("dim0", np.array([b"abc"])),
        },
    )
    result_dir = tmp_path
    subdir = result_dir / "work" / "cmip6" / "ecs"
    subdir.mkdir(parents=True)
    ecs.to_netcdf(subdir / "ecs.nc")

    output_bundle = EquilibriumClimateSensitivity().format_result(result_dir)

    assert isinstance(output_bundle, dict)
    assert output_bundle["RESULTS"]["abc"]["global"]["ecs"] == 1.0
