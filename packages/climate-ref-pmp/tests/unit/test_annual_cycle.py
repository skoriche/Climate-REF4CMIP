import datetime

import pandas as pd
import pytest
from climate_ref_pmp import AnnualCycle, provider
from climate_ref_pmp.pmp_driver import _get_resource

from climate_ref.solver import extract_covered_datasets, solve_executions
from climate_ref_core.datasets import DatasetCollection, SourceDatasetType
from climate_ref_core.diagnostics import Diagnostic


def get_first_metric_match(data_catalog: pd.DataFrame, metric: Diagnostic) -> {pd.DataFrame}:
    # obs4mips requirement is first
    datasets = extract_covered_datasets(data_catalog, metric.data_requirements[1])
    assert len(datasets) > 0
    first_key = next(iter(datasets.keys()))

    return datasets[first_key]


def test_expected_executions():
    metric = AnnualCycle()
    data_catalog = {
        SourceDatasetType.CMIP6: pd.DataFrame(
            [
                ["ts", "ACCESS-ESM1-5", "historical", "r1i1p1f1", "mon"],
                ["ts", "ACCESS-ESM1-5", "ssp119", "r1i1p1f1", "mon"],
                ["ts", "ACCESS-ESM1-5", "historical", "r2i1p1f1", "mon"],
                ["pr", "ACCESS-ESM1-5", "historical", "r1i1p1f1", "mon"],
            ],
            columns=("variable_id", "source_id", "experiment_id", "member_id", "frequency"),
        ),
        SourceDatasetType.PMPClimatology: pd.DataFrame(
            [["ERA-5", "ts"], ["ERA-5", "pr"], ["GPCP-Monthly-3-2", "pr"]],
            columns=["source_id", "variable_id"],
        ),
    }
    executions = list(solve_executions(data_catalog, metric, provider=provider))
    assert len(executions) == 3

    # ts
    assert executions[0].datasets[SourceDatasetType.CMIP6].selector == (
        ("variable_id", "ts"),
        ("source_id", "ACCESS-ESM1-5"),
        ("experiment_id", "historical"),
        ("member_id", "r1i1p1f1"),
    )
    assert executions[0].datasets[SourceDatasetType.PMPClimatology].selector == (
        ("variable_id", "ts"),
        ("source_id", "ERA-5"),
    )

    # ts with different member_id
    assert executions[1].datasets[SourceDatasetType.CMIP6].selector == (
        ("variable_id", "ts"),
        ("source_id", "ACCESS-ESM1-5"),
        ("experiment_id", "historical"),
        ("member_id", "r2i1p1f1"),
    )
    assert executions[0].datasets[SourceDatasetType.PMPClimatology].selector == (
        ("variable_id", "ts"),
        ("source_id", "ERA-5"),
    )

    # pr
    assert executions[2].datasets[SourceDatasetType.CMIP6].selector == (
        ("variable_id", "pr"),
        ("source_id", "ACCESS-ESM1-5"),
        ("experiment_id", "historical"),
        ("member_id", "r1i1p1f1"),
    )
    assert executions[2].datasets[SourceDatasetType.PMPClimatology].selector == (
        ("variable_id", "pr"),
        ("source_id", "GPCP-Monthly-3-2"),
    )


@pytest.mark.parametrize(
    "variable_id,source_id,member_id",
    [
        ("pr", "ACCESS-ESM1-5", "r1i1p1f1"),
        ("ts", "MPI-ESM1-2-LR", "r2i2p1f1"),
    ],
)
def test_annual_cycle_metric(
    variable_id,
    source_id,
    member_id,
    cmip6_data_catalog,
    obs4mips_data_catalog,
    definition_factory,
    pdo_example_dir,
    provider,
):
    metric = AnnualCycle()
    metric._provider = provider

    expected_input_filename = cmip6_data_catalog["path"].iloc[0]
    expected_reference_filename = obs4mips_data_catalog["path"].iloc[0]

    definition = definition_factory(
        cmip6=DatasetCollection(
            pd.Series(
                {
                    "instance_id": "HadISST",
                    "source_id": source_id,
                    "variable_id": variable_id,
                    "experiment_id": "historical",
                    "member_id": member_id,
                    "path": expected_input_filename,
                }
            )
            .to_frame()
            .T,
            "instance_id",
        ),
        pmp_climatology=DatasetCollection(
            pd.Series(
                {
                    "instance_id": "HadISST",
                    "source_id": "HadISST-1-1",
                    "variable_id": variable_id,
                    "path": expected_reference_filename,
                }
            )
            .to_frame()
            .T,
            "instance_id",
        ),
    )
    # The output directory must exist
    output_dir = definition.output_directory
    driver_script = _get_resource(
        "pcmdi_metrics", "mean_climate/pcmdi_compute_climatologies.py", use_resources=False
    )
    parameter_file = _get_resource(
        "climate_ref_pmp.params", "pmp_param_annualcycle_1-clims.py", use_resources=True
    )
    datecode = datetime.datetime.now().strftime("%Y%m%d")

    definition.output_directory.mkdir(parents=True)

    result = metric.build_cmds(definition)

    assert len(result) == 2

    # Check the first command
    cmd = result[0]
    assert cmd == [
        "python",
        driver_script,
        "-p",
        parameter_file,
        "--vars",
        variable_id,
        "--infile",
        str(expected_input_filename),
        "--outfile",
        f"{output_dir}/{variable_id}_{source_id}_historical_{member_id}_clims.nc",
    ]

    # Check the second command
    driver_script = _get_resource("pcmdi_metrics", "mean_climate/mean_climate_driver.py", use_resources=False)
    parameter_file = _get_resource(
        "climate_ref_pmp.params", "pmp_param_annualcycle_2-metrics.py", use_resources=True
    )
    cmd = result[1]
    assert cmd == [
        "python",
        driver_script,
        "-p",
        parameter_file,
        "--vars",
        variable_id,
        "--custom_observations",
        f"{output_dir}/obs_dict.json",
        "--test_data_path",
        str(output_dir),
        "--test_data_set",
        source_id,
        "--realization",
        member_id,
        "--filename_template",
        f"{variable_id}_{source_id}_historical_{member_id}_clims.198101-200512.AC.v{datecode}.nc",
        "--metrics_output_path",
        str(output_dir),
        "--cmec",
    ]


def test_metric_run(mocker, provider):
    metric = AnnualCycle()
    metric.provider = mocker.MagicMock()
    metric.build_cmds = mocker.MagicMock(return_value=[["mocked_command"], ["mocked_command_2"]])
    metric.build_execution_result = mocker.MagicMock()

    metric.run("definition")

    metric.build_cmds.assert_called_once_with("definition")
    assert metric.provider.run.call_count == 2
    metric.build_execution_result.assert_called_once_with("definition")


def test_build_cmd_raises(provider):
    metric = AnnualCycle()
    with pytest.raises(NotImplementedError):
        metric.build_cmd("definition")
