import pytest
from cmip_ref_metrics_pmp.pmp_driver import build_pmp_command, process_json_result

from cmip_ref_core.pycmec.metric import CMECMetric
from cmip_ref_core.pycmec.output import CMECOutput


def test_process_json_result(pdo_example_dir):
    json_file = (
        pdo_example_dir
        / "var_mode_PDO_EOF1_stat_cmip5_historical_mo_atm_ACCESS-ESM1-5_r1i1p1f1_2000-2005_cmec.json"
    )
    png_files = [pdo_example_dir / "pdo.png"]
    data_files = [pdo_example_dir / "pdo.nc"]

    cmec_output, cmec_metric = process_json_result(json_file, png_files, data_files)

    assert CMECMetric.model_validate(cmec_metric)
    assert CMECOutput.model_validate(cmec_output)
    assert len(cmec_metric.RESULTS)
    assert cmec_metric.DIMENSIONS.root["json_structure"] == [
        "model",
        "realization",
        "reference",
        "mode",
        "season",
        "method",
    ]


def test_execute_missing_driver():
    with pytest.raises(
        FileNotFoundError,
        match="Resource variability_mode/missing.py not found in pcmdi_metrics package.",
    ):
        build_pmp_command(
            driver_file="variability_mode/missing.py",
            parameter_file="pmp_param_MoV-ts.py",
            model_files=["model1.nc"],
            reference_name="HadISST-1-1",
            reference_paths=["reference.nc"],
            source_id="source_id",
            member_id="member_id",
            output_directory_path="output",
            experiment_id="historical",
        )


def test_execute_missing_parameter():
    with pytest.raises(
        FileNotFoundError,
        match="Resource pmp_missing.py not found in cmip_ref_metrics_pmp.params package.",
    ):
        build_pmp_command(
            driver_file="variability_mode/variability_modes_driver.py",
            parameter_file="pmp_missing.py",
            model_files=["model1.nc"],
            reference_name="HadISST-1-1",
            reference_paths=["reference.nc"],
            source_id="source_id",
            member_id="member_id",
            output_directory_path="output",
            experiment_id="historical",
        )


def test_execute_more_than_one_model():
    with pytest.raises(NotImplementedError, match="Only one model file is supported"):
        build_pmp_command(
            driver_file="variability_mode/variability_modes_driver.py",
            parameter_file="pmp_param_MoV-ts.py",
            model_files=["model1.nc", "model2.nc"],
            reference_name="HadISST-1-1",
            reference_paths=["reference.nc"],
            source_id="source_id",
            member_id="member_id",
            output_directory_path="output",
            experiment_id="historical",
        )
