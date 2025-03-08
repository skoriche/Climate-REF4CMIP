from cmip_ref_metrics_pmp.pmp_driver import process_json_result

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
