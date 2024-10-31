from pathlib import Path

import pytest
from ref_core.metrics import Configuration, TriggerInfo
from ref_metrics_example.example import ExampleMetric, calculate_annual_mean_timeseries


@pytest.fixture
def test_dataset(esgf_data_dir) -> Path:
    return (
        esgf_data_dir
        / "CMIP6"
        / "ScenarioMIP"
        / "CSIRO"
        / "ACCESS-ESM1-5"
        / "ssp245"
        / "r1i1p1f1"
        / "Amon"
        / "tas"
        / "gn"
        / "v20191115"
    )


def test_annual_mean(esgf_data_dir):
    input_files = list(
        (
            esgf_data_dir
            / "CMIP6"
            / "ScenarioMIP"
            / "CSIRO"
            / "ACCESS-ESM1-5"
            / "ssp245"
            / "r1i1p1f1"
            / "Amon"
            / "tas"
            / "gn"
            / "v20191115"
        ).glob("*.nc")
    )
    annual_mean = calculate_annual_mean_timeseries(input_files)

    assert annual_mean.time.size == 86


def test_example_metric(tmp_path, test_dataset):
    metric = ExampleMetric()

    configuration = Configuration(
        output_directory=tmp_path,
    )

    result = metric.run(configuration, trigger=TriggerInfo(dataset=test_dataset))

    assert result.successful
    assert result.output_bundle.exists()
    assert result.output_bundle.is_file()
    assert result.output_bundle.name == "output.json"
