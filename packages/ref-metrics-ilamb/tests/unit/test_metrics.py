import ilamb3
import pytest
from cmip_ref_metrics_ilamb.example import GlobalMeanTimeseries, calculate_global_mean_timeseries
from cmip_ref_metrics_ilamb.standard import ILAMBStandard, _set_ilamb3_options

from cmip_ref_core.datasets import DatasetCollection, MetricDataset, SourceDatasetType


@pytest.fixture
def metric_dataset(cmip6_data_catalog) -> MetricDataset:
    selected_dataset = cmip6_data_catalog[
        cmip6_data_catalog["instance_id"]
        == "CMIP6.ScenarioMIP.CSIRO.ACCESS-ESM1-5.ssp126.r1i1p1f1.Amon.tas.gn.v20210318"
    ]
    return MetricDataset(
        {
            SourceDatasetType.CMIP6: DatasetCollection(
                selected_dataset,
                "instance_id",
            )
        }
    )


def test_annual_mean(metric_dataset):
    annual_mean = calculate_global_mean_timeseries(metric_dataset["cmip6"].path.to_list())
    assert annual_mean.time.size == 132


def test_example_metric(cmip6_data_catalog, mocker, definition_factory):
    metric = GlobalMeanTimeseries()
    ds = cmip6_data_catalog.groupby("instance_id").first()

    mock_calc = mocker.patch("cmip_ref_metrics_ilamb.example.calculate_global_mean_timeseries")
    mock_calc.return_value.attrs.__getitem__.return_value = "ABC"

    definition = definition_factory(cmip6=DatasetCollection(ds, "instance_id"))

    result = metric.run(definition)

    assert mock_calc.call_count == 1

    assert str(result.output_bundle_filename) == "output.json"

    output_bundle_path = definition.output_directory / result.output_bundle_filename

    assert result.successful
    assert output_bundle_path.exists()
    assert output_bundle_path.is_file()

    assert str(result.metric_bundle_filename) == "metric.json"

    metric_bundle_path = definition.output_directory / result.metric_bundle_filename

    assert result.successful
    assert metric_bundle_path.exists()
    assert metric_bundle_path.is_file()


def test_standard_site(cmip6_data_catalog, definition_factory):
    metric = ILAMBStandard(registry_file="test.txt", sources={"tas": "test/Site/tas.nc"})
    ds = (
        cmip6_data_catalog[
            (cmip6_data_catalog["experiment_id"] == "historical")
            & (cmip6_data_catalog["variable_id"] == "tas")
        ]
        .groupby("instance_id")
        .first()
    )

    definition = definition_factory(cmip6=DatasetCollection(ds, "instance_id"))
    definition.output_directory.mkdir(parents=True, exist_ok=True)

    result = metric.run(definition)

    assert str(result.output_bundle_filename) == "output.json"

    output_bundle_path = definition.output_directory / result.output_bundle_filename

    assert result.successful
    assert output_bundle_path.exists()
    assert output_bundle_path.is_file()

    assert str(result.metric_bundle_filename) == "metric.json"

    metric_bundle_path = definition.output_directory / result.metric_bundle_filename

    assert result.successful
    assert metric_bundle_path.exists()
    assert metric_bundle_path.is_file()


def test_standard_grid(cmip6_data_catalog, definition_factory):
    metric = ILAMBStandard(
        registry_file="test.txt", sources={"gpp": "test/Grid/gpp.nc"}, relationships={"pr": "test/Grid/pr.nc"}
    )
    grp = cmip6_data_catalog[
        (cmip6_data_catalog["experiment_id"] == "historical")
        & ((cmip6_data_catalog["variable_id"] == "gpp") | (cmip6_data_catalog["variable_id"] == "pr"))
    ].groupby(["source_id", "member_id", "grid_label"])
    _, ds = next(iter(grp))

    definition = definition_factory(cmip6=DatasetCollection(ds, "instance_id"))
    definition.output_directory.mkdir(parents=True, exist_ok=True)

    result = metric.run(definition)

    assert str(result.output_bundle_filename) == "output.json"

    output_bundle_path = definition.output_directory / result.output_bundle_filename

    assert result.successful
    assert output_bundle_path.exists()
    assert output_bundle_path.is_file()

    assert str(result.metric_bundle_filename) == "metric.json"

    metric_bundle_path = definition.output_directory / result.metric_bundle_filename

    assert result.successful
    assert metric_bundle_path.exists()
    assert metric_bundle_path.is_file()


def test_standard_fail():
    with pytest.raises(ValueError):
        ILAMBStandard(registry_file="test.txt", sources={"gpp": "test/Grid/gpp.nc", "pr": "test/Grid/pr.nc"})


def test_options():
    _set_ilamb3_options("ilamb.txt")
    assert set(["global", "tropical"]).issubset(ilamb3.conf["regions"])
