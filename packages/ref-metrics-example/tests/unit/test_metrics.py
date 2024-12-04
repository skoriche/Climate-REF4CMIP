import pytest
from ref_core.datasets import DatasetCollection, MetricDataset, SourceDatasetType
from ref_core.metrics import MetricExecutionDefinition
from ref_metrics_example.example import GlobalMeanTimeseries, calculate_annual_mean_timeseries


@pytest.fixture
def metric_dataset(cmip6_data_catalog) -> MetricDataset:
    selected_dataset = cmip6_data_catalog[
        cmip6_data_catalog["instance_id"] == cmip6_data_catalog.instance_id.iloc[0]
    ]
    return MetricDataset(
        {
            SourceDatasetType.CMIP6: DatasetCollection(
                selected_dataset,
                "instance_id",
            )
        }
    )


def test_annual_mean(esgf_data_dir, metric_dataset):
    annual_mean = calculate_annual_mean_timeseries(metric_dataset["cmip6"].path.to_list())

    assert annual_mean.time.size == 286


def test_example_metric(tmp_path, metric_dataset, cmip6_data_catalog):
    metric = GlobalMeanTimeseries()
    ds = cmip6_data_catalog.groupby("instance_id").first()

    configuration = MetricExecutionDefinition(
        output_fragment=tmp_path,
        slug="global_mean_timeseries",
        metric_dataset=MetricDataset(
            {
                SourceDatasetType.CMIP6: DatasetCollection(ds, "instance_id"),
            }
        ),
    )

    result = metric.run(configuration)

    assert result.successful
    assert result.output_bundle.exists()
    assert result.output_bundle.is_file()
    assert result.output_bundle.name == "output.json"
