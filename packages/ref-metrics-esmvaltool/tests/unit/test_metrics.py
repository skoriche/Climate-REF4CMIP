import cmip_ref_metrics_esmvaltool
import pytest

import cmip_ref_core.providers
from cmip_ref_core.datasets import DatasetCollection, MetricDataset, SourceDatasetType


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


def test_example_metric(mocker, tmp_path, metric_dataset, cmip6_data_catalog, definition_factory):
    provider = cmip_ref_metrics_esmvaltool.provider
    provider.prefix = tmp_path / "conda"
    provider.prefix.mkdir()
    provider._conda_exe = provider.prefix / "mock_micromamba"
    provider._conda_exe.touch()
    provider.env_path.mkdir()

    metric = next(
        metric for metric in provider.metrics() if metric.slug == "esmvaltool-global-mean-timeseries"
    )
    ds = cmip6_data_catalog.groupby("instance_id", as_index=False).first()

    definition = definition_factory(cmip6=DatasetCollection(ds, "instance_id"))
    definition.output_directory.mkdir(parents=True)

    result_dir = definition.output_directory / "results" / "recipe_test_a"
    result = result_dir / "work" / "timeseries" / "script1" / "result.nc"

    def mock_run_fn(cmd, *args, **kwargs):
        result.parent.mkdir(parents=True)
        result.touch()

    mock_run = mocker.patch.object(
        cmip_ref_core.providers.subprocess,
        "run",
        autospec=True,
        spec_set=True,
        side_effect=mock_run_fn,
    )
    open_dataset = mocker.patch.object(
        cmip_ref_metrics_esmvaltool.metrics.example.xarray,
        "open_dataset",
        autospec=True,
        spec_set=True,
    )
    open_dataset.return_value.attrs.__getitem__.return_value = "ABC"

    result = metric.run(definition)

    mock_run.assert_called_with(
        [
            f"{provider._conda_exe}",
            "run",
            "--prefix",
            f"{provider.env_path}",
            "esmvaltool",
            "run",
            f"--config-dir={definition.to_output_path('config')}",
            f"{definition.to_output_path('recipe.yml')}",
        ],
        check=True,
    )

    output_bundle_path = definition.output_directory / result.output_bundle_filename
    metric_bundle_path = definition.output_directory / result.metric_bundle_filename

    assert result.successful
    assert output_bundle_path.exists()
    assert output_bundle_path.is_file()
    assert result.output_bundle_filename.name == "output.json"

    assert metric_bundle_path.exists()
    assert metric_bundle_path.is_file()
    assert result.metric_bundle_filename.name == "metric.json"
