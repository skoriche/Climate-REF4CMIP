import climate_ref_esmvaltool
import pytest

from climate_ref_core.datasets import DatasetCollection, ExecutionDatasetCollection, SourceDatasetType


@pytest.fixture
def execution_dataset(cmip6_data_catalog) -> ExecutionDatasetCollection:
    selected_dataset = cmip6_data_catalog[
        cmip6_data_catalog["instance_id"] == cmip6_data_catalog.instance_id.iloc[0]
    ]
    return ExecutionDatasetCollection(
        {
            SourceDatasetType.CMIP6: DatasetCollection(
                selected_dataset,
                "instance_id",
            )
        }
    )


def test_example_metric(mocker, tmp_path, execution_dataset, cmip6_data_catalog, definition_factory):
    diagnostic = climate_ref_esmvaltool.provider.get("global-mean-timeseries")
    ds = cmip6_data_catalog.groupby("instance_id", as_index=False).first()

    definition = definition_factory(diagnostic=diagnostic, cmip6=DatasetCollection(ds, "instance_id"))
    definition.output_directory.mkdir(parents=True)

    result_dir = definition.output_directory / "executions" / "recipe_test_a"
    result = result_dir / "work" / "timeseries" / "script1" / "result.nc"

    def mock_run_fn(cmd, *args, **kwargs):
        result.parent.mkdir(parents=True)
        result.touch()

    mock_run = mocker.patch.object(
        climate_ref_esmvaltool.provider,
        "run",
        autospec=True,
        spec_set=True,
        side_effect=mock_run_fn,
    )

    result = diagnostic.run(definition)

    mock_run.assert_called_with(
        [
            "esmvaltool",
            "run",
            f"--config-dir={definition.to_output_path('config')}",
            f"{definition.to_output_path('recipe.yml')}",
        ],
    )

    output_bundle_path = definition.output_directory / result.output_bundle_filename
    metric_bundle_path = definition.output_directory / result.metric_bundle_filename

    assert result.successful
    assert output_bundle_path.exists()
    assert output_bundle_path.is_file()
    assert result.output_bundle_filename.name == "output.json"

    assert metric_bundle_path.exists()
    assert metric_bundle_path.is_file()
    assert result.metric_bundle_filename.name == "diagnostic.json"
