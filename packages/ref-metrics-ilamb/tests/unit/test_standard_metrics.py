import ilamb3
import pytest
from cmip_ref_metrics_ilamb import provider
from cmip_ref_metrics_ilamb.standard import ILAMBStandard, _set_ilamb3_options

from cmip_ref.models import MetricExecutionResult as MetricExecutionResultModel
from cmip_ref.solver import solve_metric_executions
from cmip_ref.testing import validate_result
from cmip_ref_core.dataset_registry import dataset_registry_manager
from cmip_ref_core.datasets import DatasetCollection
from cmip_ref_core.metrics import Metric


def test_standard_site(cmip6_data_catalog, definition_factory):
    metric = ILAMBStandard(
        registry_file="ilamb-test", metric_name="test-site-tas", sources={"tas": "test/Site/tas.nc"}
    )
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
        registry_file="ilamb-test",
        metric_name="test-grid-gpp",
        sources={"gpp": "test/Grid/gpp.nc"},
        relationships={"pr": "test/Grid/pr.nc"},
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
        ILAMBStandard(
            registry_file="ilamb-test",
            metric_name="test-fail",
            sources={"gpp": "test/Grid/gpp.nc", "pr": "test/Grid/pr.nc"},
        )


def test_options():
    _set_ilamb3_options(dataset_registry_manager["ilamb"], "ilamb")
    assert set(["global", "tropical"]).issubset(ilamb3.conf["regions"])


metrics = [pytest.param(metric, id=metric.slug) for metric in provider.metrics()]


@pytest.mark.slow
@pytest.mark.parametrize("metric", metrics)
def test_metrics(metric: Metric, data_catalog, tmp_path, config, mocker):
    mocker.patch.object(MetricExecutionResultModel, "metric_execution_group")

    # Get the first match from the data catalog
    execution = next(
        solve_metric_executions(
            data_catalog=data_catalog,
            metric=metric,
            provider=provider,
        )
    )

    # Run the metric
    definition = execution.build_metric_execution_info(output_root=config.paths.scratch)
    definition.output_directory.mkdir(parents=True, exist_ok=True)
    result = metric.run(definition)

    # Check the result
    validate_result(config, result)
