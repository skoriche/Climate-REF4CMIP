import ilamb3
import pytest
from cmip_ref_metrics_ilamb.standard import ILAMBStandard, _set_ilamb3_options

from cmip_ref_core.datasets import DatasetCollection


def test_standard_site(cmip6_data_catalog, definition_factory):
    metric = ILAMBStandard(
        registry_file="test.txt", metric_name="test-site-tas", sources={"tas": "test/Site/tas.nc"}
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
        registry_file="test.txt",
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
            registry_file="test.txt",
            metric_name="test-fail",
            sources={"gpp": "test/Grid/gpp.nc", "pr": "test/Grid/pr.nc"},
        )


def test_options():
    _set_ilamb3_options("ilamb.txt")
    assert set(["global", "tropical"]).issubset(ilamb3.conf["regions"])
