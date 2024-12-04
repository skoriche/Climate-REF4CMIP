import pytest
from ref_core.datasets import DatasetCollection, MetricDataset, SourceDatasetType


@pytest.fixture
def dataset_collection(cmip6_data_catalog) -> DatasetCollection:
    return DatasetCollection(
        cmip6_data_catalog,
        "instance_id",
    )


@pytest.fixture
def metric_dataset(dataset_collection) -> MetricDataset:
    return MetricDataset({SourceDatasetType.CMIP6: dataset_collection})


class TestMetricDataset:
    def test_get_item(self, metric_dataset):
        assert metric_dataset["cmip6"] == metric_dataset._collection[SourceDatasetType.CMIP6]
        assert metric_dataset[SourceDatasetType.CMIP6] == metric_dataset._collection[SourceDatasetType.CMIP6]

    def test_get_item_missing(self, metric_dataset):
        with pytest.raises(KeyError):
            metric_dataset["cmip7"]

    def test_hash(self, metric_dataset, cmip6_data_catalog):
        dataset_hash = hash(metric_dataset)
        assert isinstance(dataset_hash, int)

        assert dataset_hash == hash(
            MetricDataset({SourceDatasetType.CMIP6: DatasetCollection(cmip6_data_catalog, "instance_id")})
        )
        assert dataset_hash != hash(
            MetricDataset(
                {
                    SourceDatasetType.CMIP6: DatasetCollection(
                        cmip6_data_catalog[cmip6_data_catalog.variable_id == "tas"], "instance_id"
                    )
                }
            )
        )

    def test_slug(self, metric_dataset):
        assert metric_dataset.hash == "69165f25de10cc0b682e6d1acd1026c0ad27448c"


class TestDatasetCollection:
    def test_get_item(self, dataset_collection):
        expected = dataset_collection.datasets.instance_id
        assert dataset_collection["instance_id"].equals(expected)

    def test_get_attr(self, dataset_collection):
        expected = dataset_collection.datasets.instance_id
        assert dataset_collection.instance_id.equals(expected)

    def test_hash(self, dataset_collection, cmip6_data_catalog):
        dataset_hash = hash(dataset_collection)
        assert isinstance(dataset_hash, int)
        assert dataset_hash == 162057064475757030

        assert dataset_hash == hash(DatasetCollection(cmip6_data_catalog, "instance_id"))
        assert dataset_hash != hash(
            DatasetCollection(cmip6_data_catalog[cmip6_data_catalog.variable_id == "tas"], "instance_id")
        )
