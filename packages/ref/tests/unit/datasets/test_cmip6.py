from ref.datasets.cmip6 import CMIP6DatasetAdapter


class TestCMIP6Adapter:
    def test_catalog_empty(self, db):
        adapter = CMIP6DatasetAdapter()
        df = adapter.load_catalog(db)
        assert df.empty

    def test_catalog(self, db_seeded):
        adapter = CMIP6DatasetAdapter()
        df = adapter.load_catalog(db_seeded)

        for k in adapter.dataset_specific_metadata:
            assert k in df.columns
        assert len(df) == 5
