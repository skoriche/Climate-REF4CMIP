from ref.datasets.cmip6 import CMIP6DatasetAdapter


class TestCMIP6Adapter:
    def test_catalog_empty(self, db):
        adapter = CMIP6DatasetAdapter()
        df = adapter.load_catalog(db)
        assert df.empty

    def test_catalog(self, db_seeded, data_regression):
        adapter = CMIP6DatasetAdapter()
        df = adapter.load_catalog(db_seeded)

        for k in adapter.dataset_specific_metadata + adapter.file_specific_metadata:
            assert k in df.columns

        assert len(df) == 9  # unique files
        assert df.groupby("instance_id").ngroups == 5  # unique datasets

        data_regression.check(df.to_dict(orient="records"), basename="cmip6_catalog")
