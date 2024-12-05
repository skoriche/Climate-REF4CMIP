import pandas as pd
import pytest

from ref.datasets.cmip6 import CMIP6DatasetAdapter


@pytest.fixture
def catalog_regression(data_regression, esgf_data_dir):
    def check(df: pd.DataFrame, basename: str):
        # Strip the path to make the test more robust
        df["path"] = df["path"].str.replace(str(esgf_data_dir), "{esgf_data_dir}")

        data_regression.check(df.to_dict(orient="records"), basename=basename)

    return check


class TestCMIP6Adapter:
    def test_catalog_empty(self, db):
        adapter = CMIP6DatasetAdapter()
        df = adapter.load_catalog(db)
        assert df.empty

    def test_load_catalog(self, db_seeded, catalog_regression, esgf_data_dir):
        adapter = CMIP6DatasetAdapter()
        df = adapter.load_catalog(db_seeded)

        for k in adapter.dataset_specific_metadata + adapter.file_specific_metadata:
            assert k in df.columns

        assert len(df) == 9  # unique files
        assert df.groupby("instance_id").ngroups == 5  # unique datasets

        # The order of the rows may be flakey due to sqlite ordering and the created time resolution
        catalog_regression(df.sort_values(["instance_id", "start_time"]), basename="cmip6_catalog_db")

    def test_round_trip(self, db_seeded, esgf_data_dir):
        adapter = CMIP6DatasetAdapter()
        local_data_catalog = adapter.find_local_datasets(esgf_data_dir).drop(columns=["time_range"])

        db_data_catalog = adapter.load_catalog(db_seeded)

        # TODO: start_time has a different dtype from the database due to pandas dt coercion
        db_data_catalog["start_time"] = db_data_catalog["start_time"].astype(object)
        pd.testing.assert_frame_equal(local_data_catalog, db_data_catalog, check_like=True)

    def test_load_local_datasets(self, esgf_data_dir, catalog_regression):
        adapter = CMIP6DatasetAdapter()
        data_catalog = adapter.find_local_datasets(esgf_data_dir)

        # TODO: add time_range to the db?
        assert sorted(data_catalog.columns.tolist()) == sorted(
            [*adapter.dataset_specific_metadata, *adapter.file_specific_metadata, "time_range"]
        )

        catalog_regression(
            data_catalog.sort_values(["instance_id", "start_time"]), basename="cmip6_catalog_local"
        )
