import os
import shutil

import pandas as pd
import pytest

from cmip_ref.datasets.pmp_climatology import PMPClimsDatasetAdapter


@pytest.fixture
def test_empty_dir():
    dir_path = "test_empty_directory"
    os.makedirs(dir_path, exist_ok=True)
    yield dir_path
    shutil.rmtree(dir_path)


class TestPMPClimatologyAdapter:
    def test_catalog_empty(self, db):
        adapter = PMPClimsDatasetAdapter()
        df = adapter.load_catalog(db)
        assert df.empty

    def test_load_catalog(self, db_seeded, catalog_regression, sample_data_dir):
        adapter = PMPClimsDatasetAdapter()
        df = adapter.load_catalog(db_seeded)
        for k in adapter.dataset_specific_metadata + adapter.file_specific_metadata:
            assert k in df.columns

        # The order of the rows may be flakey due to sqlite ordering and the created time resolution
        catalog_regression(df.sort_values(["instance_id", "start_time"]), basename="pmp_catalog_db")

    def test_round_trip(self, db_seeded, obs4mips_data_catalog, sample_data_dir):
        # Indexes and ordering may be different
        adapter = PMPClimsDatasetAdapter()
        local_data_catalog = (
            obs4mips_data_catalog.drop(columns=["time_range"])
            .sort_values(["start_time"])
            .reset_index(drop=True)
        )

        db_data_catalog = adapter.load_catalog(db_seeded).sort_values(["start_time"]).reset_index(drop=True)

        # TODO: start_time has a different dtype from the database due to pandas dt coercion
        db_data_catalog["start_time"] = db_data_catalog["start_time"].astype(object)
        db_data_catalog["end_time"] = db_data_catalog["end_time"].astype(object)
        db_data_catalog["vertical_levels"] = db_data_catalog["vertical_levels"].astype(float)
        pd.testing.assert_frame_equal(local_data_catalog, db_data_catalog, check_like=True)

    def test_load_local_datasets(self, sample_data_dir, catalog_regression):
        adapter = PMPClimsDatasetAdapter()
        data_catalog = adapter.find_local_datasets(str(sample_data_dir) + "/obs4MIPs")

        # TODO: add time_range to the db?
        assert sorted(data_catalog.columns.tolist()) == sorted(
            [*adapter.dataset_specific_metadata, *adapter.file_specific_metadata, "time_range"]
        )

        catalog_regression(
            data_catalog.sort_values(["instance_id", "start_time"]), basename="pmp_catalog_local"
        )

    def test_load_local_CMIP6_datasets(self, sample_data_dir):
        with pytest.raises(ValueError) as excinfo:
            adapter = PMPClimsDatasetAdapter()
            adapter.find_local_datasets(str(sample_data_dir) + "/CMIP6")
        assert str(excinfo.value) == "No obs4MIPs-compliant datasets found"

    def test_empty_directory_exception(self, test_empty_dir):
        with pytest.raises(ValueError) as excinfo:
            adapter = PMPClimsDatasetAdapter()
            adapter.find_local_datasets(test_empty_dir)
        assert str(excinfo.value) == "asset list provided is None. Please run `.get_assets()` first"
