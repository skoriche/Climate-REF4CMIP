import pandas as pd
import pytest
from pytest_regressions.data_regression import RegressionYamlDumper
from yaml.representer import SafeRepresenter

from ref.config import Config
from ref.database import Database
from ref.datasets.cmip6 import CMIP6DatasetAdapter

# Ignore the alembic folder
collect_ignore = ["alembic"]


# Add a representer for pandas Timestamps/NaT in the regression tests
RegressionYamlDumper.add_representer(
    pd.Timestamp, lambda dumper, data: SafeRepresenter.represent_datetime(dumper, data.to_pydatetime())
)
RegressionYamlDumper.add_representer(
    type(pd.NaT), lambda dumper, data: SafeRepresenter.represent_none(dumper, data)
)


@pytest.fixture(autouse=True)
def config(tmp_path, monkeypatch) -> Config:
    monkeypatch.setenv("REF_CONFIGURATION", str(tmp_path / "ref"))

    # Uses the default configuration
    cfg = Config.load(tmp_path / "ref" / "ref.toml")

    # Allow adding datasets from outside the tree for testing
    cfg.paths.allow_out_of_tree_datasets = True

    # Use a SQLite in-memory database for testing
    # cfg.db.database_url = "sqlite:///:memory:"
    cfg.save()

    return cfg


@pytest.fixture
def db(config) -> Database:
    return Database.from_config(config, run_migrations=True)


@pytest.fixture
def db_seeded(config, esgf_data_dir) -> Database:
    database = Database.from_config(config, run_migrations=True)

    adapter = CMIP6DatasetAdapter()

    # Seed with all the datasets in the ESGF data directory
    # This includes datasets which span multiple file and until 2300
    data_catalog = adapter.find_local_datasets(esgf_data_dir)
    for instance_id, data_catalog_dataset in data_catalog.groupby(adapter.slug_column):
        adapter.register_dataset(config, database, data_catalog_dataset)

    return database
