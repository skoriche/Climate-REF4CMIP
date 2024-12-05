"""
Re-useable fixtures etc. for tests that are shared across the whole project

See https://docs.pytest.org/en/7.1.x/reference/fixtures.html#conftest-py-sharing-fixtures-across-multiple-files
"""

from pathlib import Path

import esgpull
import pandas as pd
import pytest

from ref.config import Config
from ref.datasets.cmip6 import CMIP6DatasetAdapter


@pytest.fixture
def esgf_data_dir() -> Path:
    pull = esgpull.Esgpull()

    return pull.config.paths.data


@pytest.fixture
def cmip6_data_catalog(esgf_data_dir) -> pd.DataFrame:
    adapter = CMIP6DatasetAdapter()
    return adapter.find_local_datasets(esgf_data_dir)


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
