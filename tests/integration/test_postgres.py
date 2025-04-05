"""
Runs an integration test for the connecting to a Postgres DB

This runs the migrations and ingests some datasets as a test.

This test requires a running PostgreSQL server, which is started as a Docker container.
"""

from cmip_ref.database import Database
from cmip_ref.datasets.cmip6 import CMIP6DatasetAdapter


def test_connect_and_migrations(config, postgres_container, cmip6_data_catalog):
    config.db.database_url = postgres_container.connection_url()
    config.save()

    database = Database.from_config(config)
    assert database.url.startswith("postgresql")
    assert database._engine.dialect.name == "postgresql"

    adapter = CMIP6DatasetAdapter()

    with database.session.begin():
        for instance_id, data_catalog_dataset in cmip6_data_catalog.groupby(adapter.slug_column):
            adapter.register_dataset(config, database, data_catalog_dataset)
