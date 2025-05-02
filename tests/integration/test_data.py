import alembic.command

from climate_ref.database import Database


def test_sample_data_available(sample_data_dir):
    top_level_files = list(sample_data_dir.rglob("*.nc"))
    assert top_level_files, "Run `make fetch-test-data` to download test data"

    print(top_level_files)


def test_check_up_to_date(config):
    database = Database.from_config(config)

    # Verify that the migrations match the codebase for postgres
    alembic.command.check(database.alembic_config(config))

    # Verify that we can go downgrade to an empty db
    alembic.command.downgrade(database.alembic_config(config), "base")
