import pytest
import sqlalchemy
from sqlalchemy import select

from ref.database import Database, validate_database_url
from ref.models import Dataset


@pytest.mark.parametrize(
    "database_url",
    [
        "sqlite:///:memory:",
        "sqlite:///{tmp_path}/ref.db",
        "postgresql://localhost:5432/ref",
    ],
)
def test_validate_database_url(config, database_url, tmp_path):
    validate_database_url(database_url.format(tmp_path=str(tmp_path)))


@pytest.mark.parametrize("database_url", ["mysql:///:memory:", "no_scheme/test"])
def test_invalid_urls(config, database_url, tmp_path):
    with pytest.raises(ValueError):
        validate_database_url(database_url.format(tmp_path=str(tmp_path)))


def test_database(config):
    db = Database.from_config(config, run_migrations=True)

    assert db._engine
    assert db.session.is_active

    db.session.add(
        Dataset(
            dataset_id="big_dataset",
            instance_id="test",
            master_id="master",
            version="version",
            data_node="data_node",
            size=12,
            number_of_files=1,
        )
    )
    db.session.add(
        Dataset(
            dataset_id="small_dataset",
            instance_id="test",
            master_id="master",
            version="version",
            data_node="data_node",
            size=1,
            number_of_files=1,
        )
    )

    stmt = select(Dataset).where(Dataset.size >= 12)
    res = db.session.scalars(stmt).all()
    assert len(res) == 1

    assert res[0].dataset_id == "big_dataset"


def test_database_invalid_url(config, monkeypatch):
    monkeypatch.setenv("REF_DATABASE_URL", "postgresql:///localhost:12323/ref")

    with pytest.raises(sqlalchemy.exc.OperationalError):
        Database.from_config(config, run_migrations=True)
