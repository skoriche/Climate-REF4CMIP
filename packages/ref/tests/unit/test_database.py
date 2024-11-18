import pytest
import sqlalchemy
from ref_core.datasets import SourceDatasetType

from ref.database import Database, validate_database_url
from ref.models.dataset import CMIP6Dataset, Dataset


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


def test_database(db):
    assert db._engine
    assert db.session.is_active


def test_dataset_polymorphic(db):
    db.session.add(CMIP6Dataset(slug="test"))
    assert db.session.query(CMIP6Dataset).count() == 1
    assert db.session.query(Dataset).first().slug == "test"
    assert db.session.query(Dataset).first().dataset_type == SourceDatasetType.CMIP6


def test_transaction_cleanup(db):
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with db.session.begin():
            db.session.add(CMIP6Dataset(slug="test"))
            db.session.add(CMIP6Dataset(slug="test"))
    assert db.session.query(CMIP6Dataset).count() == 0


def test_database_invalid_url(config, monkeypatch):
    monkeypatch.setenv("REF_DATABASE_URL", "postgresql:///localhost:12323/ref")

    with pytest.raises(sqlalchemy.exc.OperationalError):
        Database.from_config(config, run_migrations=True)
