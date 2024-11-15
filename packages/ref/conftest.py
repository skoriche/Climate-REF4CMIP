import pytest

from ref.config import Config

# Ignore the alembic folder
collect_ignore = ["alembic"]


@pytest.fixture
def config(tmp_path, monkeypatch) -> Config:
    monkeypatch.setenv("REF_CONFIGURATION", str(tmp_path / "ref"))

    # Uses the default configuration
    cfg = Config.load(tmp_path / "ref" / "ref.toml")

    # Use a SQLite in-memory database for testing
    cfg.db.database_url = "sqlite:///:memory:"
    cfg.save()

    return cfg
