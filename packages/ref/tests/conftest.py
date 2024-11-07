import pytest
from ref.config import Config


@pytest.fixture
def config(tmp_path, monkeypatch) -> Config:
    monkeypatch.setenv("REF_CONFIGURATION", str(tmp_path / "ref"))

    # Uses the default configuration
    cfg = Config.load(tmp_path / "ref" / "ref.toml")
    cfg.save()

    return cfg
