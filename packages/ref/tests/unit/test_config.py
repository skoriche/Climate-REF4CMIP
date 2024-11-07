from pathlib import Path

import cattrs
import pytest
from ref.config import Config, Paths


class TestConfig:
    def test_load_missing(self, tmp_path, monkeypatch):
        monkeypatch.setenv("REF_CONFIGURATION", str(tmp_path / "ref"))

        # The configuration file doesn't exist
        # so it should default to some sane defaults
        assert not (tmp_path / "ref.toml").exists()

        loaded = Config.load(Path("ref.toml"))

        assert loaded.paths.data == tmp_path / "ref" / "data"
        assert loaded.paths.db == tmp_path / "ref" / "db"

        # The results aren't serialised back to disk
        assert not (tmp_path / "ref.toml").exists()
        assert loaded._raw is None
        assert loaded._config_file == Path("ref.toml")

    def test_default(self, config):
        config.paths.data = "data"
        config.save()

        # The default location is overridden in the config fixture
        loaded = Config.default()
        assert loaded.paths.data == Path("data")

    def test_load(self, config, tmp_path):
        res = config.dump(defaults=True)

        with open(tmp_path / "ref.toml", "w") as fh:
            fh.write(res.as_string())

        loaded = Config.load(tmp_path / "ref.toml")

        assert config.dumps() == loaded.dumps()

    def test_load_extra_keys(self, tmp_path):
        content = """[paths]
data = "data"
extra = "extra"

[db]
filename = "sqlite://ref.db"
"""

        with open(tmp_path / "ref.toml", "w") as fh:
            fh.write(content)

        # cattrs exceptions are a bit ugly, but you get an exception like this:
        #
        #   | cattrs.errors.ClassValidationError: While structuring Config (1 sub-exception)
        #   +-+---------------- 1 ----------------
        #     | Exception Group Traceback (most recent call last):
        #     |   File "<cattrs generated structure ref.config.Config>", line 6, in structure_Config
        #     |     res['paths'] = __c_structure_paths(o['paths'], __c_type_paths)
        #     |   File "<cattrs generated structure ref.config.Paths>", line 31, in structure_Paths
        #     |     if errors: raise __c_cve('While structuring ' + 'Paths', errors, __cl)
        #     | cattrs.errors.ClassValidationError: While structuring Paths (1 sub-exception)
        #     | Structuring class Config @ attribute paths
        #     +-+---------------- 1 ----------------
        #       | cattrs.errors.ForbiddenExtraKeysError: Extra fields in constructor for Paths: extra

        with pytest.raises(cattrs.errors.ClassValidationError):
            Config.load(tmp_path / "ref.toml")

    def test_save(self, tmp_path):
        config = Config(paths=Paths(data=Path("data")))

        with pytest.raises(ValueError):
            # The configuration file hasn't been set as it was created directly
            config.save()

        config.save(tmp_path / "ref.toml")

        assert (tmp_path / "ref.toml").exists()

    def test_defaults(self, monkeypatch):
        monkeypatch.setenv("REF_CONFIGURATION", "test")

        cfg = Config.load(Path("test.toml"))

        with_defaults = cfg.dump(defaults=True)

        without_defaults = cfg.dump(defaults=False)

        assert without_defaults == {}
        assert with_defaults == {
            "paths": {"data": "test/data", "db": "test/db", "log": "test/log", "tmp": "test/tmp"},
            "db": {"filename": "sqlite://ref.db"},
        }
