import re
from pathlib import Path

import cattrs
import pytest
from attr import evolve

from cmip_ref.config import Config, PathConfig
from cmip_ref_core.exceptions import InvalidExecutorException
from cmip_ref_core.executor import Executor


class TestConfig:
    def test_load_missing(self, tmp_path, monkeypatch):
        monkeypatch.setenv("REF_CONFIGURATION", str(tmp_path / "cmip_ref"))

        # The configuration file doesn't exist
        # so it should default to some sane defaults
        assert not (tmp_path / "ref.toml").exists()

        loaded = Config.load(Path("ref.toml"))

        assert loaded.paths.data == tmp_path / "cmip_ref" / "data"

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
filename = "sqlite://cmip_ref.db"
"""

        with open(tmp_path / "ref.toml", "w") as fh:
            fh.write(content)

        # cattrs exceptions are a bit ugly, but you get an exception like this:
        #
        #   | cattrs.errors.ClassValidationError: While structuring Config (1 sub-exception)
        #   +-+---------------- 1 ----------------
        #     | Exception Group Traceback (most recent call last):
        #     |   File "<cattrs generated structure cmip_ref.config.Config>", line 6, in structure_Config
        #     |     res['paths'] = __c_structure_paths(o['paths'], __c_type_paths)
        #     |   File "<cattrs generated structure cmip_ref.config.PathConfig>", line 31, in structure_Paths
        #     |     if errors: raise __c_cve('While structuring ' + 'PathConfig', errors, __cl)
        #     | cattrs.errors.ClassValidationError: While structuring PathConfig (1 sub-exception)
        #     | Structuring class Config @ attribute paths
        #     +-+---------------- 1 ----------------
        #       | cattrs.errors.ForbiddenExtraKeysError: Extra fields in constructor for PathConfig: extra

        with pytest.raises(cattrs.errors.ClassValidationError):
            Config.load(tmp_path / "ref.toml")

    def test_save(self, tmp_path):
        config = Config(paths=PathConfig(data=Path("data")))

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

        assert without_defaults == {
            "metric_providers": [
                {"provider": "cmip_ref_metrics_esmvaltool.provider"},
                {"provider": "cmip_ref_metrics_ilamb.provider"},
                {"provider": "cmip_ref_metrics_pmp.provider"},
            ],
        }
        assert with_defaults == {
            "metric_providers": [
                {
                    "provider": "cmip_ref_metrics_esmvaltool.provider",
                    "config": {},
                },
                {
                    "provider": "cmip_ref_metrics_ilamb.provider",
                    "config": {},
                },
                {
                    "provider": "cmip_ref_metrics_pmp.provider",
                    "config": {},
                },
            ],
            "executor": {"executor": "cmip_ref.executor.local.LocalExecutor", "config": {}},
            "paths": {
                "data": "test/data",
                "log": "test/log",
                "tmp": "test/tmp",
                "allow_out_of_tree_datasets": True,
            },
            "db": {"database_url": "sqlite:///test/db/cmip_ref.db", "run_migrations": True},
        }

    def test_executor_build(self, config):
        executor = config.executor.build()
        assert executor.name == "local"
        assert isinstance(executor, Executor)

        # None of the executors support initialisation arguments yet so this is a bit of a placeholder
        config.executor.config["test"] = "value"

        match = re.escape("LocalExecutor.__init__() got an unexpected keyword argument 'test'")
        with pytest.raises(TypeError, match=match):
            config.executor.build()

    def test_executor_build_invalid(self, config):
        config.executor = evolve(config.executor, executor="cmip_ref.config.DbConfig")

        match = "Expected an Executor, got <class 'cmip_ref.config.DbConfig'>"
        with pytest.raises(InvalidExecutorException, match=match):
            config.executor.build()
