"""Configuration management"""

# The basics of the configuration management takes a lot of inspiration from the
# `esgpull` configuration management system with some of the extra complexity removed.
# https://github.com/ESGF/esgf-download/blob/main/esgpull/config.py

from pathlib import Path
from typing import Any

import tomlkit
from attrs import Factory, define, field, frozen
from cattrs import Converter
from cattrs.gen import make_dict_unstructure_fn, override
from loguru import logger
from tomlkit import TOMLDocument

from cmip_ref.constants import config_filename
from cmip_ref.env import env
from cmip_ref.executor import import_executor
from cmip_ref_core.executor import Executor


def _pop_empty(d: dict[str, Any]) -> None:
    keys = list(d.keys())
    for key in keys:
        value = d[key]
        if isinstance(value, dict):
            _pop_empty(value)
            if not value:
                d.pop(key)


@define
class PathConfig:
    """
    Common paths used by the REF application
    """

    data: Path = field(converter=Path)
    log: Path = field(converter=Path)
    tmp: Path = field(converter=Path)

    # TODO: this should probably default to False,
    # but we don't have an easy way to update cong
    allow_out_of_tree_datasets: bool = field(default=True)

    @data.default
    def _data_factory(self) -> Path:
        return env.path("REF_CONFIGURATION") / "data"

    @log.default
    def _log_factory(self) -> Path:
        return env.path("REF_CONFIGURATION") / "log"

    @tmp.default
    def _tmp_factory(self) -> Path:
        return env.path("REF_CONFIGURATION") / "tmp"


@frozen
class ExecutorConfig:
    """
    Configuration to define the executor to use for running metrics
    """

    executor: str = field()
    """
    Executor to use for running metrics

    This should be the fully qualified name of the executor class (e.g. `cmip_ref.executor.LocalExecutor`).
    The default is to use the local executor.
    The environment variable `REF_EXECUTOR` takes precedence over this configuration value.

    This class will be used for all executions of metrics.
    """

    config: dict[str, Any] = field(factory=dict)
    """
    Additional configuration for the executor.

    See the documentation for the executor for the available configuration options.
    """

    @executor.default
    def _executor_default(self) -> str:
        return env.str("REF_EXECUTOR", default="cmip_ref.executor.local.LocalExecutor")

    def as_executor(self) -> Executor:
        """
        Load the executor class

        Returns
        -------
        :
            The executor class
        """
        return import_executor(self.executor)


@define
class MetricsProviderConfig:
    """
    Configuration for the metrics provider
    """

    provider: str
    """
    Package to use for metrics

    This should be the fully qualified name of the metric provider.
    """

    config: dict[str, Any] = field(factory=dict)
    """
    Additional configuration for the metrics package.

    See the documentation for the metrics package for the available configuration options.
    """

    # TODO: Additional configuration for narrowing down the metrics to run


@frozen
class DbConfig:
    """
    Database configuration

    We currently only plan to support SQLite and PostgreSQL databases,
    although only SQLite is currently implemented and tested.
    """

    database_url: str = field()
    """
    Database URL that describes the connection to the database.

    Defaults to sqlite:///{config.paths.db}/cmip_ref.db".
    This configuration value will be overridden by the `REF_DATABASE_URL` environment variable.

    ## Schemas

    postgresql://USER:PASSWORD@HOST:PORT/NAME
    sqlite:///RELATIVE_PATH or sqlite:////ABS_PATH or sqlite:///:memory:
    """
    run_migrations: bool = field(default=True)

    @database_url.default
    def _connection_url_factory(self) -> str:
        filename = env.path("REF_CONFIGURATION") / "db" / "cmip_ref.db"
        sqlite_url = f"sqlite:///{filename}"
        return sqlite_url


def default_metric_providers() -> list[MetricsProviderConfig]:
    """
    Default metric provider values

    Used if no metric providers are specified in the configuration

    Returns
    -------
    :
        List of default metric providers
    """  # noqa: D401
    return [
        MetricsProviderConfig(provider="cmip_ref_metrics_esmvaltool.provider", config={}),
        MetricsProviderConfig(provider="cmip_ref_metrics_ilamb.provider", config={}),
        MetricsProviderConfig(provider="cmip_ref_metrics_pmp.provider", config={}),
    ]


@define
class Config:
    """
    REF configuration

    This class is used to store the configuration of the REF application.
    """

    paths: PathConfig = Factory(PathConfig)
    db: DbConfig = Factory(DbConfig)
    executor: ExecutorConfig = Factory(ExecutorConfig)
    metric_providers: list[MetricsProviderConfig] = Factory(default_metric_providers)
    _raw: TOMLDocument | None = field(init=False, default=None)
    _config_file: Path | None = field(init=False, default=None)

    @classmethod
    def load(cls, config_file: Path, allow_missing: bool = True) -> "Config":
        """
        Load the configuration from a file

        Parameters
        ----------
        config_file
            Path to the configuration file.
            This should be a TOML file.

        Returns
        -------
        :
            The configuration loaded from the file
        """
        if config_file.is_file():
            with config_file.open() as fh:
                doc = tomlkit.load(fh)
                raw = doc
        else:
            if not allow_missing:
                raise FileNotFoundError(f"Configuration file not found: {config_file}")

            doc = TOMLDocument()
            raw = None
        config = _converter_defaults.structure(doc, cls)
        config._raw = raw
        config._config_file = config_file
        return config

    def save(self, config_file: Path | None = None) -> None:
        """
        Save the configuration as a TOML file

        The configuration will be saved to the specified file.
        If no file is specified, the configuration will be saved to the file
        that was used to load the configuration.

        Parameters
        ----------
        config_file
            The file to save the configuration to

        Raises
        ------
        ValueError
            If no configuration file is specified and the configuration was not loaded from a file
        """
        if config_file is None:
            if self._config_file is None:  # pragma: no cover
                # I'm not sure if this is possible
                raise ValueError("No configuration file specified")
            config_file = self._config_file

        config_file.parent.mkdir(parents=True, exist_ok=True)

        with open(config_file, "w") as fh:
            fh.write(self.dumps())

    @classmethod
    def default(cls) -> "Config":
        """
        Load the default configuration

        This will load the configuration from the default configuration location,
        which is typically the user's configuration directory.
        This location can be overridden by setting the `REF_CONFIGURATION` environment variable.

        Returns
        -------
        :
            The default configuration
        """
        root = env.path("REF_CONFIGURATION")
        path_to_load = root / config_filename

        logger.debug(f"Loading default configuration from {path_to_load}")
        return cls.load(path_to_load)

    def dumps(self, defaults: bool = True) -> str:
        """
        Dump the configuration to a TOML string

        Parameters
        ----------
        defaults
            If True, include default values in the output

        Returns
        -------
        :
            The configuration as a TOML string
        """
        return self.dump(defaults).as_string()

    def dump(
        self,
        defaults: bool = True,
    ) -> TOMLDocument:
        """
        Dump the configuration to a TOML document

        Parameters
        ----------
        defaults
            If True, include default values in the output

        Returns
        -------
        :
            The configuration as a TOML document
        """
        if defaults:
            converter = _converter_defaults
        else:
            converter = _converter_no_defaults
        dump = converter.unstructure(self)
        if not defaults:
            _pop_empty(dump)
        doc = TOMLDocument()
        doc.update(dump)
        return doc


def _make_converter(omit_default: bool) -> Converter:
    conv = Converter(omit_if_default=omit_default, forbid_extra_keys=True)
    conv.register_unstructure_hook(Path, str)
    conv.register_unstructure_hook(
        Config,
        make_dict_unstructure_fn(
            Config,
            conv,
            _raw=override(omit=True),
            _config_file=override(omit=True),
        ),
    )
    return conv


_converter_defaults = _make_converter(omit_default=False)
_converter_no_defaults = _make_converter(omit_default=True)
