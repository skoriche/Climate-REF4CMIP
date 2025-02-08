"""Configuration management"""

# The basics of the configuration management takes a lot of inspiration from the
# `esgpull` configuration management system with some of the extra complexity removed.
# https://github.com/ESGF/esgf-download/blob/main/esgpull/config.py

from collections.abc import Callable
from pathlib import Path
from typing import Any

import tomlkit
from attrs import Factory, define, field, frozen
from cattrs import ClassValidationError, Converter, ForbiddenExtraKeysError, IterableValidationError
from cattrs.gen import make_dict_unstructure_fn, override
from cattrs.v import format_exception as default_format_exception
from loguru import logger
from tomlkit import TOMLDocument

from cmip_ref.constants import config_filename
from cmip_ref.executor import import_executor_cls
from cmip_ref_core.env import env
from cmip_ref_core.exceptions import InvalidExecutorException
from cmip_ref_core.executor import Executor


def _pop_empty(d: dict[str, Any]) -> None:
    keys = list(d.keys())
    for key in keys:
        value = d[key]
        if isinstance(value, dict):
            _pop_empty(value)
            if not value:
                d.pop(key)


def _format_key_exception(exc: BaseException, _: type | None) -> str | None:
    """Format a n error exception."""
    if isinstance(exc, ForbiddenExtraKeysError):
        return f"extra fields found ({', '.join(exc.extra_fields)})"
    else:
        return None


def transform_error(
    exc: ClassValidationError | IterableValidationError | BaseException,
    path: str = "$",
    format_exception: Callable[[BaseException, type | None], str | None] = default_format_exception,
) -> list[str]:
    """Transform an exception into a list of error messages.

    This is based on [cattrs.transform_error][cattrs.transform_error],
     but modified to be able to ignore errors

    To get detailed error messages, the exception should be produced by a converter
    with `detailed_validation` set.

    By default, the error messages are in the form of `{description} @ {path}`.

    While traversing the exception and subexceptions, the path is formed:

    * by appending `.{field_name}` for fields in classes
    * by appending `[{int}]` for indices in iterables, like lists
    * by appending `[{str}]` for keys in mappings, like dictionaries

    Parameters
    ----------
    exc
        The exception to transform into error messages.
    path
        The root path to use.
    format_exception
        A callable to use to transform `Exceptions` into string descriptions of errors.
    """
    errors = []

    def _maybe_append_error(exc: BaseException, _: type | None, path: str) -> str | None:
        error_message = format_exception(exc, None)
        if error_message:
            errors.append(f"{error_message} @ {path}")
        return None

    if isinstance(exc, IterableValidationError):
        iterable_validation_notes, without = exc.group_exceptions()
        for inner_exc, iterable_note in iterable_validation_notes:
            p = f"{path}[{iterable_note.index!r}]"
            if isinstance(inner_exc, ClassValidationError | IterableValidationError):
                errors.extend(transform_error(inner_exc, p, format_exception))
            else:
                _maybe_append_error(inner_exc, iterable_note.type, p)
        for inner_exc in without:
            _maybe_append_error(inner_exc, None, path)
    elif isinstance(exc, ClassValidationError):
        class_validation_notes, without = exc.group_exceptions()
        for inner_exc, class_note in class_validation_notes:
            p = f"{path}.{class_note.name}"
            if isinstance(inner_exc, ClassValidationError | IterableValidationError):
                errors.extend(transform_error(inner_exc, p, format_exception))
            else:
                _maybe_append_error(inner_exc, class_note.type, p)
        for inner_exc in without:
            _maybe_append_error(inner_exc, None, path)
    else:
        _maybe_append_error(exc, None, path)

    return errors


@define
class PathConfig:
    """
    Common paths used by the REF application
    """

    # TODO: split data into a per data source configuration
    data: Path = field(converter=Path)
    """
    Root data directory for input data

    The paths used in the data catalogs are relative to this directory.

    This directory must be accessible by all the metric services that are used to run the metrics,
    but does not need to be mounted in the same location on all the metric services.
    """

    log: Path = field(converter=Path)
    """
    Directory to store log files from the compute engine

    This is not currently used by the REF, but is included for future use.
    """

    scratch: Path = field(converter=Path)
    """
    Shared scratch space for the REF.

    This directory is used to write the intermediate results of a metric execution.
    After the metric has been run, the results will be copied to the results directory.

    This directory must be accessible by all the metric services that are used to run the metrics,
    but does not need to be mounted in the same location on all the metric services.
    """

    # TODO: This could be another data source option
    results: Path = field(converter=Path)
    """
    Path to store the results of the metrics
    """

    # TODO: this should probably default to False,
    # but we don't have an easy way to update cong
    allow_out_of_tree_datasets: bool = field(default=True)
    """
    Allow datasets that are not in the data tree

    If True, datasets that are not in the data tree can be added to the data catalog.
    This is useful for testing and development, but should be disabled when using a non-local
    executor.
    """

    @data.default
    def _data_factory(self) -> Path:
        return env.path("REF_CONFIGURATION") / "data"

    @log.default
    def _log_factory(self) -> Path:
        return env.path("REF_CONFIGURATION") / "log"

    @scratch.default
    def _scratch_factory(self) -> Path:
        return env.path("REF_CONFIGURATION") / "scratch"

    @results.default
    def _results_factory(self) -> Path:
        return env.path("REF_CONFIGURATION") / "results"


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

    def build(self) -> Executor:
        """
        Create an instance of the executor

        Returns
        -------
        :
            An executor that can be used to run metrics
        """
        ExecutorCls = import_executor_cls(self.executor)
        executor = ExecutorCls(**self.config)

        if not isinstance(executor, Executor):
            raise InvalidExecutorException(executor, f"Expected an Executor, got {type(executor)}")
        return executor


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


def _load_config(config_file: str | Path, doc: dict[str, Any]) -> "Config":
    # Try loading the configuration with strict validation
    try:
        return _converter_defaults.structure(doc, Config)
    except Exception as exc:
        # Find the extra key errors which are displayed as warnings
        key_validation_errors = transform_error(exc, format_exception=_format_key_exception)
        for key_error in key_validation_errors:
            logger.warning(f"Error loading configuration from {config_file}: {key_error}")

    # Try again with relaxed validation
    return _converter_defaults_relaxed.structure(doc, Config)


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

        try:
            config = _load_config(config_file, doc)
        except Exception as exc:
            # If that still fails, error out
            key_validation_errors = transform_error(exc, format_exception=default_format_exception)
            for key_error in key_validation_errors:
                logger.error(f"Error loading configuration from {config_file}: {key_error}")

            # Deliberately not raising "from exc" to avoid long tracebacks from cattrs
            # The transformed error messages are sufficient for debugging
            raise ValueError(f"Error loading configuration from {config_file}") from None

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


def _make_converter(omit_default: bool, forbid_extra_keys: bool) -> Converter:
    conv = Converter(omit_if_default=omit_default, forbid_extra_keys=forbid_extra_keys)
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


_converter_defaults = _make_converter(omit_default=False, forbid_extra_keys=True)
_converter_defaults_relaxed = _make_converter(omit_default=False, forbid_extra_keys=False)
_converter_no_defaults = _make_converter(omit_default=True, forbid_extra_keys=True)
