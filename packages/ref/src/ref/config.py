"""Configuration management"""

# The basics of the configuration management takes a lot of inspiration from the
# `esgpull` configuration management system with some of the extra complexity removed.
# https://github.com/ESGF/esgf-download/blob/main/esgpull/config.py

from pathlib import Path
from typing import Any

import tomlkit
from attrs import Factory, define, field
from cattrs import Converter
from cattrs.gen import make_dict_unstructure_fn, override
from tomlkit import TOMLDocument

from ref.constants import config_filename
from ref.env import env


def _pop_empty(d: dict[str, Any]) -> None:
    keys = list(d.keys())
    for key in keys:
        value = d[key]
        if isinstance(value, dict):
            _pop_empty(value)
            if not value:
                d.pop(key)


@define
class Paths:
    """
    Common paths used by the REF application
    """

    data: Path = field(converter=Path)
    db: Path = field(converter=Path)
    log: Path = field(converter=Path)
    tmp: Path = field(converter=Path)

    @data.default
    def _data_factory(self) -> Path:
        return env.path("REF_CONFIGURATION") / "data"

    @db.default
    def _db_factory(self) -> Path:
        return env.path("REF_CONFIGURATION") / "db"

    @log.default
    def _log_factory(self) -> Path:
        return env.path("REF_CONFIGURATION") / "log"

    @tmp.default
    def _tmp_factory(self) -> Path:
        return env.path("REF_CONFIGURATION") / "tmp"


@define
class Db:
    """
    Database configuration
    """

    filename: str = "sqlite://ref.db"


@define
class Config:
    """
    REF configuration

    This class is used to store the configuration of the REF application.
    """

    paths: Paths = Factory(Paths)
    db: Db = Factory(Db)
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
        return cls.load(root / config_filename)

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
