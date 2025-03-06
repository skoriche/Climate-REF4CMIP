"""A module for defining and fetching ILAMB/IOMB datasets"""

import importlib
import importlib.resources
from pathlib import Path
from typing import Literal

import pandas as pd
import pooch

from cmip_ref_core.datasets import DatasetCollection

ILAMB_DATA_VERSION = "0.1"  # we don't really have data versions for the collection :/
ILAMB_REGISTRIES = [f.name for f in importlib.resources.files("cmip_ref_metrics_ilamb.registry").iterdir()]
ILAMBRegistryFile = Literal["ilamb.txt", "iomb.txt", "test.txt"]


def build_ilamb_data_registry(registry_file: ILAMBRegistryFile, version: str) -> pooch.Pooch:
    """
    Build a pooch data registry associated with ILAMB/IOMB.

    Parameters
    ----------
    registry_file : str
        The name of the file on which to base this registry.
    version : str
        The version of the data.

    Returns
    -------
    pooch.Pooch
        The pooch registry.
    """
    # temporary silliness, we will fix out data organization soon
    if registry_file in ["ilamb.txt", "test.txt"]:
        registry = pooch.create(
            path=pooch.os_cache("ilamb3"),
            base_url="https://www.ilamb.org/ILAMB-Data/DATA",
            version=version,
            env="REF_METRICS_ILAMB_DATA_DIR",
        )
    if registry_file in ["iomb.txt"]:
        registry = pooch.create(
            path=pooch.os_cache("ilamb3"),
            base_url="https://www.ilamb.org/IOMB-Data/DATA",
            version=version,
            env="REF_METRICS_ILAMB_DATA_DIR",
        )
    registry.load_registry(importlib.resources.open_binary("cmip_ref_metrics_ilamb.registry", registry_file))
    return registry


def registry_to_collection(registry: pooch.Pooch) -> DatasetCollection:
    """
    Convert a ILAMB/IOMB registry to a DatasetCollection for use in REF.

    Parameters
    ----------
    registry : pooch.Pooch
        The pooch registry.

    Returns
    -------
    DatasetCollection
        The converted collection.
    """
    df = pd.DataFrame(
        [
            {
                "key": key,
                "path": registry.abspath / Path(key),  # type: ignore
            }
            for key in registry.registry.keys()
        ]
    )
    collection = DatasetCollection(df, "key")
    return collection
