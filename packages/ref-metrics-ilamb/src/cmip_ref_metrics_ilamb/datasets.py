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
    registry = pooch.create(
        path=pooch.os_cache("ilamb3"),  # so that users of ilamb don't duplicate data
        base_url="https://www.ilamb.org/ILAMB-Data/DATA",
        version=version,
        env="REF_METRICS_ILAMB_DATA_DIR",
    )
    registry.load_registry(importlib.resources.open_binary("cmip_ref_metrics_ilamb.registry", registry_file))
    return registry


def registry_to_collection(registry: pooch.Pooch) -> DatasetCollection:
    """
    Convert a ILAMB/IOMB registry to a DatasetCollection for use in REF.

    Keys of the registry are expected to be of the form
    `{variable_id}/{source_id}/filename.nc`. These will be columns of the
    dataframe which is part of the returned collection.

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
                "variable_id": key.split("/")[0],
                "source_id": key.split("/")[1].replace(".nc", ""),
                "path": registry.abspath / Path(key),  # type: ignore
            }
            for key in registry.registry.keys()
        ]
    )
    df["instance_id"] = df["variable_id"] + "_" + df["source_id"]
    collection = DatasetCollection(df, "instance_id")
    return collection
