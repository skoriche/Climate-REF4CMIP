"""A module for defining and fetching ILAMB/IOMB datasets"""

from pathlib import Path

import pandas as pd
import pooch

from cmip_ref_core.datasets import DatasetCollection

ILAMB_DATA_VERSION = "0.1"  # we don't really have data versions for the collection :/


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
