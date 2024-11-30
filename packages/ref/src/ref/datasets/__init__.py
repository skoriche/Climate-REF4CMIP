"""
Dataset handling utilities
"""

from typing import TYPE_CHECKING

from ref_core.datasets import SourceDatasetType

if TYPE_CHECKING:
    from ref.datasets.base import DatasetAdapter


def get_dataset_adapter(source_type: str) -> "DatasetAdapter":
    """
    Get the appropriate adapter for the specified source type

    Parameters
    ----------
    source_type
        Type of source dataset

    Returns
    -------
    :
        DatasetAdapter instance
    """
    if source_type.lower() == SourceDatasetType.CMIP6.value:
        from ref.datasets.cmip6 import CMIP6DatasetAdapter

        return CMIP6DatasetAdapter()
    else:
        raise ValueError(f"Unknown source type: {source_type}")
