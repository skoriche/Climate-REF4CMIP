"""
Dataset handling utilities
"""

from typing import TYPE_CHECKING

import pandas as pd
from ref_core.datasets import SourceDatasetType

if TYPE_CHECKING:
    from ref.datasets.base import DatasetAdapter


def validate_data_catalog(adapter: "DatasetAdapter", data_catalog: pd.DataFrame) -> pd.DataFrame:
    """
    Validate the data catalog against the adapter

    Parameters
    ----------
    adapter
        Dataset adapter
    data_catalog
        Data catalog to validate

    Returns
    -------
    :
        Validated data catalog
    """
    # Check if the data catalog contains the required columns
    missing_columns = set(adapter.dataset_specific_metadata + adapter.file_specific_metadata) - set(
        data_catalog.columns
    )
    if missing_columns:
        raise ValueError(f"Data catalog is missing required columns: {missing_columns}")

    # Verify that the dataset specific columns don't vary by dataset
    unique_metadata = (
        data_catalog[list(adapter.dataset_specific_metadata)].groupby(adapter.slug_column).nunique()
    )
    if unique_metadata.gt(1).any(axis=1).any():
        invalid_datasets = unique_metadata[unique_metadata.gt(1).any(axis=1)]
        invalid_datasets = invalid_datasets[invalid_datasets.gt(1).any(axis=0)]
        raise ValueError(f"Dataset specific metadata varies by dataset: {invalid_datasets}")

    return data_catalog


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
    if source_type == SourceDatasetType.CMIP6.value:
        from ref.datasets.cmip6 import CMIP6DatasetAdapter

        return CMIP6DatasetAdapter()
    else:
        raise ValueError(f"Unknown source type: {source_type}")
