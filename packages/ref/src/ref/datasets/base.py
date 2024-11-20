from pathlib import Path
from typing import Protocol

import pandas as pd

from ref.config import Config
from ref.database import Database
from ref.models.dataset import Dataset


class DatasetAdapter(Protocol):
    """
    An adapter to provide a common interface for different dataset types

    This allows the same code to work with different dataset types.
    """

    slug_column: str
    dataset_specific_metadata: tuple[str, ...]
    file_specific_metadata: tuple[str, ...] = ()

    def pretty_subset(self, data_catalog: pd.DataFrame) -> pd.DataFrame:
        """
        Get a subset of the data_catalog to pretty print
        """
        ...

    def find_datasets(self, file_or_directory: Path) -> pd.DataFrame:
        """
        Generate a data catalog from the specified file or directory

        This data catalog should contain all the metadata needed by the database.
        The index of the data catalog should be the dataset slug.
        """
        ...

    def register_dataset(
        self, config: Config, db: Database, data_catalog_dataset: pd.DataFrame
    ) -> Dataset | None:
        """
        Register a dataset in the database using the data catalog
        """
        ...

    def validate_data_catalog(self, data_catalog: pd.DataFrame) -> pd.DataFrame:
        """
        Validate a data catalog

        Parameters
        ----------
        data_catalog
            Data catalog to validate

        Returns
        -------
        :
            Validated data catalog
        """
        # Check if the data catalog contains the required columns
        missing_columns = set(self.dataset_specific_metadata + self.file_specific_metadata) - set(
            data_catalog.columns
        )
        if missing_columns:
            raise ValueError(f"Data catalog is missing required columns: {missing_columns}")

        # Verify that the dataset specific columns don't vary by dataset by counting the unique values
        # for each dataset and checking if there are any that have more than one unique value.

        unique_metadata = (
            data_catalog[list(self.dataset_specific_metadata)].groupby(self.slug_column).nunique()
        )
        if unique_metadata.gt(1).any(axis=1).any():
            # Drop out the rows where the values are the same
            invalid_datasets = unique_metadata[unique_metadata.gt(1).any(axis=1)]
            # Drop out the columns where the values are the same
            invalid_datasets = invalid_datasets[invalid_datasets.gt(1)].dropna(axis=1)
            raise ValueError(
                f"Dataset specific metadata varies by dataset.\nUnique values: {invalid_datasets}"
            )

        return data_catalog
