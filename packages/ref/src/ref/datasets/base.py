from pathlib import Path
from typing import Protocol, TypeVar

import pandas as pd

from ref.config import Config
from ref.database import Database
from ref.models.dataset import Dataset

T = TypeVar("T", bound=Dataset)


class DatasetAdapter(Protocol):
    """
    An adapter to provide a common interface for different dataset types

    This allows the same code to work with different dataset types.
    """

    dataset_model: T
    slug_column: str
    dataset_specific_metadata: tuple[str, ...]
    file_specific_metadata: tuple[str, ...]

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

    def register_dataset(self, config: Config, db: Database, data_catalog_dataset: pd.DataFrame) -> T | None:
        """
        Register a dataset in the database using the data catalog
        """
        ...
