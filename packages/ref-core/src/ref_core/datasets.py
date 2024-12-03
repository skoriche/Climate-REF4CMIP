import base64
import enum

import pandas as pd
from attrs import field, frozen


class SourceDatasetType(enum.Enum):
    """
    Types of supported source datasets
    """

    CMIP6 = "cmip6"
    CMIP7 = "cmip7"


def _clean_facets(raw_values: dict[str, str | tuple[str, ...] | list[str]]) -> dict[str, tuple[str, ...]]:
    """
    Clean the value of a facet filter to a tuple of strings
    """
    result = {}

    for key, value in raw_values.items():
        if isinstance(value, list):
            result[key] = tuple(value)
        elif isinstance(value, str):
            result[key] = (value,)
        elif isinstance(value, tuple):
            result[key] = value
    return result


@frozen
class FacetFilter:
    """
    A filter to apply to a data catalog of datasets.
    """

    facets: dict[str, tuple[str, ...]] = field(converter=_clean_facets)
    """
    Filters to apply to the data catalog.

    The keys are the metadata fields to filter on, and the values are the values to filter on.
    The result will only contain datasets where for all fields,
    the value of the field is one of the given values.
    """
    keep: bool = True
    """
    Whether to keep or remove datasets that match the filter.

    If true (default), datasets that match the filter will be kept else they will be removed.
    """


@frozen
class DatasetCollection:
    """
    Group of datasets required for a given metric execution for a specific source dataset type.
    """

    datasets: pd.DataFrame
    slug_column: str

    def __getattr__(self, item):
        return getattr(self.datasets, item)

    def __getitem__(self, item):
        return self.datasets[item]

    def __hash__(self) -> int:
        return hash(self.datasets[self.slug_column].to_string(index=False))


class MetricDataset:
    """
    The complete set of datasets required for a metric execution.

    This may cover multiple source dataset types.
    """

    def __init__(self, collection: dict[SourceDatasetType, DatasetCollection]):
        self._collection = collection

    def __getitem__(self, key: SourceDatasetType | str) -> DatasetCollection:
        if isinstance(key, str):
            key = SourceDatasetType(key)
        return self._collection[key]

    def __hash__(self):
        return hash(tuple(hash(item) for item in self._collection.items()))

    def __len__(self):
        return len(self._collection)

    @property
    def slug(self):
        """
        Unique identifier for the collection

        This is a base64 encoded hash of the collections.
        The value isn't reversible but can be used to uniquely identify the collection.

        Returns
        -------
        :
            Base64 encoded hash of the collection
        """
        return base64.b64encode(str(self.__hash__()))
