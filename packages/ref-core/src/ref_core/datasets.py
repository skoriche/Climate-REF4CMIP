import enum

from attrs import frozen


class SourceDatasetType(enum.Enum):
    """
    Types of supported source datasets
    """

    CMIP6 = "cmip6"
    CMIP7 = "cmip7"


@frozen
class FacetFilter:
    """
    A filter to apply to a data catalog of datasets.
    """

    facets: dict[str, str | tuple[str, ...]]
    """
    Filters to apply to the data catalog.

    The keys are the metadata fields to filter on, and the values are the values to filter on.
    The result will only contain datasets where for all fields the value of the field is one of the given values.
    """
    keep: bool = True
    """
    Whether to keep or remove datasets that match the filter.

    If true (default), datasets that match the filter will be kept else they will be removed.
    """
