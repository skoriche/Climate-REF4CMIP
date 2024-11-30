import enum

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
