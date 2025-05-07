from collections.abc import Sequence

from pydantic import BaseModel


class SeriesMetricValue(BaseModel):
    """
    A 1-d array with an associated index and additional dimensions

    These values are typically sourced from the CMEC metrics bundle
    """

    dimensions: dict[str, str]
    """
    Key, value pairs that identify the dimensions of the metric

    These values are used for a faceted search of the metric values.
    """
    values: Sequence[float | int]
    """
    A 1-d array of values
    """
    index: Sequence[float | int | str]
    """
    A 1-d array of index values

    Values must be strings or numbers and have the same length as values.
    Non-unique index values are not allowed.
    """

    index_name: str
    """
    The name of the index.

    This is used for presentation purposes and is not used in the controlled vocabulary.
    """

    attributes: dict[str, str | float | int] | None = None
    """
    Additional unstructured attributes associated with the metric value
    """


class ScalarMetricValue(BaseModel):
    """
    A scalar value with an associated dimensions
    """

    dimensions: dict[str, str]
    """
    Key, value pairs that identify the dimensions of the metric

    These values are used for a faceted search of the metric values.
    """
    value: float | int
    """
    A scalar value
    """
    attributes: dict[str, str | float | int] | None = None
    """
    Additional unstructured attributes associated with the metric value
    """
