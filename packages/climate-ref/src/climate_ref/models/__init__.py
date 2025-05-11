"""
Declaration of the models used by the REF.

These models are used to represent the data that is stored in the database.
"""

from typing import TypeVar

from climate_ref.models.base import Base
from climate_ref.models.dataset import Dataset
from climate_ref.models.diagnostic import Diagnostic
from climate_ref.models.execution import (
    Execution,
    ExecutionGroup,
    ExecutionOutput,
)
from climate_ref.models.metric_value import MetricValue, ScalarMetricValue, SeriesMetricValue
from climate_ref.models.provider import Provider

Table = TypeVar("Table", bound=Base)


__all__ = [
    "Base",
    "Dataset",
    "Diagnostic",
    "Execution",
    "ExecutionGroup",
    "ExecutionOutput",
    "MetricValue",
    "Provider",
    "ScalarMetricValue",
    "SeriesMetricValue",
    "Table",
]
