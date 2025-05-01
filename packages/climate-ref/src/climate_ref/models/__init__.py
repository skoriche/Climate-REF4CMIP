"""
Declaration of the models used by the REF.

These models are used to represent the data that is stored in the database.
"""

from typing import TypeVar

from climate_ref.models.base import Base
from climate_ref.models.dataset import Dataset
from climate_ref.models.metric import Metric
from climate_ref.models.metric_execution import (
    MetricExecutionGroup,
    MetricExecutionResult,
    ResultOutput,
)
from climate_ref.models.metric_value import MetricValue
from climate_ref.models.provider import Provider

Table = TypeVar("Table", bound=Base)


__all__ = [
    "Base",
    "Dataset",
    "Metric",
    "MetricExecutionGroup",
    "MetricExecutionResult",
    "MetricValue",
    "Provider",
    "ResultOutput",
    "Table",
]
