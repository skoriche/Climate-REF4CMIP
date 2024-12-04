"""
Declaration of the models used by the REF.

These models are used to represent the data that is stored in the database.
"""

from typing import TypeVar

from ref.models.base import Base
from ref.models.dataset import Dataset
from ref.models.metric import Metric
from ref.models.metric_execution import MetricExecution
from ref.models.provider import Provider

Table = TypeVar("Table", bound=Base)


__all__ = ["Base", "Dataset", "Table", "Metric", "MetricExecution", "Provider"]
