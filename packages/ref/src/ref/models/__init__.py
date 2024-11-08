"""
Declaration of the models used by the REF.

These models are used to represent the data that is stored in the database.
"""

from ref.models.base import Base
from ref.models.dataset import Dataset

__all__ = ["Base", "Dataset"]
