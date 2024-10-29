"""
Interfaces for metrics providers.

This defines how metrics packages interoperate with the REF framework.
"""

import pathlib
from typing import Protocol

from pydantic import BaseModel


class MetricsProvider(Protocol):
    """
    Interface for that a metrics provider must implement.

    This provides a consistent interface to multiple different metrics packages.
    """

    name: str
    version: str


class Configuration(BaseModel):
    """
    Configuration that describes the input data sources
    """

    output_directory: pathlib.Path
    """
    Directory to write output files to
    """
