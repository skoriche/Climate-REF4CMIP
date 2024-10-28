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


class MetricResult(BaseModel):
    """
    The result of running a metric.

    The content of the result follows the Earth System Metrics and Diagnostics Standards
    ([EMDS](https://github.com/Earth-System-Diagnostics-Standards/EMDS/blob/main/standards.md)).
    """

    output_bundle: pathlib.Path
    """
    Path to the output bundle file.

    The contents of this file are defined by
    [EMDS standard](https://github.com/Earth-System-Diagnostics-Standards/EMDS/blob/main/standards.md#common-output-bundle-format-)
    """
    successful: bool
    """
    Whether the metric ran successfully.
    """


class Configuration:
    """
    Configuration that describes the input data sources
    """

    ...


class Metric(Protocol):
    """
    Interface for a metric that must be implemented.
    """

    name: str
    """
    Name of the metric being run
    """

    provider: MetricsProvider

    def run(self, configuration: Configuration) -> MetricResult:
        """
        Run the metric using .

        Parameters
        ----------
        configuration : Configuration
            The configuration to run the metric on.

        Returns
        -------
        MetricResult
            The result of running the metric.
        """
