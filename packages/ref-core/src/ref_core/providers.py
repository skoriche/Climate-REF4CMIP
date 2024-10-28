"""
Interfaces for metrics providers.

This defines how metrics packages interoperate with the REF framework.
"""

from typing import Protocol


class MetricsProvider(Protocol):
    """
    Interface for that a metrics provider must implement.

    This provides a consistent interface to multiple different metrics packages.
    """

    name: str


def run_metric(metric: MetricInformation, configuration: Configuration) -> MetricResult:
    """
    Run a metric on a configuration.

    Parameters
    ----------
    metric : MetricInformation
        The metric to run.
    configuration : Configuration
        The configuration to run the metric on.

    Returns
    -------
    MetricResult
        The result of running the metric.
    """
    pass
