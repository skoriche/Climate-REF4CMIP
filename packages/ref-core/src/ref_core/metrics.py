import pathlib
from typing import Protocol, runtime_checkable

from pydantic import BaseModel

from ref_core.providers import Configuration


class MetricResult(BaseModel):
    """
    The result of running a metric.

    The content of the result follows the Earth System Metrics and Diagnostics Standards
    ([EMDS](https://github.com/Earth-System-Diagnostics-Standards/EMDS/blob/main/standards.md)).
    """

    # Do we want to load a serialised version of the output bundle here or just a file path?

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
    # Log info is in the output bundle file already, but is definitely useful


@runtime_checkable
class Metric(Protocol):
    """
    Interface for a metric that must be implemented.
    """

    name: str
    """
    Name of the metric being run
    """

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


class MetricManager:
    """
    Manages the registration of metrics and retrieval by name.
    """

    def __init__(self) -> None:
        self._metrics: dict[str, Metric] = {}

    def register(self, metric: Metric) -> None:
        """
        Register a metric with the manager.

        Parameters
        ----------
        metric : Metric
            The metric to register.
        """
        if not isinstance(metric, Metric):
            raise ValueError("Metric must be an instance of Metric")
        self._metrics[metric.name.lower()] = metric

    def get(self, name: str) -> Metric:
        """
        Get a metric by name.

        Parameters
        ----------
        name : str
            Name of the metric (case-sensitive).

        Raises
        ------
        KeyError
            If the metric with the given name is not found.

        Returns
        -------
        Metric
            The requested metric.
        """
        return self._metrics[name.lower()]
