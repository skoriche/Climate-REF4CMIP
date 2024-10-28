from pathlib import Path

from ref_core.providers import Configuration, MetricResult


class ExampleMetric:
    """
    Example metric that does nothing but count the number of times it has been run.
    """

    def __init__(self):
        self._count = 0

    def run(self, configuration: Configuration) -> MetricResult:
        """
        Run a metric

        Parameters
        ----------
        configuration

        Returns
        -------
        :
            The result of running the metric.
        """
        self._count += 1

        return MetricResult(
            output_bundle=Path("output.json"),
            successful=True,
        )
