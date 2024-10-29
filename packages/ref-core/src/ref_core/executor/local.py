from ref_core.metrics import Metric, MetricResult
from ref_core.providers import Configuration


class LocalExecutor:
    """
    Run a metric locally, in-process.

    This is mainly useful for debugging and testing.
    The production executor will run the metric in a separate process or container,
    the exact manner of which is yet to be determined.
    """

    name = "local"

    def run_metric(self, metric: Metric, configuration: Configuration) -> MetricResult:  # type: ignore
        """
        Run a metric in process

        Parameters
        ----------
        metric
            Metric to run
        configuration
            Configuration to run the metric with

        Returns
        -------
        :
            Results from running the metric
        """
        return metric.run(configuration=configuration)
