from ref_core.metrics import Metric, MetricExecutionDefinition, MetricResult


class LocalExecutor:
    """
    Run a metric locally, in-process.

    This is mainly useful for debugging and testing.
    The production executor will run the metric in a separate process or container,
    the exact manner of which is yet to be determined.
    """

    name = "local"

    def run_metric(self, metric: Metric, definition: MetricExecutionDefinition) -> MetricResult:
        """
        Run a metric in process

        Parameters
        ----------
        metric
            Metric to run
        definition
            A description of the information needed for this execution of the metric

        Returns
        -------
        :
            Results from running the metric
        """
        # TODO: Update fragment use the output directory which may vary depending on the executor
        definition.output_fragment.mkdir(parents=True, exist_ok=True)

        return metric.run(definition=definition)
