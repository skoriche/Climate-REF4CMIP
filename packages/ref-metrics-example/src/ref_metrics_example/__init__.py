"""
Rapid evaluating CMIP data
"""

import importlib.metadata

from ref_core.metrics import MetricManager

from ref_metrics_example.example import ExampleMetric

__version__ = importlib.metadata.version("ref_metrics_example")
__core_version__ = importlib.metadata.version("ref_core")

# Initialise the metrics manager and register the example metric
metrics = MetricManager()
metrics.register(ExampleMetric())

# TODO: Figure out registering a provider
