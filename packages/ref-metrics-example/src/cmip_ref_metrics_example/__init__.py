"""
Rapid evaluating CMIP data
"""

import importlib.metadata

from cmip_ref_core.providers import MetricsProvider
from cmip_ref_metrics_example.example import GlobalMeanTimeseries

__version__ = importlib.metadata.version("cmip_ref_metrics_example")

# Initialise the metrics manager and register the example metric
provider = MetricsProvider("Example", __version__)
provider.register(GlobalMeanTimeseries())
