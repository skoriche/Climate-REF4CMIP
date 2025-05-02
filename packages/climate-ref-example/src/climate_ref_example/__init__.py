"""
Rapid evaluating CMIP data
"""

import importlib.metadata

from climate_ref_core.providers import MetricsProvider
from climate_ref_example.example import GlobalMeanTimeseries

__version__ = importlib.metadata.version("climate-ref-example")

# Initialise the metrics manager and register the example metric
provider = MetricsProvider("Example", __version__)
provider.register(GlobalMeanTimeseries())
