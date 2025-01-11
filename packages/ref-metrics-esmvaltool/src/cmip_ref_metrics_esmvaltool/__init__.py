"""
Rapid evaluating CMIP data with ESMValTool.
"""

import importlib.metadata

from cmip_ref_core.providers import MetricsProvider

from cmip_ref_metrics_esmvaltool.example import GlobalMeanTimeseries

__version__ = importlib.metadata.version("cmip_ref_metrics_esmvaltool")

# Initialise the metrics manager and register the example metric
provider = MetricsProvider("ESMValTool", __version__)
provider.register(GlobalMeanTimeseries())
