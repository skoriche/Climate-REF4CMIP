"""
Rapid evaluating CMIP data
"""

import importlib.metadata

from cmip_ref_core.providers import MetricsProvider
from cmip_ref_metrics_ilamb.example import GlobalMeanTimeseries
from cmip_ref_metrics_ilamb.standard import ILAMBStandard

__version__ = importlib.metadata.version("cmip_ref_metrics_ilamb")

# Initialise the metrics manager and register the example metric
provider = MetricsProvider("ILAMB", __version__)
provider.register(GlobalMeanTimeseries())
provider.register(ILAMBStandard("tas", "test_Test", "test.txt"))
