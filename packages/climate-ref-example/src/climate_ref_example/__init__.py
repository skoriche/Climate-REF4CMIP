"""
Rapid evaluating CMIP data
"""

import importlib.metadata

from climate_ref_core.providers import DiagnosticProvider
from climate_ref_example.example import GlobalMeanTimeseries

__version__ = importlib.metadata.version("climate-ref-example")

# Initialise the diagnostics manager and register the example diagnostic
provider = DiagnosticProvider("Example", __version__)
provider.register(GlobalMeanTimeseries())
