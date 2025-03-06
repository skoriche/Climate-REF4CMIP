"""
Rapid evaluating CMIP data
"""

import importlib.metadata
import importlib.resources

import yaml

from cmip_ref_core.providers import MetricsProvider
from cmip_ref_metrics_ilamb.standard import ILAMBStandard

__version__ = importlib.metadata.version("cmip_ref_metrics_ilamb")

provider = MetricsProvider("ILAMB", __version__)

# Dynamically register ILAMB metrics
for yaml_file in importlib.resources.files("cmip_ref_metrics_ilamb.configure").iterdir():
    with open(str(yaml_file)) as fin:
        metrics = yaml.safe_load(fin)
    registry_file = metrics.pop("registry")
    for metric, options in metrics.items():
        provider.register(ILAMBStandard(registry_file, metric, options.pop("sources"), **options))
