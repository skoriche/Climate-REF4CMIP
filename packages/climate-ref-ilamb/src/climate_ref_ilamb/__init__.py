"""
Diagnostic provider for ILAMB

This module provides a diagnostics provider for ILAMB, a tool for evaluating
climate models against observations.
"""

import importlib.metadata
import importlib.resources

import yaml

from climate_ref_core.dataset_registry import DATASET_URL, dataset_registry_manager
from climate_ref_core.providers import DiagnosticProvider
from climate_ref_ilamb.standard import ILAMBStandard

__version__ = importlib.metadata.version("climate-ref-ilamb")

provider = DiagnosticProvider("ILAMB", __version__)

# Register some datasets
dataset_registry_manager.register(
    "ilamb-test",
    base_url=DATASET_URL,
    package="climate_ref_ilamb.dataset_registry",
    resource="test.txt",
)
dataset_registry_manager.register(
    "ilamb",
    base_url=DATASET_URL,
    package="climate_ref_ilamb.dataset_registry",
    resource="ilamb.txt",
)
dataset_registry_manager.register(
    "iomb",
    base_url=DATASET_URL,
    package="climate_ref_ilamb.dataset_registry",
    resource="iomb.txt",
)

# Dynamically register ILAMB diagnostics
for yaml_file in importlib.resources.files("climate_ref_ilamb.configure").iterdir():
    with open(str(yaml_file)) as fin:
        metrics = yaml.safe_load(fin)
    registry_filename = metrics.pop("registry")
    for metric, options in metrics.items():
        provider.register(ILAMBStandard(registry_filename, metric, options.pop("sources"), **options))
