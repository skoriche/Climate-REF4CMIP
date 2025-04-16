"""
Rapid evaluating CMIP data
"""

import importlib.metadata

__version__ = importlib.metadata.version("cmip_ref")


from cmip_ref.testing import SAMPLE_DATA_VERSION
from cmip_ref_core.dataset_registry import dataset_registry_manager

# Register the obs4REF data registry
dataset_registry_manager.register(
    "obs4ref",
    "https://pub-b093171261094c4ea9adffa01f94ee06.r2.dev/",
    package="cmip_ref.dataset_registry",
    resource="obs4ref_reference.txt",
)
# Register the sample data registry -- used for testing
dataset_registry_manager.register(
    "sample-data",
    "https://raw.githubusercontent.com/Climate-REF/ref-sample-data/refs/tags/{version}/data/",
    package="cmip_ref.dataset_registry",
    resource="sample_data.txt",
    version=SAMPLE_DATA_VERSION,
)


__all__ = ["__version__"]
