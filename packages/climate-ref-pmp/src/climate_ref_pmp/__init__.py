"""
Rapid evaluating CMIP data
"""

import importlib.metadata

from climate_ref_core.dataset_registry import dataset_registry_manager
from climate_ref_core.providers import CondaDiagnosticProvider
from climate_ref_pmp.diagnostics import AnnualCycle, ExtratropicalModesOfVariability

__version__ = importlib.metadata.version("climate-ref-pmp")

# Create the PMP diagnostics provider
# PMP uses a conda environment to run the diagnostics
provider = CondaDiagnosticProvider("PMP", __version__)

provider.register(ExtratropicalModesOfVariability("PDO"))
provider.register(ExtratropicalModesOfVariability("NPGO"))
provider.register(ExtratropicalModesOfVariability("NAO"))
provider.register(ExtratropicalModesOfVariability("NAM"))
provider.register(ExtratropicalModesOfVariability("PNA"))
provider.register(ExtratropicalModesOfVariability("NPO"))
provider.register(ExtratropicalModesOfVariability("SAM"))
provider.register(AnnualCycle())


dataset_registry_manager.register(
    "pmp-climatology",
    "https://pub-b093171261094c4ea9adffa01f94ee06.r2.dev/",
    package="climate_ref_pmp.dataset_registry",
    resource="pmp_climatology.txt",
)
