"""
Rapid evaluating CMIP data with ESMValTool.
"""

import climate_ref_esmvaltool.metrics
from climate_ref_core.dataset_registry import dataset_registry_manager
from climate_ref_core.providers import CondaMetricsProvider
from climate_ref_esmvaltool._version import __version__
from climate_ref_esmvaltool.recipe import _ESMVALTOOL_COMMIT

# Initialise the metrics manager.
provider = CondaMetricsProvider(
    "ESMValTool",
    __version__,
    repo="https://github.com/ESMValGroup/ESMValTool.git",
    tag_or_commit=_ESMVALTOOL_COMMIT,
)

# Register the metrics.
for _metric_cls_name in climate_ref_esmvaltool.metrics.__all__:
    _metric_cls = getattr(climate_ref_esmvaltool.metrics, _metric_cls_name)
    provider.register(_metric_cls())

# Register OBS, OBS6, and raw data
dataset_registry_manager.register(
    "esmvaltool",
    "https://pub-b093171261094c4ea9adffa01f94ee06.r2.dev/",
    package="climate_ref_esmvaltool.dataset_registry",
    resource="data.txt",
)
