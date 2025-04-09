"""
Rapid evaluating CMIP data with ESMValTool.
"""

import cmip_ref_metrics_esmvaltool.metrics
from cmip_ref_core.providers import CondaMetricsProvider
from cmip_ref_metrics_esmvaltool._version import __version__
from cmip_ref_metrics_esmvaltool.recipe import _ESMVALTOOL_COMMIT

# Initialise the metrics manager.
provider = CondaMetricsProvider(
    "ESMValTool",
    __version__,
    repo="https://github.com/ESMValGroup/ESMValTool.git",
    tag_or_commit=_ESMVALTOOL_COMMIT,
)

# Register the metrics.
for _metric_cls_name in cmip_ref_metrics_esmvaltool.metrics.__all__:
    _metric_cls = getattr(cmip_ref_metrics_esmvaltool.metrics, _metric_cls_name)
    provider.register(_metric_cls())
