from ref_metrics_example import __core_version__ as core_version
from ref_metrics_example import __version__ as version


# Placeholder to get CI working
def test_version():
    assert version == "0.1.0"
    assert core_version == "0.1.0"
