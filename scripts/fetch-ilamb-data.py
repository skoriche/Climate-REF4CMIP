"""
Fetch ILAMB data from the given registry.

Running on `test.txt` is required to run the local test suite.
"""

import sys

from cmip_ref_metrics_ilamb.datasets import ILAMB_DATA_VERSION, ILAMB_REGISTRIES, build_ilamb_data_registry

_LEN_ARGV = 2  # single argument plus script name

if __name__ == "__main__":
    if not len(sys.argv) == _LEN_ARGV:
        print("Usage: python fetch-ilamb-data.py registry_file.txt")
        sys.exit(1)
    registry_file = sys.argv[1]
    if registry_file not in ILAMB_REGISTRIES:
        raise ValueError(f"'{registry_file}' not a valid registry ({ILAMB_REGISTRIES}).")
    registry = build_ilamb_data_registry(sys.argv[1], ILAMB_DATA_VERSION)
    for key in registry.registry.keys():
        registry.fetch(key)
