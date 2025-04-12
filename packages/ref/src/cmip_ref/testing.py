"""
Testing utilities
"""

import shutil
from pathlib import Path

from loguru import logger

from cmip_ref_core.dataset_registry import fetch_all_files, registry


def _determine_test_directory() -> Path | None:
    expected = Path(__file__).parents[4] / "tests" / "test-data"

    if not expected.exists():  # pragma: no cover
        return None
    return expected


TEST_DATA_DIR = _determine_test_directory()
SAMPLE_DATA_VERSION = "v0.4.3"


def fetch_sample_data(force_cleanup: bool = False, symlink: bool = False) -> None:
    """
    Fetch the sample data for the given version.

    Parameters
    ----------
    version
        The version tag of the sample data to fetch.

        This will fail if the version is not found in the sample data registry
        or if the sample data registry file is incompatible with this version.
    force_cleanup
        If True, remove any existing files
    symlink
        If True, symlink in the data otherwise copy the files

        The symlink approach is faster, but will fail when running with a non-local executor
        because the symlinks can't be followed.
    """

    if TEST_DATA_DIR is None:  # pragma: no cover
        logger.warning("Test data directory not found, skipping sample data fetch")
        return

    sample_data_registry = registry["sample-data"]

    output_dir = TEST_DATA_DIR / "sample-data"
    version_file = output_dir / "version.txt"
    existing_version = None

    if output_dir.exists():  # pragma: no branch
        if version_file.exists():  # pragma: no branch
            with open(version_file) as fh:
                existing_version = fh.read().strip()

        if force_cleanup or existing_version != SAMPLE_DATA_VERSION:  # pragma: no branch
            logger.warning("Removing existing sample data")
            shutil.rmtree(output_dir)

    fetch_all_files(sample_data_registry, output_dir, symlink)

    # Write out the current sample data version to the copying as complete
    with open(output_dir / "version.txt", "w") as fh:
        fh.write(SAMPLE_DATA_VERSION)
