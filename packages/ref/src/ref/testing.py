import importlib.resources
import os
import shutil
from pathlib import Path

import pooch
from loguru import logger


def _determine_test_directory() -> Path | None:
    expected = Path(__file__).parents[4] / "tests" / "test-data"

    if not expected.exists():
        return None
    return expected


def _build_sample_data_registry(sample_data_version: str) -> pooch.Pooch:
    registry = pooch.create(
        path=pooch.os_cache("ref_sample_data"),
        base_url="https://raw.githubusercontent.com/CMIP-REF/ref-sample-data/refs/tags/{version}/data/",
        version=sample_data_version,
        env="REF_SAMPLE_DATA_DIR",
    )

    with importlib.resources.files("ref").joinpath("datasets").joinpath("sample_data.txt").open("rb") as fh:
        registry.load_registry(fh)

    return registry


TEST_DATA_DIR = _determine_test_directory()
SAMPLE_DATA_VERSION = "v0.2.0"


def fetch_sample_data(version: str = SAMPLE_DATA_VERSION) -> None:
    sample_registry = _build_sample_data_registry(version)

    output_dir = TEST_DATA_DIR / "sample-data"

    if output_dir.exists():
        logger.warning("Removing existing sample data")
        shutil.rmtree(output_dir)

    for key in sample_registry.registry.keys():
        fetch_file = sample_registry.fetch(key)

        if TEST_DATA_DIR:
            linked_file = output_dir / key
            linked_file.parent.mkdir(parents=True, exist_ok=True)
            logger.info(f"Creating symlink for {linked_file}")
            os.symlink(fetch_file, linked_file)
