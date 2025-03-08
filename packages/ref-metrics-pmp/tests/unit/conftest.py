from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def pdo_example_dir() -> Path:
    return Path(__file__).parent / "test-data" / "pdo-example"
