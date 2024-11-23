"""
Re-useable fixtures etc. for tests that are shared across the whole project

See https://docs.pytest.org/en/7.1.x/reference/fixtures.html#conftest-py-sharing-fixtures-across-multiple-files
"""

import logging
from pathlib import Path

import esgpull
import pytest
from loguru import logger


@pytest.fixture
def esgf_data_dir() -> Path:
    pull = esgpull.Esgpull()

    return pull.config.paths.data


@pytest.fixture
def caplog(_caplog):
    class PropogateHandler(logging.Handler):
        def emit(self, record):
            logging.getLogger(record.name).handle(record)

    handler_id = logger.add(PropogateHandler(), format="{message}")
    yield _caplog
    logger.remove(handler_id)
