from pathlib import Path

import pytest

from cmip_ref.datasets.utils import validate_path
from cmip_ref_core.exceptions import OutOfTreeDatasetException


def test_validate_prefix_with_valid_relative_path(config):
    config.paths.data = Path("/data")

    raw_path = "/data/subfolder/file.csv"
    expected_path = Path("subfolder/file.csv")

    result = validate_path(config, raw_path)
    assert result == expected_path


def test_validate_prefix_with_allow_out_of_tree_datasets(config):
    config.paths.data = Path("/data")
    config.paths.allow_out_of_tree_datasets = True

    raw_path = "/other_dir/file.csv"
    expected_path = Path("/other_dir/file.csv")

    result = validate_path(config, raw_path)
    assert result == expected_path


def test_validate_prefix_with_invalid_relative_path(config):
    config.paths.data = Path("/data")
    config.paths.allow_out_of_tree_datasets = False

    raw_path = "/other_dir/file.csv"
    with pytest.raises(OutOfTreeDatasetException):
        validate_path(config, raw_path)
