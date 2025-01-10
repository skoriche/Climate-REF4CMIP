from pathlib import Path

from cmip_ref_core.exceptions import OutOfTreeDatasetException
from loguru import logger

from cmip_ref.config import Config


def validate_path(config: Config, raw_path: str) -> Path:
    """
    Validate the prefix of a dataset against the data directory
    """
    prefix = Path(raw_path)

    # Check if the prefix is relative to the data directory
    if prefix.is_relative_to(config.paths.data):
        prefix = prefix.relative_to(config.paths.data)
    elif config.paths.allow_out_of_tree_datasets:
        logger.warning(f"Dataset {prefix} is not relative to {config.paths.data}")
    else:
        raise OutOfTreeDatasetException(prefix, config.paths.data)

    return prefix
