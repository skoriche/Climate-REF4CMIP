"""
Data registries for non-published reference data

These data are placeholders until these data have been added to obs4MIPs.
The AR7 FT REF requires that reference datasets are openly licensed before it is included
in any published data catalogs.
"""

import importlib.resources
import os
import pathlib
import shutil

import pooch
from loguru import logger


def fetch_all_files(registry: pooch.Pooch, output_dir: pathlib.Path | None, symlink: bool = False) -> None:
    """
    Fetch all files associated with a pooch registry and write them to an output directory.

    Pooch fetches, caches and validates the downloaded files.
    Subsequent calls to this function will not refetch any previously downloaded files.

    Parameters
    ----------
    registry
        Pooch directory containing a set of files that should be fetched.
    output_dir
        The root directory to write the files to.

        The directory will be created if it doesn't exist,
        and matching files will be overwritten.

        If no directory is provided, the files will be fetched from the remote server,
        but not copied anywhere.
    symlink
        If True, symlink all files to this directory.
        Otherwise, perform a copy.
    """
    if output_dir:
        output_dir.mkdir(parents=True, exist_ok=True)

    for key in registry.registry.keys():
        fetch_file = registry.fetch(key)

        if output_dir is None:
            # Just warm the cache and move onto the next file
            continue

        linked_file = output_dir / key
        linked_file.parent.mkdir(parents=True, exist_ok=True)
        if not linked_file.exists():  # pragma: no cover
            if symlink:
                logger.info(f"Linking {key} to {linked_file}")

                os.symlink(fetch_file, linked_file)
            else:
                logger.info(f"Copying {key} to {linked_file}")
                shutil.copy(fetch_file, linked_file)
        else:
            logger.info(f"File {linked_file} already exists. Skipping.")


class ReferenceDataRegistry:
    """
    Registry of reference datasets

    This is a singleton class that contains a registry of reference datasets

    """

    def __init__(self) -> None:
        self._registries: dict[str, pooch.Pooch] = {}

    def __getitem__(self, item: str) -> pooch.Pooch:
        """
        Get a pooch registry by name
        """
        return self._registries[item]

    def register(  # noqa: PLR0913
        self,
        name: str,
        base_url: str,
        package: str,
        resource: str,
        cache_name: str | None = None,
        version: str | None = None,
    ) -> None:
        """
        Build a pooch registry of reference data

        This will create a new registry and add it to the list of registries.
        This is typically used by a provider to register a new collections of datasets.

        Parameters
        ----------
        name
            Name of the registry

            This is used to identify the registry
        base_url
            Commmon URL prefix for the files
        package
            Name of the package containing the registry resource.
        resource
            Name of the resource in the package that contains a list of files and checksums.

            This must be formatted in a way that is expected by pooch.
        version
            The version of the data.

            Changing the version will invalidate the cache and force a re-download of the data.
        cache_name
            Name to use to generate the cache directory.

            This defaults to the value of `name` if not provided.
        """
        if cache_name is None:
            cache_name = "ref"

        registry = pooch.create(
            path=pooch.os_cache(cache_name),
            base_url=base_url,
            version=version,
            env="REF_METRICS_DATA_DIR",
        )
        registry.load_registry(str(importlib.resources.files(package) / resource))
        self._registries[name] = registry


data_registry = ReferenceDataRegistry()
