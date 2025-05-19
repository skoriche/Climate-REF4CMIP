from __future__ import annotations

import traceback
import warnings
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import xarray as xr
from ecgtools import Builder
from ecgtools.parsers.utilities import extract_attr_with_regex  # type: ignore
from loguru import logger

from climate_ref.datasets.base import DatasetAdapter
from climate_ref.models.dataset import CMIP6Dataset


def _parse_datetime(dt_str: pd.Series[str]) -> pd.Series[datetime | Any]:
    """
    Pandas tries to coerce everything to their own datetime format, which is not what we want here.
    """

    def _inner(date_string: str | None) -> datetime | None:
        if not date_string:
            return None

        # Try to parse the date string with and without milliseconds
        try:
            dt = datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            dt = datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S.%f")

        return dt

    return pd.Series(
        [_inner(dt) for dt in dt_str],
        index=dt_str.index,
        dtype="object",
    )


def _apply_fixes(data_catalog: pd.DataFrame) -> pd.DataFrame:
    def _fix_parent_variant_label(group: pd.DataFrame) -> pd.DataFrame:
        if group["parent_variant_label"].nunique() == 1:
            return group
        group["parent_variant_label"] = group["variant_label"].iloc[0]

        return group

    data_catalog = (
        data_catalog.groupby("instance_id")
        .apply(_fix_parent_variant_label, include_groups=False)
        .reset_index(level="instance_id")
    )

    if "branch_time_in_child" in data_catalog:
        data_catalog["branch_time_in_child"] = _clean_branch_time(data_catalog["branch_time_in_child"])
    if "branch_time_in_parent" in data_catalog:
        data_catalog["branch_time_in_parent"] = _clean_branch_time(data_catalog["branch_time_in_parent"])

    return data_catalog


def _clean_branch_time(branch_time: pd.Series[str]) -> pd.Series[float]:
    # EC-Earth3 uses "D" as a suffix for the branch_time_in_child and branch_time_in_parent columns
    # Handle missing values (these result in nan values)
    return pd.to_numeric(branch_time.astype(str).str.replace("D", ""), errors="coerce")


def parse_cmip6(file: str) -> dict[str, Any]:
    """
    Parser for CMIP6

    This function parses the CMIP6 dataset and returns a dictionary with the metadata.
    This was copied from the ecgtools package, but we want to log the exception when it fails.
    """
    keys = sorted(
        {
            "activity_id",
            "branch_method",
            "branch_time_in_child",
            "branch_time_in_parent",
            "experiment",
            "experiment_id",
            "frequency",
            "grid",
            "grid_label",
            "institution_id",
            "nominal_resolution",
            "parent_activity_id",
            "parent_experiment_id",
            "parent_source_id",
            "parent_time_units",
            "parent_variant_label",
            "realm",
            "product",
            "source_id",
            "source_type",
            "sub_experiment",
            "sub_experiment_id",
            "table_id",
            "variable_id",
            "variant_label",
        }
    )

    try:
        with xr.open_dataset(file, chunks={}, use_cftime=True) as ds:
            info = {key: ds.attrs.get(key) for key in keys}
            info["member_id"] = info["variant_label"]

            variable_id = info["variable_id"]
            if variable_id:  # pragma: no branch
                attrs = ds[variable_id].attrs
                for attr in ["standard_name", "long_name", "units"]:
                    info[attr] = attrs.get(attr)

            # Set the default of # of vertical levels to 1
            vertical_levels = 1
            start_time, end_time = None, None
            init_year = None
            try:
                vertical_levels = ds[ds.cf["vertical"].name].size
            except (KeyError, AttributeError, ValueError):
                ...

            try:
                start_time, end_time = str(ds.cf["T"][0].data), str(ds.cf["T"][-1].data)
            except (KeyError, AttributeError, ValueError):
                ...
            if info.get("sub_experiment_id"):  # pragma: no branch
                init_year = extract_attr_with_regex(info["sub_experiment_id"], r"\d{4}")
                if init_year:  # pragma: no cover
                    init_year = int(init_year)
            info["vertical_levels"] = vertical_levels
            info["init_year"] = init_year
            info["start_time"] = start_time
            info["end_time"] = end_time
            if not (start_time and end_time):
                info["time_range"] = None
            else:
                info["time_range"] = f"{start_time}-{end_time}"
        info["path"] = str(file)
        info["version"] = extract_attr_with_regex(str(file), regex=r"v\d{4}\d{2}\d{2}|v\d{1}") or "v0"
        return info

    except Exception:
        logger.exception(f"Failed to parse {file}")
        return {"INVALID_ASSET": file, "TRACEBACK": traceback.format_exc()}


class CMIP6DatasetAdapter(DatasetAdapter):
    """
    Adapter for CMIP6 datasets
    """

    dataset_cls = CMIP6Dataset
    slug_column = "instance_id"

    dataset_specific_metadata = (
        "activity_id",
        "branch_method",
        "branch_time_in_child",
        "branch_time_in_parent",
        "experiment",
        "experiment_id",
        "frequency",
        "grid",
        "grid_label",
        "institution_id",
        "nominal_resolution",
        "parent_activity_id",
        "parent_experiment_id",
        "parent_source_id",
        "parent_time_units",
        "parent_variant_label",
        "product",
        "realm",
        "source_id",
        "source_type",
        "sub_experiment",
        "sub_experiment_id",
        "table_id",
        "variable_id",
        "variant_label",
        "member_id",
        "vertical_levels",
        "version",
        # Variable identifiers
        "standard_name",
        "long_name",
        "units",
        slug_column,
    )

    file_specific_metadata = ("start_time", "end_time", "path")

    def __init__(self, n_jobs: int = 1):
        self.n_jobs = n_jobs

    def pretty_subset(self, data_catalog: pd.DataFrame) -> pd.DataFrame:
        """
        Get a subset of the data_catalog to pretty print

        This is particularly useful for CMIP6 datasets, which have a lot of metadata columns.

        Parameters
        ----------
        data_catalog
            Data catalog to subset

        Returns
        -------
        :
            Subset of the data catalog to pretty print

        """
        return data_catalog[
            [
                "activity_id",
                "institution_id",
                "source_id",
                "experiment_id",
                "member_id",
                "table_id",
                "variable_id",
                "grid_label",
                "version",
            ]
        ]

    def find_local_datasets(self, file_or_directory: Path) -> pd.DataFrame:
        """
        Generate a data catalog from the specified file or directory

        Each dataset may contain multiple files, which are represented as rows in the data catalog.
        Each dataset has a unique identifier, which is in `slug_column`.

        Parameters
        ----------
        file_or_directory
            File or directory containing the datasets

        Returns
        -------
        :
            Data catalog containing the metadata for the dataset
        """
        with warnings.catch_warnings():
            # Ignore the DeprecationWarning from xarray
            warnings.simplefilter("ignore", DeprecationWarning)

            builder = Builder(
                paths=[str(file_or_directory)],
                depth=10,
                include_patterns=["*.nc"],
                joblib_parallel_kwargs={"n_jobs": self.n_jobs},
            ).build(parsing_func=parse_cmip6)  # type: ignore

        datasets: pd.DataFrame = builder.df.drop(["init_year"], axis=1)

        # Convert the start_time and end_time columns to datetime objects
        # We don't know the calendar used in the dataset (TODO: Check what ecgtools does)
        datasets["start_time"] = _parse_datetime(datasets["start_time"])
        datasets["end_time"] = _parse_datetime(datasets["end_time"])

        drs_items = [
            "activity_id",
            "institution_id",
            "source_id",
            "experiment_id",
            "member_id",
            "table_id",
            "variable_id",
            "grid_label",
            "version",
        ]
        datasets["instance_id"] = datasets.apply(
            lambda row: "CMIP6." + ".".join([row[item] for item in drs_items]), axis=1
        )

        # Temporary fix for some datasets
        # TODO: Replace with a standalone package that contains metadata fixes for CMIP6 datasets
        datasets = _apply_fixes(datasets)

        return datasets
