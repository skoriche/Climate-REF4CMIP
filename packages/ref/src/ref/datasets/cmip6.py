from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import ecgtools.parsers
import pandas as pd
from ecgtools import Builder
from loguru import logger
from ref_core.exceptions import RefException

from ref.cli.ingest import validate_path
from ref.config import Config
from ref.database import Database
from ref.datasets.base import DatasetAdapter
from ref.models.dataset import CMIP6Dataset, CMIP6File


def _parse_datetime(dt_str: pd.Series[str]) -> pd.Series[datetime | Any]:
    """
    Pandas tries to coerce everything to their own datetime format, which is not what we want here.
    """
    return pd.Series(
        [datetime.strptime(dt, "%Y-%m-%d %H:%M:%S") if dt else None for dt in dt_str],
        index=dt_str.index,
        dtype="object",
    )


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
        "standard_name",
        "long_name",
        "units",
        "vertical_levels",
        "init_year",
        "version",
        slug_column,
    )

    file_specific_metadata = ("start_time", "end_time", "path")

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
        builder = Builder(
            paths=[str(file_or_directory)],
            depth=10,
            include_patterns=["*.nc"],
            # joblib_parallel_kwargs={"n_jobs": 1},
        ).build(parsing_func=ecgtools.parsers.parse_cmip6)

        datasets = builder.df

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
        ]
        datasets["instance_id"] = datasets.apply(
            lambda row: "CMIP6." + ".".join([row[item] for item in drs_items]), axis=1
        )

        return datasets

    def register_dataset(
        self, config: Config, db: Database, data_catalog_dataset: pd.DataFrame
    ) -> CMIP6Dataset | None:
        """
        Register a dataset in the database using the data catalog

        Parameters
        ----------
        config
            Configuration object
        db
            Database instance
        data_catalog_dataset
            A subset of the data catalog containing the metadata for a single dataset

        Returns
        -------
        :
            Registered dataset if successful, else None
        """
        self.validate_data_catalog(data_catalog_dataset)

        unique_slugs = data_catalog_dataset[self.slug_column].unique()
        if len(unique_slugs) != 1:
            raise RefException(f"Found multiple datasets in the same directory: {unique_slugs}")
        slug = unique_slugs[0]

        dataset_metadata = data_catalog_dataset[list(self.dataset_specific_metadata)].iloc[0].to_dict()
        dataset, created = db.get_or_create(CMIP6Dataset, slug=slug, **dataset_metadata)

        if not created:
            logger.warning(f"{dataset} already exists in the database. Skipping")
            return None

        db.session.flush()

        for dataset_file in data_catalog_dataset.to_dict(orient="records"):
            path = validate_path(config, dataset_file.pop("path"))

            db.session.add(
                CMIP6File(
                    path=str(path),
                    dataset_id=dataset.id,
                    start_time=dataset_file.pop("start_time"),
                    end_time=dataset_file.pop("end_time"),
                )
            )

        return dataset
