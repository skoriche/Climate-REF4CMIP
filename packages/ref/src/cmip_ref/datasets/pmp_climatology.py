from __future__ import annotations

import pandas as pd
from loguru import logger

from cmip_ref.config import Config
from cmip_ref.database import Database
from cmip_ref.datasets.obs4mips import Obs4MIPsDatasetAdapter
from cmip_ref.datasets.utils import load_catalog_with_files, validate_path
from cmip_ref.models.dataset import PMPClimatologyDataset, PMPClimatologyFile
from cmip_ref_core.exceptions import RefException


class PMPClimsDatasetAdapter(Obs4MIPsDatasetAdapter):
    """
    Adapter for climatology datasets post-processed from obs4MIPs datasets by PMP.

    These data look like obs4MIPs datasets so are handdled
    """

    dataset_cls = PMPClimatologyDataset
    file_cls = PMPClimatologyFile

    def register_dataset(
        self, config: Config, db: Database, data_catalog_dataset: pd.DataFrame
    ) -> PMPClimatologyDataset | None:
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
        dataset, created = db.get_or_create(self.dataset_cls, slug=slug, **dataset_metadata)
        if not created:
            logger.warning(f"{dataset} already exists in the database. Skipping")
            return None
        db.session.flush()
        for dataset_file in data_catalog_dataset.to_dict(orient="records"):
            path = validate_path(dataset_file.pop("path"))

            db.session.add(
                PMPClimatologyFile(
                    path=str(path),
                    dataset_id=dataset.id,
                    start_time=dataset_file.pop("start_time"),
                    end_time=dataset_file.pop("end_time"),
                )
            )
        return dataset

    def load_catalog(
        self, db: Database, include_files: bool = True, limit: int | None = None
    ) -> pd.DataFrame:
        """
        Load the data catalog containing the currently tracked datasets/files from the database

        Iterating over different datasets within the data catalog can be done using a `groupby`
        operation for the `instance_id` column.

        The index of the data catalog is the primary key of the dataset.
        This should be maintained during any processing.

        Returns
        -------
        :
            Data catalog containing the metadata for the currently ingested datasets
        """
        return load_catalog_with_files(
            db,
            PMPClimatologyDataset,
            PMPClimatologyFile,
            self.dataset_specific_metadata,
            self.file_specific_metadata,
            include_files,
            limit,
        )
