from pathlib import Path

import pandas as pd
from sqlalchemy.orm import joinedload

from cmip_ref.database import Database
from cmip_ref.models import Dataset, Table


def validate_path(raw_path: str) -> Path:
    """
    Validate the prefix of a dataset against the data directory
    """
    prefix = Path(raw_path)

    if not prefix.exists():
        raise FileNotFoundError(prefix)

    if not prefix.is_absolute():
        raise ValueError(f"Path {prefix} must be absolute")

    return prefix


def load_catalog_with_files(  # noqa: PLR0913
    db: Database,
    DatasetModel: type[Table],
    FileModel: type[Table],
    dataset_specific_metadata: Iterable[str],
    file_specific_metadata: Iterable[str],
    include_files: bool = True,
    limit: int | None = None,
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
    # TODO: Paginate this query to avoid loading all the data at once
    if include_files:
        result = (
            db.session.query(FileModel)
            # The join is necessary to be able to order by the dataset columns
            .join(FileModel.dataset)
            # The joinedload is necessary to avoid N+1 queries (one for each dataset)
            # https://docs.sqlalchemy.org/en/14/orm/loading_relationships.html#the-zen-of-joined-eager-loading
            .options(joinedload(FileModel.dataset))
            .order_by(Dataset.updated_at.desc())
            .limit(limit)
            .all()
        )

        return pd.DataFrame(
            [
                {
                    **{k: getattr(file, k) for k in file_specific_metadata},
                    **{k: getattr(file.dataset, k) for k in dataset_specific_metadata},
                }
                for file in result
            ],
            index=[file.dataset.id for file in result],
        )
    else:
        result_datasets = (
            db.session.query(DatasetModel).order_by(Dataset.updated_at.desc()).limit(limit).all()
        )

        return pd.DataFrame(
            [{k: getattr(dataset, k) for k in dataset_specific_metadata} for dataset in result_datasets],
            index=[file.id for file in result_datasets],
        )
