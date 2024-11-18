import datetime
from typing import Any, ClassVar

from ref_core.datasets import SourceDatasetType
from sqlalchemy import ForeignKey, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from ref.models.base import Base


class Dataset(Base):
    """
    Represents a dataset

    A dataset is a collection of data files, that is used as an input to the benchmarking process.
    Adding/removing or updating a dataset will trigger a new metrics calculation.

    A polymorphic association is used to capture the different types of datasets as each
    dataset type may have different metadata fields.
    This enables the use of a single table to store all datasets,
    but still allows for querying specific metadata fields for each dataset type.
    """

    __tablename__ = "dataset"

    id: Mapped[int] = mapped_column(primary_key=True)
    slug: Mapped[str] = mapped_column(unique=True)
    """
    Globally unique identifier for the dataset.

    In the case of CMIP6 datasets, this is the instance_id.
    """
    dataset_type: Mapped[SourceDatasetType] = mapped_column(nullable=False)
    """
    Type of dataset
    """
    created_at: Mapped[datetime.datetime] = mapped_column(server_default=func.now())
    """
    When the dataset was added to the database
    """
    updated_at: Mapped[datetime.datetime] = mapped_column(server_default=func.now(), onupdate=func.now())
    """
    When the dataset was updated.

    Updating a dataset will trigger a new metrics calculation.
    """

    def __repr__(self) -> str:
        return f"<Dataset slug={self.slug} dataset_type={self.dataset_type} >"

    __mapper_args__: ClassVar[Any] = {"polymorphic_on": dataset_type}


class CMIP6Dataset(Dataset):
    """
    Represents a CMIP6 dataset

    TODO: Should the metadata fields be part of the file or dataset?
    """

    __tablename__ = "cmip6_dataset"
    id: Mapped[int] = mapped_column(ForeignKey("dataset.id"), primary_key=True)

    __mapper_args__: ClassVar[Any] = {"polymorphic_identity": SourceDatasetType.CMIP6}


class CMIP6File(Base):
    """
    Capture metadata for a file in an CMIP6 dataset

    A dataset may have multiple files, but is represented as a single dataset in the database.
    A lot of the metadata will be duplicated for each file in the dataset,
    but this will be more efficient for querying, filtering and building a data catalog.
    """

    __tablename__ = "cmip6_dataset_file"

    id: Mapped[int] = mapped_column(primary_key=True)
    dataset_id: Mapped[int] = mapped_column(
        ForeignKey("cmip6_dataset.id", ondelete="CASCADE"), nullable=False
    )
    """
    Foreign key to the dataset table
    """

    instance_id: Mapped[str] = mapped_column()
    """
    Unique identifier for the dataset.
    """

    # CMIP6 metadata fields to track
    # We might need to make these fields optional
    activity_id: Mapped[str] = mapped_column()
    branch_method: Mapped[str] = mapped_column()
    branch_time_in_child: Mapped[float] = mapped_column()
    branch_time_in_parent: Mapped[float] = mapped_column()
    end_time: Mapped[datetime.datetime] = mapped_column(nullable=True)
    experiment: Mapped[str] = mapped_column()
    experiment_id: Mapped[str] = mapped_column()
    frequency: Mapped[str] = mapped_column()
    grid: Mapped[str] = mapped_column()
    grid_label: Mapped[str] = mapped_column()
    institution_id: Mapped[str] = mapped_column()
    member_id: Mapped[str] = mapped_column()
    nominal_resolution: Mapped[str] = mapped_column()
    parent_activity_id: Mapped[str] = mapped_column()
    parent_experiment_id: Mapped[str] = mapped_column()
    parent_source_id: Mapped[str] = mapped_column()
    parent_time_units: Mapped[str] = mapped_column()
    parent_variant_label: Mapped[str] = mapped_column()
    realm: Mapped[str] = mapped_column()
    product: Mapped[str] = mapped_column()
    source_id: Mapped[str] = mapped_column()
    source_type: Mapped[str] = mapped_column()
    start_time: Mapped[datetime.datetime] = mapped_column(nullable=True)
    sub_experiment: Mapped[str] = mapped_column()
    sub_experiment_id: Mapped[str] = mapped_column()
    table_id: Mapped[str] = mapped_column()
    variable_id: Mapped[str] = mapped_column()
    variant_label: Mapped[str] = mapped_column()
    version: Mapped[str] = mapped_column()

    prefix: Mapped[str] = mapped_column()
    """
    Prefix that describes where the dataset is stored relative to the data directory
    """

    dataset = relationship("CMIP6Dataset", backref="files")

    @staticmethod
    def build(**kwargs) -> "CMIP6File":
        """
        Build a new CMIP6Dataset instance

        Parameters
        ----------
        kwargs
            Metadata fields for the dataset

        Returns
        -------
        :
            A new CMIP6Dataset instance
        """
        kwargs_to_drop = [key for key in kwargs if key not in CMIP6File.__table__.columns.keys()]
        for kw in kwargs_to_drop:
            kwargs.pop(kw, None)

        return CMIP6File(**kwargs)
