import datetime
from typing import Any, ClassVar

from ref_core.datasets import SourceDatasetType
from sqlalchemy import func
from sqlalchemy.orm import Mapped, Session, mapped_column, relationship

from ref.models.base import Base


class Dataset(Base):
    """
    Represents a dataset

    A dataset is a collection of data files, that is used as a

    A polymorphic association is used to capture the different types of datasets as each
    dataset type may have different metadata fields.
    This enables the use of a single table to store all datasets,
    but still allows for querying specific metadata fields for each dataset type.
    """

    __tablename__ = "dataset"

    id: Mapped[int] = mapped_column(primary_key=True)
    dataset_type: Mapped[SourceDatasetType] = mapped_column(nullable=False)
    """
    Type of dataset
    """
    dataset_specific_id: Mapped[int] = mapped_column(nullable=False)
    """
    ID of the dataset in the source database
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
    prefix: Mapped[str] = mapped_column()
    """
    Prefix that describes where the dataset is stored relative to the data directory
    """

    # Polymorphic identity to map subclasses
    __mapper_args__: ClassVar[Any] = {
        "polymorphic_on": dataset_type,
        "polymorphic_identity": "base_dataset",
    }

    def __repr__(self) -> str:
        return f"<Dataset id={self.id} dataset_type={self.dataset_type} >"


class CMIP6Dataset(Base):
    """
    Capture metadata for a CMIP6 dataset
    """

    __tablename__ = "cmip6_dataset"
    id: Mapped[int] = mapped_column(primary_key=True)
    dataset = relationship("Dataset", backref="genomic_metadata", uselist=False)
    instance_id: Mapped[str] = mapped_column(unique=True)
    """
    Unique identifier for the dataset.
    """

    # CMIP6 metadata fields to track
    activity_id: Mapped[str] = mapped_column()
    branch_method: Mapped[str] = mapped_column()
    branch_time_in_child: Mapped[str] = mapped_column()
    branch_time_in_parent: Mapped[str] = mapped_column()
    experiment: Mapped[str] = mapped_column()
    experiment_id: Mapped[str] = mapped_column()
    frequency: Mapped[str] = mapped_column()
    grid: Mapped[str] = mapped_column()
    grid_label: Mapped[str] = mapped_column()
    institution_id: Mapped[str] = mapped_column()
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
    sub_experiment: Mapped[str] = mapped_column()
    sub_experiment_id: Mapped[str] = mapped_column()
    table_id: Mapped[str] = mapped_column()
    variable_id: Mapped[str] = mapped_column()
    variant_label: Mapped[str] = mapped_column()

    # Polymorphic mapping
    __mapper_args__: ClassVar[Any] = {
        "polymorphic_identity": SourceDatasetType.CMIP6,
        "concrete": True,
    }

    def save(self, session: Session, commit: bool = True) -> None:
        """
        Save the dataset to the database

        Parameters
        ----------
        session
            The database session to use
        commit
            If True, commit the transaction
        """
        session.add(self)
        session.flush()
        session.add(Dataset(dataset_type=SourceDatasetType.CMIP6, dataset_specific_id=self.id))

        if commit:
            session.commit()
