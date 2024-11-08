from sqlalchemy.orm import Mapped, mapped_column

from ref.models.base import Base


class Dataset(Base):
    """
    Represents a dataset
    """

    __tablename__ = "dataset"

    dataset_id: Mapped[str] = mapped_column(primary_key=True)
    """
    Complete dataset identifier

    Includes the data node and version
    """
    instance_id: Mapped[str]
    """
    Unique identifier for the dataset
    """
    master_id: Mapped[str]
    """
    Identifer for the dataset (excluding version)
    """
    version: Mapped[str]
    """
    Version of the dataset
    """
    data_node: Mapped[str]
    """
    Data node where the dataset is stored
    """
    size: Mapped[int]
    """
    Size of the dataset in bytes
    """
    number_of_files: Mapped[int]

    # Should we also track the following fields?
    #  variable_id, table_id, institution_id, model_id, experiment_id, source_id, member_id, grid_label,
    #  time_range, time_frequency, realm,retracted

    def __repr__(self):
        return f"<Dataset dataset_id={self.dataset_id}>"
