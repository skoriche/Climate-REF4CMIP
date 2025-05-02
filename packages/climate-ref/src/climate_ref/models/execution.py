import enum
import pathlib
from typing import TYPE_CHECKING, Any

from loguru import logger
from sqlalchemy import Column, ForeignKey, Table, UniqueConstraint, func
from sqlalchemy.orm import Mapped, Session, mapped_column, relationship
from sqlalchemy.orm.query import RowReturningQuery

from climate_ref.models import Dataset
from climate_ref.models.base import Base, CreatedUpdatedMixin
from climate_ref_core.datasets import ExecutionDatasetCollection

if TYPE_CHECKING:
    from climate_ref.database import Database
    from climate_ref.models.diagnostic import Diagnostic
    from climate_ref.models.metric_value import MetricValue


class ExecutionGroup(CreatedUpdatedMixin, Base):
    """
    Represents a group of executions with a shared set of input datasets.

    When solving, the `ExecutionGroup`s are derived from the available datasets,
    the defined diagnostics and their data requirements. From the information in the
    group an execution can be triggered, which is an actual run of a diagnostic calculation
    with a specific set of input datasets.

    When the `ExecutionGroup` is created, it is marked dirty, meaning there are no
    current executions available. When an Execution was run successfully for a
    ExecutionGroup, the dirty mark is removed. After ingesting new data and
    solving again and if new versions of the input datasets are available, the
    ExecutionGroup will be marked dirty again.

    The diagnostic_id and key form a unique identifier for `ExecutionGroup`s.
    """

    __tablename__ = "execution_group"
    __table_args__ = (UniqueConstraint("diagnostic_id", "key", name="execution_ident"),)

    id: Mapped[int] = mapped_column(primary_key=True)

    diagnostic_id: Mapped[int] = mapped_column(ForeignKey("diagnostic.id"))
    """
    The diagnostic that this execution group belongs to
    """

    key: Mapped[str] = mapped_column(index=True)
    """
    Key for the datasets in this Execution group.
    """

    dirty: Mapped[bool] = mapped_column(default=False)
    """
    Whether the execution group should be rerun

    An execution group is dirty if the diagnostic or any of the input datasets has been
    updated since the last execution.
    """

    selectors: Mapped[dict[str, Any]] = mapped_column(default=dict)
    """
    Collection of selectors that define the group

    These selectors are the unique key, value pairs that were selected during the initial groupby
    operation.
    These are also used to define the dataset key.
    """

    diagnostic: Mapped["Diagnostic"] = relationship(back_populates="execution_groups")
    executions: Mapped[list["Execution"]] = relationship(
        back_populates="execution_group", order_by="Execution.created_at"
    )

    def should_run(self, dataset_hash: str) -> bool:
        """
        Check if the diagnostic execution group needs to be executed.

        The diagnostic execution group should be run if:

        * the execution group is marked as dirty
        * no executions have been performed ever
        * the dataset hash is different from the last run
        """
        if not self.executions:
            logger.debug(f"Execution group {self.diagnostic.slug}/{self.key} was never executed")
            return True

        if self.executions[-1].dataset_hash != dataset_hash:
            logger.debug(
                f"Execution group {self.diagnostic.slug}/{self.key} hash mismatch:"
                f" {self.executions[-1].dataset_hash} != {dataset_hash}"
            )
            return True

        if self.dirty:
            logger.debug(f"Execution group {self.diagnostic.slug}/{self.key} is dirty")
            return True

        return False


execution_datasets = Table(
    "execution_dataset",
    Base.metadata,
    Column("execution_id", ForeignKey("execution.id")),
    Column("dataset_id", ForeignKey("dataset.id")),
)


class Execution(CreatedUpdatedMixin, Base):
    """
    Represents a single execution of a diagnostic

    Each result is part of a group of executions that share similar input datasets.

    An execution group might be run multiple times as new data becomes available,
    each run will create a `Execution`.
    """

    __tablename__ = "execution"

    id: Mapped[int] = mapped_column(primary_key=True)

    output_fragment: Mapped[str] = mapped_column()
    """
    Relative directory to store the output of the execution.

    During execution this directory is relative to the temporary directory.
    If the diagnostic execution is successful, the executions will be moved to the final output directory
    and the temporary directory will be cleaned up.
    This directory may contain multiple input and output files.
    """

    execution_group_id: Mapped[int] = mapped_column(
        ForeignKey(
            "execution_group.id",
            name="fk_execution_id",
        )
    )
    """
    The execution group that this execution belongs to
    """

    dataset_hash: Mapped[str] = mapped_column(index=True)
    """
    Hash of the datasets used to calculate the diagnostic

    This is used to verify if an existing diagnostic execution has been run with the same datasets.
    """

    successful: Mapped[bool] = mapped_column(nullable=True)
    """
    Was the run successful
    """

    path: Mapped[str] = mapped_column(nullable=True)
    """
    Path to the output bundle

    Relative to the diagnostic execution result output directory
    """

    retracted: Mapped[bool] = mapped_column(default=False)
    """
    Whether the diagnostic execution result has been retracted or not

    This may happen if a dataset has been retracted, or if the diagnostic execution was incorrect.
    Rather than delete the values, they are marked as retracted.
    These data may still be visible in the UI, but should be marked as retracted.
    """

    execution_group: Mapped["ExecutionGroup"] = relationship(back_populates="executions")
    outputs: Mapped[list["ExecutionOutput"]] = relationship(back_populates="execution")
    values: Mapped[list["MetricValue"]] = relationship(back_populates="execution")

    datasets: Mapped[list[Dataset]] = relationship(secondary=execution_datasets)
    """
    The datasets used in this execution
    """

    def register_datasets(self, db: "Database", execution_dataset: ExecutionDatasetCollection) -> None:
        """
        Register the datasets used in the diagnostic calculation with the execution
        """
        for _, dataset in execution_dataset.items():
            db.session.execute(
                execution_datasets.insert(),
                [{"execution_id": self.id, "dataset_id": idx} for idx in dataset.index],
            )

    def mark_successful(self, path: pathlib.Path | str) -> None:
        """
        Mark the diagnostic execution as successful
        """
        # TODO: this needs to accept both a diagnostic and output bundle
        self.successful = True
        self.path = str(path)

    def mark_failed(self) -> None:
        """
        Mark the diagnostic execution as unsuccessful
        """
        self.successful = False


class ResultOutputType(enum.Enum):
    """
    Types of supported outputs

    These map to the categories of output in the CMEC output bundle
    """

    Plot = "plot"
    Data = "data"
    HTML = "html"


class ExecutionOutput(CreatedUpdatedMixin, Base):
    """
    An output generated as part of an execution.

    This output may be a plot, data file or HTML file.
    These outputs are defined in the CMEC output bundle
    """

    __tablename__ = "execution_output"

    id: Mapped[int] = mapped_column(primary_key=True)

    execution_id: Mapped[int] = mapped_column(ForeignKey("execution.id"), index=True)

    output_type: Mapped[ResultOutputType] = mapped_column(index=True)
    """
    Type of the output

    This will determine how the output is displayed
    """

    filename: Mapped[str] = mapped_column(nullable=True)
    """
    Path to the output

    Relative to the diagnostic execution result output directory
    """

    short_name: Mapped[str] = mapped_column(nullable=True)
    """
    Short key of the output

    This is unique for a given result and output type
    """

    long_name: Mapped[str] = mapped_column(nullable=True)
    """
    Human readable name describing the plot
    """

    description: Mapped[str] = mapped_column(nullable=True)
    """
    Long description describing the plot
    """

    execution: Mapped["Execution"] = relationship(back_populates="outputs")


def get_execution_group_and_latest(
    session: Session,
) -> RowReturningQuery[tuple[ExecutionGroup, Execution | None]]:
    """
    Query to get the most recent result for each execution group

    Parameters
    ----------
    session
        The database session to use for the query.

    Returns
    -------
        Query to get the most recent result for each execution group.
        The result is a tuple of the execution group and the most recent result,
        which can be None.
    """
    # Find the most recent result for each execution group
    # This uses an aggregate function because it is more efficient than order by
    subquery = (
        session.query(
            Execution.execution_group_id,
            func.max(Execution.created_at).label("latest_created_at"),
        )
        .group_by(Execution.execution_group_id)
        .subquery()
    )

    # Join the diagnostic execution with the latest result
    query = (
        session.query(ExecutionGroup, Execution)
        .outerjoin(subquery, ExecutionGroup.id == subquery.c.execution_group_id)
        .outerjoin(
            Execution,
            (Execution.execution_group_id == ExecutionGroup.id)
            & (Execution.created_at == subquery.c.latest_created_at),
        )
    )

    return query  # type: ignore
