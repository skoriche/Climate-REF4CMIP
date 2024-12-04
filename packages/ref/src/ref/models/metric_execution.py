from typing import TYPE_CHECKING

from sqlalchemy import Column, ForeignKey, Table
from sqlalchemy.orm import Mapped, mapped_column, relationship

from ref.models.base import Base, CreatedUpdatedMixin

if TYPE_CHECKING:
    from ref.models.metric import Metric


class MetricExecution(CreatedUpdatedMixin, Base):
    """
    Represents an execution of a metric calculation

    Each execution is a run of a metric calculation with a specific set of input datasets.
    The metric_id, key form an identifier of a unique group.
    """

    __tablename__ = "metric_execution"

    id: Mapped[int] = mapped_column(primary_key=True)

    retracted: Mapped[bool] = mapped_column(default=False)
    """
    Whether the metric execution has been retracted or not

    This may happen if a dataset has been retracted, or if the metric execution was incorrect.
    Rather than delete the values, they are marked as retracted.
    These data may still be visible in the UI, but should be marked as retracted.
    """

    metric_id: Mapped[int] = mapped_column(ForeignKey("metric.id"))
    """
    The target metric
    """

    key: Mapped[str] = mapped_column(index=True)
    """
    Key for the metric execution

    This should be unique for each run of a metric.
    """

    dirty: Mapped[bool] = mapped_column(default=False)
    """
    Whether the execution should be rerun

    An execution is dirty if the metric or any of the input datasets has been updated since the last run.
    """

    metric: Mapped["Metric"] = relationship(back_populates="executions")
    results: Mapped[list["MetricExecutionResult"]] = relationship(
        back_populates="metric_execution", order_by="MetricExecutionResult.created_at"
    )


class MetricExecutionResult(CreatedUpdatedMixin, Base):
    """
    Represents a run of a metric calculation

    The metric_id, key form an identifier of a unique group
    """

    __tablename__ = "metric_execution_result"

    id: Mapped[int] = mapped_column(primary_key=True)

    metric_execution_id: Mapped[int] = mapped_column(ForeignKey("metric_execution.id"))
    """
    The target metric execution
    """

    dataset_hash: Mapped[str] = mapped_column(index=True, unique=True)
    """
    Hash of the datasets used to calculate the metric

    This is used to verify if an existing metric execution has been run with the same datasets.
    """

    successful: Mapped[bool] = mapped_column(nullable=True)
    """
    Was the run successful
    """

    path: Mapped[str] = mapped_column(nullable=True)
    """
    Path to the output bundle
    """

    metric_execution: Mapped["MetricExecution"] = relationship(back_populates="results")


metric_datasets = Table(
    "metric_execution_result_dataset",
    Base.metadata,
    Column("metric_execution_result_id", ForeignKey("metric_execution_result.id")),
    Column("dataset_id", ForeignKey("dataset.id")),
)
