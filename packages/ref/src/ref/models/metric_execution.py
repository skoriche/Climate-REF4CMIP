from typing import TYPE_CHECKING

from sqlalchemy import Column, ForeignKey, Table
from sqlalchemy.orm import Mapped, mapped_column, relationship

from ref.models.base import Base, CreatedUpdatedMixin

if TYPE_CHECKING:
    from ref.models.metric import Metric


class MetricExecution(CreatedUpdatedMixin, Base):
    """
    Represents a run of a metric calculation

    The metric_id, key form an identifier of a unique group
    """

    __tablename__ = "metric_run"

    id: Mapped[int] = mapped_column(primary_key=True)

    retracted: Mapped[bool] = mapped_column(default=False)
    """
    Whether the metric run has been retracted or not

    This may happen if a dataset has been retracted.
    """

    metric_id: Mapped[int] = mapped_column(ForeignKey("metric.id"))
    """
    The metric that was run
    """

    key: Mapped[str] = mapped_column(index=True)
    """
    Key for the metric run

    This should be unique for each run of a metric.
    """

    successful: Mapped[bool] = mapped_column(nullable=True)
    """
    Was the run successful
    """

    path: Mapped[str] = mapped_column(nullable=True)
    """
    Path fragment relative to the output directory where the serialised output bundle is stored
    """

    metric: Mapped["Metric"] = relationship(back_populates="runs")


metric_datasets = Table(
    "metric_run_dataset",
    Base.metadata,
    Column("metric_run_id", ForeignKey("metric_run.id")),
    Column("dataset_id", ForeignKey("dataset.id")),
)
