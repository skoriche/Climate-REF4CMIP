from sqlalchemy import Column, ForeignKey, Table
from sqlalchemy.orm import Mapped, mapped_column, relationship

from ref.models.base import Base, CreatedUpdatedMixin


class Metric(CreatedUpdatedMixin, Base):
    """
    Represents a provider that can provide metric calculations
    """

    __tablename__ = "metric"

    id: Mapped[int] = mapped_column(primary_key=True)
    slug: Mapped[str] = mapped_column(unique=True)
    """
    Globally unique identifier for the metric.

    This will be used to reference the metric in the benchmarking process
    """

    name: Mapped[str] = mapped_column()
    """
    Long name of the metric
    """

    version: Mapped[str] = mapped_column(nullable=False)
    """
    Version of the metric.

    This may not update on every provider update, but should be updated when the metric is updated.
    """

    provider_id: Mapped[int] = mapped_column(ForeignKey("provider.id"))
    """
    The provider that provides the metric
    """
    provider = relationship("Provider", back_populates="metrics")

    enabled: Mapped[bool] = mapped_column(default=True)
    """
    Whether the metric is enabled or not

    If a metric is not enabled, it will not be used for any calculations.
    """

    def __repr__(self) -> str:
        return f"<Metric slug={self.slug} version={self.version}>"

    def full_slug(self) -> str:
        """
        Get the full slug of the metric, including the provider slug

        Returns
        -------
        str
            Full slug of the metric
        """
        return f"{self.provider.slug}/{self.slug}"


class MetricRun(CreatedUpdatedMixin, Base):
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
