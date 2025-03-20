from typing import TYPE_CHECKING, Any

from sqlalchemy import Column, ForeignKey, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from cmip_ref.models.base import Base, CreatedUpdatedMixin
from cmip_ref_core.pycmec.controlled_vocabulary import CV, Dimension

if TYPE_CHECKING:
    from cmip_ref.models.metric_execution import MetricExecution


class MetricValue(CreatedUpdatedMixin, Base):
    """
    Represents a single metric value
    """

    __tablename__ = "metric_value"

    id: Mapped[int] = mapped_column(primary_key=True)
    metric_execution_id: Mapped[int] = mapped_column(ForeignKey("metric_execution.id"))

    value: Mapped[float] = mapped_column()
    attributes: Mapped[dict[str, Any]] = mapped_column()

    metric_execution: Mapped["MetricExecution"] = relationship(back_populates="values")

    def __repr__(self) -> str:
        return f"<MetricValue metric_execution={self.metric_execution_id}>"

    @staticmethod
    def build_dimension_column(dimension: Dimension) -> Column[str]:
        """
        Create a column representing a CV dimension

        Any changes to this functionality likely require a manual database migration
        of the existing columns.

        Parameters
        ----------
        dimension
            Dimension to create the column for

        Returns
        -------
            An instance of an sqlalchemy Column

            This doesn't create the column in the database,
            but enables the ORM to access it.

        """
        return Column(dimension.name, Text, index=True, nullable=True)

    @classmethod
    def register_cv_dimensions(cls, cv: CV) -> None:
        """
        Register the dimensions supplied in the controlled vocabulary

        This has to be done at run-time to support custom CVs.
        Any extra columns already in the database, but not in the CV are ignored.

        Parameters
        ----------
        cv
            Controlled vocabulary being used
        """
        table = cls.__table__

        for dimension in cv.dimensions:
            table.append_column(cls.build_dimension_column(dimension))  # type: ignore
