from typing import TYPE_CHECKING, Any, ClassVar

from sqlalchemy import Column, ForeignKey, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from cmip_ref.models.base import Base, CreatedUpdatedMixin
from cmip_ref_core.pycmec.controlled_vocabulary import CV, Dimension

if TYPE_CHECKING:
    from cmip_ref.models.metric_execution import MetricExecutionResult


class MetricValue(CreatedUpdatedMixin, Base):
    """
    Represents a single metric value

    This value has a number of dimensions which are used to query the metric value.
    These dimensions describe aspects such as the type of statistic being measured,
    the region of interest or the model from which the statistic is being measured.
    """

    __tablename__ = "metric_value"

    id: Mapped[int] = mapped_column(primary_key=True)
    metric_execution_result_id: Mapped[int] = mapped_column(ForeignKey("metric_execution_result.id"))

    value: Mapped[float] = mapped_column()
    attributes: Mapped[dict[str, Any]] = mapped_column()

    metric_execution_result: Mapped["MetricExecutionResult"] = relationship(back_populates="values")

    _dimension_columns: ClassVar[list[str]] = []

    @property
    def dimensions(self) -> dict[str, str]:
        """
        Get the non-null dimensions and their values

        Any changes to the resulting dictionary are not reflected in the object

        Returns
        -------
            Collection of dimensions names and their values
        """
        dims = {}
        for key in self._dimension_columns:
            value = getattr(self, key)
            if value is not None:
                dims[key] = value
        return dims

    def __repr__(self) -> str:
        return f"<MetricValue metric_execution={self.metric_execution_result} value={self.value}>"

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
            An instance of a sqlalchemy Column

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
        for dimension in cv.dimensions:
            if not hasattr(cls, dimension.name):
                setattr(cls, dimension.name, cls.build_dimension_column(dimension))
                cls._dimension_columns.append(dimension.name)

    @classmethod
    def build(
        cls,
        *,
        metric_execution_result_id: int,
        value: float,
        dimensions: dict[str, str],
        attributes: dict[str, Any] | None,
    ) -> "MetricValue":
        """
        Build a MetricValue from a collection of dimensions and a value

        This is a helper method that validates the dimensions supplied and provides an interface
        similar to [cmip_ref_core.pycmec.metrics.MetricValue][].

        Parameters
        ----------
        metric_execution_result_id
            Execution result that created the metric value
        value
            The value of the metric
        dimensions
            Dimensions that describe the metric execution result
        attributes
            Optional additional attributes to describe the value,
            but are not in the controlled vocabulary.

        Returns
        -------
            Newly created MetricValue
        """
        for k in dimensions:
            if k not in cls._dimension_columns:
                raise ValueError(f"Unknown dimension column '{k}'")

        return MetricValue(
            metric_execution_result_id=metric_execution_result_id,
            value=value,
            attributes=attributes,
            **dimensions,
        )
