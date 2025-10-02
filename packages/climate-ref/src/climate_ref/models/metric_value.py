import enum
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, ClassVar

from sqlalchemy import ForeignKey, event
from sqlalchemy.orm import Mapped, mapped_column, relationship

from climate_ref.models.base import Base
from climate_ref.models.mixins import CreatedUpdatedMixin, DimensionMixin

if TYPE_CHECKING:
    from climate_ref.models.execution import Execution


class MetricValueType(enum.Enum):
    """
    Type of metric value

    This is used to determine how the metric value should be interpreted.
    """

    # The value is a single number
    SCALAR = "scalar"

    # The value is a list of numbers
    SERIES = "series"


class MetricValue(DimensionMixin, CreatedUpdatedMixin, Base):
    """
    Represents a single metric value

    This is a base class for different types of metric values (e.g. scalar, series) which
    are stored in a single table using single table inheritance.

    This value has a number of dimensions which are used to query the diagnostic values.
    These dimensions describe aspects such as the type of statistic being measured,
    the region of interest or the model from which the statistic is being measured.

    The columns in this table are not known statically because the REF can track an arbitrary
    set of dimensions depending on the controlled vocabulary that will be used.
    A call to `register_cv_dimensions` must be made before using this class.
    """

    __tablename__ = "metric_value"

    __mapper_args__: ClassVar[Mapping[str, str]] = {  # type: ignore
        "polymorphic_on": "type",
    }

    _cv_dimensions: ClassVar[list[str]] = []

    id: Mapped[int] = mapped_column(primary_key=True)
    execution_id: Mapped[int] = mapped_column(ForeignKey("execution.id"), index=True)

    attributes: Mapped[dict[str, Any]] = mapped_column()

    execution: Mapped["Execution"] = relationship(back_populates="values")

    type: Mapped[MetricValueType] = mapped_column(index=True)
    """
    Type of metric value

    This value is used to determine how the metric value should be interpreted.
    """

    def __repr__(self) -> str:
        return f"<MetricValue id={self.id} execution={self.execution} dimensions={self.dimensions}>"


class ScalarMetricValue(MetricValue):
    """
    A scalar value with an associated dimensions

    This is a subclass of MetricValue that is used to represent a scalar value.
    """

    __mapper_args__: ClassVar[Mapping[str, Any]] = {  # type: ignore
        "polymorphic_identity": MetricValueType.SCALAR,
    }

    # This is a scalar value
    value: Mapped[float] = mapped_column(nullable=True)

    def __repr__(self) -> str:
        return (
            f"<ScalarMetricValue "
            f"id={self.id} execution={self.execution} dimensions={self.dimensions} value={self.value}>"
        )

    @classmethod
    def build(
        cls,
        *,
        execution_id: int,
        value: float,
        dimensions: dict[str, str],
        attributes: dict[str, Any] | None,
    ) -> "MetricValue":
        """
        Build a MetricValue from a collection of dimensions and a value

        This is a helper method that validates the dimensions supplied and provides an interface
        similar to [climate_ref_core.metric_values.ScalarMetricValue][].

        Parameters
        ----------
        execution_id
            Execution that created the diagnostic value
        value
            The value of the diagnostic
        dimensions
            Dimensions that describe the diagnostic execution result
        attributes
            Optional additional attributes to describe the value,
            but are not in the controlled vocabulary.

        Raises
        ------
        KeyError
            If an unknown dimension was supplied.

            Dimensions must exist in the controlled vocabulary.

        Returns
        -------
            Newly created MetricValue
        """
        for k in dimensions:
            if k not in cls._cv_dimensions:
                raise KeyError(f"Unknown dimension column '{k}'")

        return ScalarMetricValue(
            execution_id=execution_id,
            value=value,
            attributes=attributes,
            **dimensions,
        )


class SeriesMetricValue(MetricValue):
    """
    A 1d series with associated dimensions

    This is a subclass of MetricValue that is used to represent a series.
    This can be used to represent time series, vertical profiles or other 1d data.
    """

    __mapper_args__: ClassVar[Mapping[str, Any]] = {  # type: ignore
        "polymorphic_identity": MetricValueType.SERIES,
    }

    # This is a scalar value
    values: Mapped[list[float | int]] = mapped_column(nullable=True)
    index: Mapped[list[float | int | str]] = mapped_column(nullable=True)
    index_name: Mapped[str] = mapped_column(nullable=True)

    def __repr__(self) -> str:
        return (
            f"<SeriesMetricValue id={self.id} execution={self.execution} "
            f"dimensions={self.dimensions} index_name={self.index_name}>"
        )

    @classmethod
    def build(  # noqa: PLR0913
        cls,
        *,
        execution_id: int,
        values: list[float | int],
        index: list[float | int | str],
        index_name: str,
        dimensions: dict[str, str],
        attributes: dict[str, Any] | None,
    ) -> "MetricValue":
        """
        Build a database object from a series

        Parameters
        ----------
        execution_id
            Execution that created the diagnostic value
        values
            1-d array of values
        index
            1-d array of index values
        index_name
            Name of the index. Used for presentation purposes
        dimensions
            Dimensions that describe the diagnostic execution result
        attributes
            Optional additional attributes to describe the value,
            but are not in the controlled vocabulary.

        Raises
        ------
        KeyError
            If an unknown dimension was supplied.

            Dimensions must exist in the controlled vocabulary.
        ValueError
            If the length of values and index do not match.

        Returns
        -------
            Newly created MetricValue
        """
        for k in dimensions:
            if k not in cls._cv_dimensions:
                raise KeyError(f"Unknown dimension column '{k}'")

        if len(values) != len(index):
            raise ValueError(f"Index length ({len(index)}) must match values length ({len(values)})")

        return SeriesMetricValue(
            execution_id=execution_id,
            values=values,
            index=index,
            index_name=index_name,
            attributes=attributes,
            **dimensions,
        )


@event.listens_for(SeriesMetricValue, "before_insert")
@event.listens_for(SeriesMetricValue, "before_update")
def validate_series_lengths(mapper: Any, connection: Any, target: SeriesMetricValue) -> None:
    """
    Validate that values and index have matching lengths

    This is done on insert and update to ensure that the database is consistent.
    """
    if target.values is not None and target.index is not None and len(target.values) != len(target.index):
        raise ValueError(
            f"Index length ({len(target.index)}) must match values length ({len(target.values)})"
        )
