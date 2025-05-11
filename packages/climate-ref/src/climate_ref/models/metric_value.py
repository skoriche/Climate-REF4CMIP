import enum
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, ClassVar

from loguru import logger
from sqlalchemy import Column, ForeignKey, Text, event
from sqlalchemy.orm import Mapped, mapped_column, relationship

from climate_ref.models.base import Base, CreatedUpdatedMixin
from climate_ref_core.pycmec.controlled_vocabulary import CV, Dimension

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


class MetricValue(CreatedUpdatedMixin, Base):
    """
    Represents a single metric value

    This value has a number of dimensions which are used to query the diagnostic value.
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

    id: Mapped[int] = mapped_column(primary_key=True)
    execution_id: Mapped[int] = mapped_column(ForeignKey("execution.id"))

    attributes: Mapped[dict[str, Any]] = mapped_column()

    execution: Mapped["Execution"] = relationship(back_populates="values")

    type: Mapped[MetricValueType] = mapped_column()
    """
    Type of metric value

    This value is used to determine how the metric value should be interpreted.
    """

    _cv_dimensions: ClassVar[list[str]] = []

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
        for key in self._cv_dimensions:
            value = getattr(self, key)
            if value is not None:
                dims[key] = value
        return dims

    def __repr__(self) -> str:
        return f"<MetricValue id={self.id} execution={self.execution} dimensions={self.dimensions}>"

    @staticmethod
    def build_dimension_column(dimension: Dimension) -> Column[str]:
        """
        Create a column representing a CV dimension

        These columns are not automatically generated with alembic revisions.
        Any changes to this functionality likely require a manual database migration
        of the existing columns.

        Parameters
        ----------
        dimension
            Dimension definition to create the column for.

            Currently only the "name" field is being used.

        Returns
        -------
            An instance of a sqlalchemy Column

            This doesn't create the column in the database,
            but enables the ORM to access it.

        """
        return Column(
            dimension.name,
            Text,
            index=True,
            nullable=True,
            info={"skip_autogenerate": True},
        )

    @classmethod
    def register_cv_dimensions(cls, cv: CV) -> None:
        """
        Register the dimensions supplied in the controlled vocabulary

        This has to be done at run-time to support custom CVs.
        Any extra columns already in the database, but not in the CV are ignored.

        Parameters
        ----------
        cv
            Controlled vocabulary being used by the application.
            This controlled vocabulary contains the definitions of the dimensions that can be used.
        """
        for dimension in cv.dimensions:
            target_attribute = dimension.name
            if target_attribute in cls._cv_dimensions:
                continue

            cls._cv_dimensions.append(target_attribute)
            logger.debug(f"Registered MetricValue dimension: {target_attribute}")

            if hasattr(cls, target_attribute):
                # This should only occur in test suite as we don't support removing dimensions at runtime
                logger.warning("Column attribute already exists on MetricValue. Ignoring")
            else:
                setattr(cls, target_attribute, cls.build_dimension_column(dimension))

            # TODO: Check if the underlying table already contains columns

    @classmethod
    def _reset_cv_dimensions(cls) -> None:
        """
        Remove any previously registered dimensions

        Used by the test suite and should not be called at runtime.

        This doesn't remove any previous column definitions due to a limitation that columns in
        declarative classes cannot be removed.
        This means that `hasattr(MetricValue, "old_attribute")`
        will still return True after resetting, but the values will not be included in any executions.
        """
        logger.warning(f"Removing MetricValue dimensions: {cls._cv_dimensions}")

        keys = list(cls._cv_dimensions)
        for key in keys:
            cls._cv_dimensions.remove(key)

        assert not len(cls._cv_dimensions)


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
    A scalar value with an associated dimensions

    This is a subclass of MetricValue that is used to represent a scalar value.
    """

    __mapper_args__: ClassVar[Mapping[str, Any]] = {  # type: ignore
        "polymorphic_identity": MetricValueType.SERIES,
    }

    # This is a scalar value
    values: Mapped[list[float | int]] = mapped_column(nullable=True)
    index: Mapped[list[float | int | str]] = mapped_column(nullable=True)
    index_name: Mapped[str] = mapped_column(nullable=True)

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
