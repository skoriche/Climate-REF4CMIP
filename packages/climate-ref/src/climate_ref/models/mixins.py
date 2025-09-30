"""Model mixins for shared functionality"""

import datetime
from typing import TYPE_CHECKING, ClassVar

from loguru import logger
from sqlalchemy import Column, Text, func
from sqlalchemy.orm import Mapped, mapped_column

if TYPE_CHECKING:
    from climate_ref_core.pycmec.controlled_vocabulary import CV, Dimension


class CreatedUpdatedMixin:
    """
    Mixin for models that have a created_at and updated_at fields
    """

    created_at: Mapped[datetime.datetime] = mapped_column(server_default=func.now())
    """
    When the dataset was added to the database
    """

    updated_at: Mapped[datetime.datetime] = mapped_column(
        server_default=func.now(), onupdate=func.now(), index=True
    )
    """
    When the dataset was updated.
    """


class DimensionMixin:
    """
    Mixin that adds controlled vocabulary dimension support to a model

    This mixin provides methods and properties for managing CV dimensions
    on database models. Dimensions are stored as individual indexed columns
    that are registered at runtime based on the controlled vocabulary.

    Classes using this mixin must:
    - Define _cv_dimensions as a ClassVar[list[str]] = []
    - Have a __tablename__ attribute (SQLAlchemy requirement)
    """

    _cv_dimensions: ClassVar[list[str]]

    @property
    def dimensions(self) -> dict[str, str]:
        """
        Get the non-null dimensions and their values

        Any changes to the resulting dictionary are not reflected in the object

        Returns
        -------
            Collection of dimension names and their values
        """
        dims = {}
        for key in self._cv_dimensions:
            value = getattr(self, key)
            if value is not None:
                dims[key] = value
        return dims

    @staticmethod
    def build_dimension_column(dimension: "Dimension") -> Column[str]:
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
    def register_cv_dimensions(cls, cv: "CV") -> None:
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
        model_name = cls.__name__

        for dimension in cv.dimensions:
            target_attribute = dimension.name
            if target_attribute in cls._cv_dimensions:
                continue

            cls._cv_dimensions.append(target_attribute)
            logger.debug(f"Registered {model_name} dimension: {target_attribute}")

            if hasattr(cls, target_attribute):
                # This should only occur in test suite as we don't support removing dimensions at runtime
                logger.warning(f"Column attribute already exists on {model_name}. Ignoring")
            else:
                setattr(cls, target_attribute, cls.build_dimension_column(dimension))

    @classmethod
    def _reset_cv_dimensions(cls) -> None:
        """
        Remove any previously registered dimensions

        Used by the test suite and should not be called at runtime.

        This doesn't remove any previous column definitions due to a limitation that columns in
        declarative classes cannot be removed.
        This means that `hasattr(cls, "old_attribute")`
        will still return True after resetting, but the values will not be included in any executions.
        """
        model_name = cls.__name__
        logger.warning(f"Removing {model_name} dimensions: {cls._cv_dimensions}")

        keys = list(cls._cv_dimensions)
        for key in keys:
            cls._cv_dimensions.remove(key)

        assert not len(cls._cv_dimensions)
