from typing import Any, TypeVar

from sqlalchemy import JSON, MetaData
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """
    Base class for all models
    """

    type_annotation_map = {  # noqa: RUF012
        dict[str, Any]: JSON,
        list[float | int]: JSON,
        list[float | int | str]: JSON,
    }
    metadata = MetaData(
        # Enforce a common naming convention for constraints
        # https://alembic.sqlalchemy.org/en/latest/naming.html
        naming_convention={
            "ix": "ix_%(column_0_label)s",
            "uq": "uq_%(table_name)s_%(column_0_name)s",
            "ck": "ck_%(table_name)s_`%(constraint_name)s`",
            "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
            "pk": "pk_%(table_name)s",
        }
    )


Table = TypeVar("Table", bound=Base)
