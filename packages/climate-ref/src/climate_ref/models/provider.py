from typing import TYPE_CHECKING

from sqlalchemy.orm import Mapped, mapped_column, relationship

from climate_ref.models.base import Base, CreatedUpdatedMixin

if TYPE_CHECKING:
    from climate_ref.models.diagnostic import Diagnostic


class Provider(CreatedUpdatedMixin, Base):
    """
    Represents a provider that can provide diagnostic calculations
    """

    __tablename__ = "provider"

    id: Mapped[int] = mapped_column(primary_key=True)
    slug: Mapped[str] = mapped_column(unique=True)
    """
    Globally unique identifier for the provider.
    """

    name: Mapped[str] = mapped_column()
    """
    Long name of the provider
    """

    version: Mapped[str] = mapped_column(nullable=False)
    """
    Version of the provider.

    This should map to the package version.
    """

    diagnostics: Mapped[list["Diagnostic"]] = relationship(back_populates="provider")

    def __repr__(self) -> str:
        return f"<Provider slug={self.slug} version={self.version}>"
