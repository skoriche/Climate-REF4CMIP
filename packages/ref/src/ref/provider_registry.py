from attrs import field, frozen
from ref_core.providers import MetricsProvider

from ref.database import Database


@frozen
class ProviderRegistry:
    """
    Registry for the currently active providers

    In some cases we can't directly import a provider and it's metrics,
    in this case we need to proxy the metrics.
    """

    providers: list[MetricsProvider] = field(factory=list)

    @staticmethod
    def build_from_db(db: Database) -> "ProviderRegistry":
        """
        Create a ProviderRegistry instance using information from the database

        Parameters
        ----------
        db
            Database instance

        Returns
        -------
        :
            A new ProviderRegistry instance
        """
        # TODO: We don't yet have any tables to represent metrics providers
        return ProviderRegistry(providers=[])
