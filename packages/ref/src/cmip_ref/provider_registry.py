"""
Registry of the currently active providers in the REF

This module provides a registry for the currently active providers.
Often, we can't directly import a provider and it's metrics
as each provider maintains its own virtual environment to avoid dependency conflicts.

For remote providers, a proxy is used to access the metadata associated with the metrics.
These metrics cannot be run locally, but can be executed using other executors.
"""

from attrs import field, frozen
from loguru import logger

from cmip_ref.config import Config
from cmip_ref.database import Database
from cmip_ref_core.metrics import Metric
from cmip_ref_core.providers import MetricsProvider, import_provider


def _register_provider(db: Database, provider: MetricsProvider) -> None:
    """
    Register a provider with the database

    This is temporary until we have a proper flow for registering providers

    Parameters
    ----------
    provider
        MetricsProvider instance
    """
    from cmip_ref.models import Metric, Provider

    provider_model, created = db.get_or_create(
        Provider,
        slug=provider.slug,
        version=provider.version,
        defaults={
            "name": provider.name,
        },
    )
    if created:
        logger.info(f"Created provider {provider.slug}")
        db.session.flush()

    for metric in provider.metrics():
        metric_model, created = db.get_or_create(
            Metric,
            slug=metric.slug,
            provider_id=provider_model.id,
            defaults={
                "name": metric.name,
            },
        )
        if created:
            logger.info(f"Created metric {metric_model.slug}")


@frozen
class ProviderRegistry:
    """
    Registry for the currently active providers

    In some cases we can't directly import a provider and it's metrics,
    in this case we need to proxy the metrics.
    """

    providers: list[MetricsProvider] = field(factory=list)

    def get(self, slug: str) -> MetricsProvider:
        """
        Retrieve a provider by name

        Parameters
        ----------
        slug
            Slug of the provider of interest

        Raises
        ------
        KeyError
            A provider with the matching slug has not been registered

        Returns
        -------
            The requested provider
        """
        for p in self.providers:
            if p.slug == slug:
                return p

        raise KeyError(f"No provider with slug matching: {slug}")

    def get_metric(self, provider_slug: str, metric_slug: str) -> "Metric":
        """
        Retrieve a metric by name

        This is a convenience method to retrieve a metric from a provider

        Parameters
        ----------
        provider_slug :
            Slug of the provider of interest
        metric_slug
            Slug of the metric of interest

        Raises
        ------
        KeyError
            If the provider/metric with the given slugs is not found.

        Returns
        -------
            The requested metric.
        """
        return self.get(provider_slug).get(metric_slug)

    @staticmethod
    def build_from_config(config: Config, db: Database) -> "ProviderRegistry":
        """
        Create a ProviderRegistry instance using information from the database

        Parameters
        ----------
        config
            Configuration object
        db
            Database instance

        Returns
        -------
        :
            A new ProviderRegistry instance
        """
        providers = []
        for provider_info in config.metric_providers:
            provider = import_provider(provider_info.provider)
            provider.configure(config)
            providers.append(provider)

        with db.session.begin_nested():
            for provider in providers:
                _register_provider(db, provider)

        return ProviderRegistry(providers=providers)
