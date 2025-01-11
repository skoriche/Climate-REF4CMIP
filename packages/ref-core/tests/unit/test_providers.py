import pytest

from cmip_ref_core.exceptions import InvalidMetricException
from cmip_ref_core.metrics import Metric
from cmip_ref_core.providers import MetricsProvider


class TestMetricsProvider:
    def test_provider(self):
        provider = MetricsProvider("provider_name", "v0.23")

        assert provider.name == "provider_name"
        assert provider.version == "v0.23"
        assert len(provider) == 0

    def test_provider_register(self, mock_metric):
        provider = MetricsProvider("provider_name", "v0.23")
        provider.register(mock_metric)

        assert len(provider) == 1
        assert "mock" in provider._metrics
        assert isinstance(provider.get("mock"), Metric)

        assert len(provider.metrics()) == 1
        assert provider.metrics()[0].name == "mock"

    def test_provider_register_invalid(self):
        class InvalidMetric:
            pass

        provider = MetricsProvider("provider_name", "v0.23")
        with pytest.raises(InvalidMetricException):
            provider.register(InvalidMetric())

    def test_provider_fixture(self, provider):
        assert provider.name == "mock_provider"
        assert provider.version == "v0.1.0"
        assert len(provider) == 2
        assert "mock" in provider._metrics
        assert "failed" in provider._metrics

        result = provider.get("mock")
        assert isinstance(result, Metric)
