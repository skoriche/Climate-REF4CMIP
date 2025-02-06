import pytest

from cmip_ref.provider_registry import ProviderRegistry, import_provider
from cmip_ref_core.exceptions import InvalidProviderException
from cmip_ref_core.providers import MetricsProvider


@pytest.mark.parametrize("fqn", ["cmip_ref_metrics_esmvaltool.provider", "cmip_ref_metrics_esmvaltool"])
def test_import_provider(fqn):
    provider = import_provider(fqn)

    assert provider.name == "ESMValTool"
    assert provider.slug == "esmvaltool"
    assert isinstance(provider, MetricsProvider)


def test_import_provider_missing():
    fqn = "cmip_ref"
    match = f"Invalid provider: '{fqn}'\n Provider 'provider' not found in cmip_ref"
    with pytest.raises(InvalidProviderException, match=match):
        import_provider(fqn)

    fqn = "cmip_ref.datasets.WrongProvider"
    match = f"Invalid provider: '{fqn}'\n Provider 'WrongProvider' not found in cmip_ref.datasets"
    with pytest.raises(InvalidProviderException, match=match):
        import_provider(fqn)

    fqn = "missing.local.WrongProvider"
    match = f"Invalid provider: '{fqn}'\n Module 'missing.local' not found"
    with pytest.raises(InvalidProviderException, match=match):
        import_provider(fqn)

    fqn = "cmip_ref.constants.config_filename"
    match = f"Invalid provider: '{fqn}'\n Expected MetricsProvider, got <class 'str'>"
    with pytest.raises(InvalidProviderException, match=match):
        import_provider(fqn)


class TestProviderRegistry:
    def test_create(self, config, mocker):
        db = mocker.MagicMock()

        mock_import = mocker.patch("cmip_ref.provider_registry.import_provider")
        mock_register = mocker.patch("cmip_ref.provider_registry._register_provider")

        registry = ProviderRegistry.build_from_config(config, db)
        assert len(registry.providers) == 1
        assert registry.providers[0] == mock_import.return_value

        assert mock_import.call_count == 1
        mock_register.assert_called_once_with(db, mock_import.return_value)
