from cmip_ref.provider_registry import ProviderRegistry


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
