class TestProvidersList:
    def test_list(self, config, invoke_cli):
        result = invoke_cli(["providers", "list"])
        assert result.exit_code == 0
        assert "provider" in result.stdout
        assert "example" in result.stdout


class TestProvidersCreateEnv:
    def test_create_env(self, config, invoke_cli):
        result = invoke_cli(["providers", "create-env"])
        assert result.exit_code == 0

    def test_create_env_invalid_provider(self, config, invoke_cli):
        invoke_cli(
            [
                "providers",
                "create-env",
                "--provider",
                "nonexistent",
            ],
            expected_exit_code=1,
        )
