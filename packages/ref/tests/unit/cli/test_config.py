import os


def test_without_subcommand(invoke_cli):
    result = invoke_cli(["config"], expected_exit_code=2)
    assert "Missing command." in result.stderr


def test_config_help(invoke_cli):
    result = invoke_cli(["config", "--help"], expected_exit_code=2)

    assert "View and update the REF configuration" in result.stderr


class TestConfigList:
    def test_config_list(self, config, invoke_cli):
        result = invoke_cli(["config", "list"])

        config_dir = os.environ.get("REF_CONFIGURATION")
        assert f'data = "{config_dir}/data"\n' in result.output
        assert 'database_url = "sqlite://' in result.output

    def test_config_list_custom_missing(self, config, invoke_cli):
        result = invoke_cli(
            [
                "--configuration-directory",
                "missing",
                "config",
                "list",
            ],
            expected_exit_code=1,
        )

        assert "Configuration file not found" in result.stdout


class TestConfigUpdate:
    def test_config_update(self, invoke_cli):
        result = invoke_cli(["config", "update"])

        # TODO: actually implement this functionality
        assert "config" in result.stdout
