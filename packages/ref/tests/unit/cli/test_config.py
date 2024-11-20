import os

from typer.testing import CliRunner

from ref.cli import app

runner = CliRunner()


def test_without_subcommand():
    result = runner.invoke(app, ["config"])
    assert result.exit_code == 2
    assert "Missing command." in result.output


def test_config_help():
    result = runner.invoke(app, ["config", "--help"])
    assert result.exit_code == 0

    assert "View and update the REF configuration" in result.output


class TestConfigList:
    def test_config_list(self, config):
        result = runner.invoke(app, ["config", "list"])
        assert result.exit_code == 0

        config_dir = os.environ.get("REF_CONFIGURATION")
        assert f'data = "{config_dir}/data"\n' in result.output
        assert 'database_url = "sqlite://' in result.output

    def test_config_list_custom(self, config):
        result = runner.invoke(
            app, ["config", "list", "--configuration-directory", str(config._config_file.parent)]
        )
        assert result.exit_code == 0

    def test_config_list_custom_missing(self, config):
        result = runner.invoke(app, ["config", "list", "--configuration-directory", "missing"])
        assert result.exit_code == 1


class TestConfigUpdate:
    def test_config_update(self):
        result = runner.invoke(app, ["config", "update"])
        assert result.exit_code == 0

        # TODO: actually implement this functionality
        assert "config" in result.output
