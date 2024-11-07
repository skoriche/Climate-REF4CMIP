import platformdirs
from ref.cli import app
from typer.testing import CliRunner

runner = CliRunner()


def test_without_subcommand():
    result = runner.invoke(app, ["config"])
    assert result.exit_code == 2
    assert "Missing command." in result.output


def test_config_help():
    result = runner.invoke(app, ["config", "--help"])
    assert result.exit_code == 0

    expected = """
 Usage: ref config [OPTIONS] COMMAND [ARGS]...

 View and update the REF configuration

╭─ Options ────────────────────────────────────────────────────────────────────╮
│ --help          Show this message and exit.                                  │
╰──────────────────────────────────────────────────────────────────────────────╯
╭─ Commands ───────────────────────────────────────────────────────────────────╮
│ list     Print the current ref configuration                                 │
│ update   Print the current ref configuration                                 │
╰──────────────────────────────────────────────────────────────────────────────╯
"""
    assert expected in result.output


class TestConfigList:
    def test_config_list(self):
        result = runner.invoke(app, ["config", "list"])
        assert result.exit_code == 0

        config_dir = platformdirs.user_config_dir("cmip-ref")
        assert f'data = "{config_dir}/data"\n' in result.output
        assert 'filename = "sqlite://ref.db"\n' in result.output

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
