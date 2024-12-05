from typer.testing import CliRunner

from ref import __core_version__, __version__
from ref.cli import app

runner = CliRunner(
    mix_stderr=False,
)


def test_without_subcommand():
    result = runner.invoke(app, [])
    assert result.exit_code == 0
    assert "Usage:" in result.stdout
    assert "ref [OPTIONS] COMMAND [ARGS]" in result.stdout
    assert "ref: A CLI for the CMIP Rapid Evaluation Framework" in result.stdout


def test_version():
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert f"ref: {__version__}\nref-core: {__core_version__}" in result.output


def test_verbose():
    exp_log = "| DEBUG    | ref.config:default:178 - Loading default configuration from"
    result = runner.invoke(
        app,
        ["--verbose", "config", "list"],
    )
    assert exp_log in result.stderr

    result = runner.invoke(
        app,
        ["config", "list"],
    )
    # Only info and higher messages logged
    assert exp_log not in result.stderr


def test_config_directory_custom(config):
    config.paths.tmp = "test-value"
    config.save()

    result = runner.invoke(
        app,
        [
            "--configuration-directory",
            str(config._config_file.parent),
            "config",
            "list",
        ],
    )
    assert result.exit_code == 0
    assert 'tmp = "test-value"\n' in result.output


def test_config_directory_append(config):
    # configuration directory must be passed before command
    result = runner.invoke(
        app,
        [
            "config",
            "list",
            "--configuration-directory",
            str(config._config_file.parent),
        ],
    )
    assert result.exit_code == 2
