from typer.testing import CliRunner

from ref import __core_version__, __version__
from ref.cli import app

runner = CliRunner(
    mix_stderr=False,
)


def test_without_subcommand():
    result = runner.invoke(app, [])
    assert result.exit_code == 0
    assert "Usage: ref" in result.stdout
    assert "ref: A CLI for the CMIP Rapid Evaluation Framework" in result.stdout


def test_version():
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert f"ref: {__version__}\nref-core: {__core_version__}" in result.output


def test_verbose():
    exp_log = "| DEBUG    | ref.config:default:176 - Loading default configuration from"
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
