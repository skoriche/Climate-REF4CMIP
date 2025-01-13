from cmip_ref import __version__
from cmip_ref_core import __version__ as __core_version__


def test_without_subcommand(invoke_cli):
    result = invoke_cli([])
    assert "Usage:" in result.stdout
    assert "cmip_ref [OPTIONS] COMMAND [ARGS]" in result.stdout
    assert "cmip_ref: A CLI for the CMIP Rapid Evaluation Framework" in result.stdout


def test_version(invoke_cli):
    result = invoke_cli(["--version"])
    assert f"cmip_ref: {__version__}\ncmip_ref-core: {__core_version__}" in result.stdout


def test_verbose(invoke_cli):
    exp_log = "| DEBUG    | cmip_ref.config:default:178 - Loading default configuration from"
    result = invoke_cli(
        ["--verbose", "config", "list"],
    )
    assert exp_log in result.stderr

    result = invoke_cli(
        ["config", "list"],
    )
    # Only info and higher messages logged
    assert exp_log not in result.stderr


def test_config_directory_custom(config, invoke_cli):
    config.paths.tmp = "test-value"
    config.save()

    result = invoke_cli(
        [
            "--configuration-directory",
            str(config._config_file.parent),
            "config",
            "list",
        ],
    )
    assert 'tmp = "test-value"\n' in result.output


def test_config_directory_append(config, invoke_cli):
    # configuration directory must be passed before command
    invoke_cli(
        [
            "config",
            "list",
            "--configuration-directory",
            str(config._config_file.parent),
        ],
        expected_exit_code=2,
    )
