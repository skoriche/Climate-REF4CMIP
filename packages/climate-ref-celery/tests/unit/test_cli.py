import importlib.metadata

import pytest
from climate_ref_celery.cli import app
from typer.testing import CliRunner

from climate_ref_core.providers import DiagnosticProvider

runner = CliRunner()


def test_cli_help():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0


@pytest.fixture
def mock_create_celery_app(mocker):
    return mocker.patch("climate_ref_celery.cli.create_celery_app")


@pytest.fixture
def mock_register_celery_tasks(mocker):
    return mocker.patch("climate_ref_celery.cli.register_celery_tasks")


@pytest.mark.parametrize("provider", ["test_package", "test_package:provider"])
def test_start_worker_success(mocker, mock_create_celery_app, mock_register_celery_tasks, provider):
    mock_celery_app = mock_create_celery_app.return_value
    mock_provider = mocker.MagicMock(spec=DiagnosticProvider)
    mock_provider.slug = "example"

    mock_entry_point = mocker.Mock(spec=importlib.metadata.EntryPoint)
    mock_entry_point.name = "test_package"
    mock_entry_point.value = "test_package:provider"
    mock_entry_point.load.return_value = mock_provider
    mock_entry_points = mocker.patch("importlib.metadata.entry_points", return_value=[mock_entry_point])

    result = runner.invoke(app, ["start-worker", "--provider", provider])

    assert result.exit_code == 0
    mock_entry_points.assert_called_once_with(group="climate-ref.providers")
    mock_register_celery_tasks.assert_called_once_with(mock_create_celery_app.return_value, mock_provider)
    mock_celery_app.worker_main.assert_called_once_with(
        argv=["worker", "-E", "--loglevel=info", "--queues=example"]
    )


def test_start_worker_multiple(mocker, mock_create_celery_app, mock_register_celery_tasks):
    mock_celery_app = mock_create_celery_app.return_value

    mock_provider_a = mocker.MagicMock(spec=DiagnosticProvider)
    mock_provider_a.slug = "example"
    mock_entry_point_a = mocker.Mock(spec=importlib.metadata.EntryPoint)
    mock_entry_point_a.name = "test_package"
    mock_entry_point_a.value = "test_package:provider"
    mock_entry_point_a.load.return_value = mock_provider_a

    mock_provider_b = mocker.MagicMock(spec=DiagnosticProvider)
    mock_provider_b.slug = "other"
    mock_entry_point_b = mocker.Mock(spec=importlib.metadata.EntryPoint)
    mock_entry_point_b.name = "other_package"
    mock_entry_point_b.value = "other_package:provider"
    mock_entry_point_b.load.return_value = mock_provider_b

    mock_entry_points = mocker.patch(
        "importlib.metadata.entry_points", return_value=[mock_entry_point_a, mock_entry_point_b]
    )

    result = runner.invoke(app, ["start-worker", "--provider", "test_package", "--provider", "other_package"])

    assert result.exit_code == 0
    mock_entry_points.assert_called_with(group="climate-ref.providers")
    mock_register_celery_tasks.assert_any_call(mock_create_celery_app.return_value, mock_provider_a)
    mock_register_celery_tasks.assert_any_call(mock_create_celery_app.return_value, mock_provider_b)
    mock_celery_app.worker_main.assert_called_once_with(
        argv=["worker", "-E", "--loglevel=info", "--queues=example,other"]
    )


def test_start_core_worker_success(mock_create_celery_app, mock_register_celery_tasks):
    mock_celery_app = mock_create_celery_app.return_value

    result = runner.invoke(app, ["start-worker"])

    assert result.exit_code == 0
    mock_celery_app.worker_main.assert_called_once_with(
        argv=["worker", "-E", "--loglevel=info", "--queues=celery"]
    )


def test_start_worker_success_extra_args(mocker, mock_create_celery_app, mock_register_celery_tasks):
    mock_worker_main = mock_create_celery_app.return_value
    mock_provider = mocker.MagicMock(spec=DiagnosticProvider)
    mock_provider.slug = "example"

    mock_entry_point = mocker.Mock(spec=importlib.metadata.EntryPoint)
    mock_entry_point.name = "test_package"
    mock_entry_point.load.return_value = mock_provider
    mocker.patch("importlib.metadata.entry_points", return_value=[mock_entry_point])

    result = runner.invoke(
        app,
        [
            "start-worker",
            "--loglevel",
            "error",
            "--provider",
            "test_package",
            "--",
            "--extra-args",
            "--concurrency=2",
        ],
    )

    assert result.exit_code == 0, result.output
    mock_worker_main.worker_main.assert_called_once_with(
        argv=["worker", "-E", "--loglevel=error", "--queues=example", "--extra-args", "--concurrency=2"]
    )


def test_start_worker_package_not_registered(mocker, mock_create_celery_app):
    mocker.patch("importlib.metadata.entry_points", return_value=[])

    result = runner.invoke(app, ["start-worker", "--provider", "unregistered_package"])

    assert result.exit_code == 1
    assert "No entry point named 'unregistered_package' was found" in result.output
    assert "Found entry points: []" in result.output
    mock_create_celery_app.assert_called_once_with("climate_ref_celery")


def test_start_worker_package_not_found(mocker, mock_create_celery_app):
    mock_entry_point = mocker.Mock(spec=importlib.metadata.EntryPoint)
    mock_entry_point.name = "missing_package"
    mock_entry_point.value = "missing_package:provider"
    mock_entry_point.load.side_effect = ModuleNotFoundError
    mock_entry_points = mocker.patch("importlib.metadata.entry_points", return_value=[mock_entry_point])

    result = runner.invoke(app, ["start-worker", "--provider", "missing_package"])

    assert result.exit_code == 1
    assert "Package 'missing_package' not found" in result.output
    mock_create_celery_app.assert_called_once_with("climate_ref_celery")
    mock_entry_points.assert_called_once_with(group="climate-ref.providers")


def test_start_worker_missing_provider(mocker, mock_create_celery_app):
    mock_entry_point = mocker.Mock(spec=importlib.metadata.EntryPoint)
    mock_entry_point.name = "test_package"
    mock_entry_point.value = "test_package:provider"
    mock_entry_point.load.side_effect = AttributeError
    mocker.patch("importlib.metadata.entry_points", return_value=[mock_entry_point])

    result = runner.invoke(app, ["start-worker", "--provider", "test_package"])

    assert result.exit_code == 1, result.output
    assert "'test_package' does not define a 'provider' attribute" in result.output


def test_start_worker_incorrect_provider(mocker, mock_create_celery_app):
    # Not a DiagnosticProvider
    mock_provider = mocker.Mock()

    mock_entry_point = mocker.Mock(spec=importlib.metadata.EntryPoint)
    mock_entry_point.name = "test_package"
    mock_entry_point.load.return_value = mock_provider
    mocker.patch("importlib.metadata.entry_points", return_value=[mock_entry_point])

    result = runner.invoke(app, ["start-worker", "--provider", "test_package"])

    assert result.exit_code == 1, result.output
    assert "Expected DiagnosticProvider, got <class 'unittest.mock.Mock'>" in result.output


def test_start_worker_deprecated_package(mocker, mock_create_celery_app):
    mock_provider = mocker.MagicMock(spec=DiagnosticProvider)
    mock_provider.slug = "example"

    mock_entry_point = mocker.Mock(spec=importlib.metadata.EntryPoint)
    mock_entry_point.name = "test_package"
    mock_entry_point.value = "test_package:provider"
    mock_entry_point.load.return_value = mock_provider
    mocker.patch("importlib.metadata.entry_points", return_value=[mock_entry_point])

    result = runner.invoke(app, ["start-worker", "--package", "test_package"])

    assert result.exit_code == 0, result.output
    assert "The '--package' argument is deprecated. Use '--provider' instead." in result.output


def test_list_config():
    result = runner.invoke(app, ["list-config"])

    assert result.exit_code == 0, result.output
    assert "broker_url: 'redis://localhost:6379/1'" in result.stdout
