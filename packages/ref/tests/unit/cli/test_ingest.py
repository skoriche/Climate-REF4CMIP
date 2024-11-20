from pathlib import Path

import pytest
from ref_core.exceptions import OutOfTreeDatasetException
from typer.testing import CliRunner

from ref.cli import app
from ref.cli.ingest import validate_prefix
from ref.models import Dataset
from ref.models.dataset import CMIP6Dataset, CMIP6File

runner = CliRunner()


def test_validate_prefix_with_valid_relative_path(config):
    config.paths.data = Path("/data")

    raw_path = "/data/subfolder/file.csv"
    expected_path = Path("subfolder/file.csv")

    result = validate_prefix(config, raw_path)
    assert result == expected_path


def test_validate_prefix_with_allow_out_of_tree_datasets(config):
    config.paths.data = Path("/data")
    config.paths.allow_out_of_tree_datasets = True

    raw_path = "/other_dir/file.csv"
    expected_path = Path("/other_dir/file.csv")

    result = validate_prefix(config, raw_path)
    assert result == expected_path


def test_validate_prefix_with_invalid_relative_path(config):
    config.paths.data = Path("/data")
    config.paths.allow_out_of_tree_datasets = False

    raw_path = "/other_dir/file.csv"
    with pytest.raises(OutOfTreeDatasetException):
        validate_prefix(config, raw_path)


def test_ingest_help():
    result = runner.invoke(app, ["ingest", "--help"])
    assert result.exit_code == 0

    assert "Ingest a dataset" in result.output


class TestIngest:
    data_dir = Path("CMIP6") / "ScenarioMIP" / "CSIRO" / "ACCESS-ESM1-5" / "ssp126" / "r1i1p1f1"

    def test_ingest(self, esgf_data_dir, db):
        result = runner.invoke(app, ["ingest", str(esgf_data_dir / self.data_dir), "--source-type", "cmip6"])
        assert result.exit_code == 0, result.output

        assert db.session.query(Dataset).count() == 5
        assert db.session.query(CMIP6Dataset).count() == 5
        assert db.session.query(CMIP6File).count() == 9

    def test_ingest_and_solve(self, esgf_data_dir, db):
        result = runner.invoke(app, ["ingest", str(esgf_data_dir), "--source-type", "cmip6", "--solve"])
        assert result.exit_code == 0, result.output
        assert "Solving for metrics that require recalculation." in result.output

    def test_ingest_multiple_times(self, esgf_data_dir, db):
        result = runner.invoke(
            app, ["ingest", str(esgf_data_dir / self.data_dir / "Amon" / "tas"), "--source-type", "cmip6"]
        )
        assert result.exit_code == 0, result.output

        assert db.session.query(Dataset).count() == 1
        assert db.session.query(CMIP6File).count() == 2

        result = runner.invoke(
            app, ["ingest", str(esgf_data_dir / self.data_dir / "Amon" / "tas"), "--source-type", "cmip6"]
        )
        assert result.exit_code == 0, result.output

        assert db.session.query(Dataset).count() == 1

        result = runner.invoke(
            app, ["ingest", str(esgf_data_dir / self.data_dir / "Amon" / "rsut"), "--source-type", "cmip6"]
        )
        assert result.exit_code == 0, result.output

        assert db.session.query(Dataset).count() == 2

    def test_ingest_missing(self, esgf_data_dir, db):
        result = runner.invoke(app, ["ingest", str(esgf_data_dir / "missing"), "--source-type", "cmip6"])
        assert isinstance(result.exception, FileNotFoundError)
        assert result.exception.filename == esgf_data_dir / "missing"

        assert f'File or directory {esgf_data_dir / "missing"} does not exist' in result.output

    def test_ingest_dryrun(self, esgf_data_dir, db):
        result = runner.invoke(app, ["ingest", str(esgf_data_dir), "--source-type", "cmip6", "--dry-run"])
        assert result.exit_code == 0, result.output

        # Check that no data was loaded
        assert db.session.query(Dataset).count() == 0
