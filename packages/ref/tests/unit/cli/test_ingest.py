from typer.testing import CliRunner

from ref.cli import app
from ref.models import Dataset
from ref.models.dataset import CMIP6Dataset, CMIP6File

runner = CliRunner()


def test_ingest_help():
    result = runner.invoke(app, ["ingest", "--help"])
    assert result.exit_code == 0

    assert "Ingest a dataset" in result.output


class TestIngest:
    def test_ingest(self, esgf_data_dir, db):
        result = runner.invoke(app, ["ingest", str(esgf_data_dir), "--source-type", "cmip6"])
        assert result.exit_code == 0, result.output

        assert db.session.query(Dataset).count() == 7
        assert db.session.query(CMIP6Dataset).count() == 7
        assert db.session.query(CMIP6File).count() == 11

    def test_ingest_twice(self, esgf_data_dir, db):
        result = runner.invoke(
            app, ["ingest", str(esgf_data_dir / "CMIP6" / "ScenarioMIP"), "--source-type", "cmip6"]
        )
        assert result.exit_code == 0, result.output

        assert db.session.query(Dataset).count() == 6
        assert db.session.query(CMIP6Dataset).count() == 6
        assert db.session.query(CMIP6File).count() == 10

        result = runner.invoke(
            app, ["ingest", str(esgf_data_dir / "CMIP6" / "ScenarioMIP"), "--source-type", "cmip6"]
        )
        assert result.exit_code == 0, result.output

        assert db.session.query(Dataset).count() == 6
        assert db.session.query(CMIP6Dataset).count() == 6
        assert db.session.query(CMIP6File).count() == 10

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
