from ref_core.datasets import SourceDatasetType
from typer.testing import CliRunner

from ref.cli import app
from ref.cli.ingest import ingest

runner = CliRunner()


def test_ingest_help():
    result = runner.invoke(app, ["ingest", "--help"])
    assert result.exit_code == 0

    assert "View and update the REF configuration" in result.output


class TestIngest:
    def test_ingest(self, esgf_data_dir):
        result = runner.invoke(app, ["ingest", str(esgf_data_dir), "--source-type", "cmip6"])
        assert result.exit_code == 0, result.output

    def test_ingest_directly(self, esgf_data_dir):
        ingest(
            file_or_directory=esgf_data_dir,
            source_type=SourceDatasetType.CMIP6,
            configuration_directory=None,
            dry_run=False,
        )
