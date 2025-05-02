import datetime
import pathlib

from rich.console import Console

from climate_ref.cli.executions import _results_directory_panel
from climate_ref.models import Execution, ExecutionGroup
from climate_ref.models.execution import execution_datasets


def test_execution_help(invoke_cli):
    result = invoke_cli(["executions", "--help"])

    assert "View diagnostic executions" in result.stdout


class TestExecutionList:
    def _setup_db(self, db):
        with db.session.begin():
            db.session.add(ExecutionGroup(key="key1", diagnostic_id=1))
            db.session.add(ExecutionGroup(key="key2", diagnostic_id=1))

    def test_list(self, sample_data_dir, db_seeded, invoke_cli):
        self._setup_db(db_seeded)

        result = invoke_cli(["executions", "list-groups"])

        assert "key1" in result.stdout
        assert "key2" in result.stdout
        assert "dirty" in result.stdout

    def test_list_limit(self, sample_data_dir, db_seeded, invoke_cli):
        self._setup_db(db_seeded)

        result = invoke_cli(["executions", "list-groups", "--limit", "1"])

        assert "key1" in result.stdout
        assert "key2" not in result.stdout

    def test_list_columns(self, sample_data_dir, db_seeded, invoke_cli):
        self._setup_db(db_seeded)

        result = invoke_cli(["executions", "list-groups", "--column", "key", "--column", "diagnostic"])

        assert "key1" in result.stdout
        assert "diagnostic" in result.stdout
        assert "dirty" not in result.stdout

    def test_list_columns_missing(self, sample_data_dir, db_seeded, invoke_cli):
        self._setup_db(db_seeded)

        invoke_cli(
            ["executions", "list-groups", "--column", "key", "--column", "missing"], expected_exit_code=1
        )


class TestExecutionInspect:
    def test_inspect(self, sample_data_dir, db_seeded, invoke_cli, file_regression, config):
        # Ensure the executions path is consistent
        config.paths.results = pathlib.Path("/results")
        config.save()

        # Create a diagnostic execution group with a result
        execution_group = ExecutionGroup(
            key="key1",
            diagnostic_id=1,
            # Ensure dates are consistent
            created_at=datetime.datetime(2021, 1, 1),
            updated_at=datetime.datetime(2021, 2, 1),
        )
        with db_seeded.session.begin():
            db_seeded.session.add(execution_group)
            db_seeded.session.flush()

            execution = Execution(
                execution_group_id=execution_group.id,
                successful=True,
                output_fragment="output",
                dataset_hash="hash",
            )
            db_seeded.session.add(execution)
            db_seeded.session.flush()
            db_seeded.session.execute(
                execution_datasets.insert(),
                [{"execution_id": execution.id, "dataset_id": idx} for idx in [1, 2]],
            )
        result = invoke_cli(["executions", "inspect", str(execution_group.id)])

        assert "Successful: True" in result.stdout
        file_regression.check(result.stdout)

    def test_inspect_failed(self, sample_data_dir, db_seeded, invoke_cli):
        # Create a diagnostic execution group with a result
        execution_group = ExecutionGroup(
            key="key1",
            diagnostic_id=1,
        )
        with db_seeded.session.begin():
            db_seeded.session.add(execution_group)
            db_seeded.session.flush()

            result = Execution(
                execution_group_id=execution_group.id,
                successful=False,
                output_fragment="output",
                dataset_hash="hash",
            )
            db_seeded.session.add(result)

        result = invoke_cli(["executions", "inspect", str(execution_group.id)])

        assert "Successful: False" in result.stdout

    def test_inspect_no_results(self, sample_data_dir, db_seeded, invoke_cli):
        metric_execution_group = ExecutionGroup(key="key1", diagnostic_id=1)
        with db_seeded.session.begin():
            db_seeded.session.add(metric_execution_group)

        result = invoke_cli(["executions", "inspect", str(metric_execution_group.id)])

        assert "not-started" in result.stdout

    def test_inspect_missing(self, invoke_cli):
        result = invoke_cli(["executions", "inspect", "999"], expected_exit_code=1)
        assert "Execution not found: 999" in result.stderr

    def test_results_directory_panel(self, tmp_path):
        tmp_path = tmp_path / "inner"
        tmp_path.mkdir()

        with open(tmp_path / "file1.txt", "w") as f:
            f.write("test")

        tmp_path.joinpath(".hidden").touch()

        inner_dir = tmp_path / "dir1"
        inner_dir.mkdir()
        inner_dir.joinpath("file2").touch()

        table = _results_directory_panel(tmp_path)

        console = Console()
        with console.capture() as capture:
            console.print(table)

        assert "file1.txt (4 bytes)" in capture.get()
        assert "┣━━ 📂 dir1" in capture.get()
        assert "hidden" not in capture.get()
