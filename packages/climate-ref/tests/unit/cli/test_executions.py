import datetime
import pathlib

import pytest
from climate_ref_esmvaltool import provider as esmvaltool_provider
from climate_ref_pmp import provider as pmp_provider
from rich.console import Console

from climate_ref.cli.executions import _results_directory_panel
from climate_ref.models import Execution, ExecutionGroup
from climate_ref.models.diagnostic import Diagnostic
from climate_ref.models.execution import execution_datasets
from climate_ref.provider_registry import _register_provider


@pytest.fixture
def db_with_groups(db_seeded):
    """Fixture to set up a database with various execution groups for testing filters."""
    with db_seeded.session.begin():
        _register_provider(db_seeded, pmp_provider)
        _register_provider(db_seeded, esmvaltool_provider)

        # Diagnostic 1, Provider 1, Facets: source_id=GFDL-ESM4, variable_id=tas
        diag_1 = (
            db_seeded.session.query(Diagnostic).filter_by(slug="enso_tel").first()
        )  # ENSO diagnostic from PMP
        eg1 = ExecutionGroup(
            key="key1",
            diagnostic_id=diag_1.id,
            selectors={"cmip6": [["source_id", "GFDL-ESM4"], ["variable_id", "tas"]]},
        )
        db_seeded.session.add(eg1)

        # Diagnostic 2, Provider 1, Facets: source_id=ACCESS-ESM1-5, variable_id=pr
        diag_2 = (
            db_seeded.session.query(Diagnostic)
            .filter_by(slug="extratropical-modes-of-variability-nao")
            .first()
        )  # Mode of variability diagnostic from PMP
        eg2 = ExecutionGroup(
            key="key2",
            diagnostic_id=diag_2.id,
            selectors={"cmip6": [["source_id", "ACCESS-ESM1-5"], ["variable_id", "pr"]]},
        )
        db_seeded.session.add(eg2)

        # Diagnostic 1, Provider 2, Facets: source_id=CNRM-CM6-1, variable_id=tas
        diag_3 = (
            db_seeded.session.query(Diagnostic).filter_by(slug="enso-characteristics").first()
        )  # ENSO diagnostic from ESMValTool
        eg3 = ExecutionGroup(
            key="key3",
            diagnostic_id=diag_3.id,
            selectors={"cmip6": [["source_id", "CNRM-CM6-1"], ["variable_id", "tas"]]},
        )
        db_seeded.session.add(eg3)

        # Diagnostic 4, Provider 2, No specific facets (or different ones)
        diag_4 = (
            db_seeded.session.query(Diagnostic).filter_by(slug="sea-ice-area-basic").first()
        )  # ENSO diagnostic from ESMValTool
        eg4 = ExecutionGroup(
            key="key4", diagnostic_id=diag_4.id, selectors={"cmip6": [["experiment_id", "historical"]]}
        )
        db_seeded.session.add(eg4)

        # Add some executions to avoid "not-started" status
        db_seeded.session.flush()
        db_seeded.session.add(
            Execution(
                execution_group_id=eg1.id, successful=True, output_fragment="out1", dataset_hash="hash1"
            )
        )
        db_seeded.session.add(
            Execution(
                execution_group_id=eg2.id, successful=True, output_fragment="out2", dataset_hash="hash2"
            )
        )
        db_seeded.session.add(
            Execution(
                execution_group_id=eg3.id, successful=False, output_fragment="out3", dataset_hash="hash3"
            )
        )
        db_seeded.session.add(
            Execution(
                execution_group_id=eg4.id, successful=True, output_fragment="out4", dataset_hash="hash4"
            )
        )

        # Add a dirty execution group
        eg5 = ExecutionGroup(
            key="key5",
            diagnostic_id=diag_4.id,
            selectors={"cmip6": [["experiment_id", "historical"]]},
            dirty=True,
        )
        db_seeded.session.add(eg5)
        db_seeded.session.flush()
        db_seeded.session.add(
            Execution(
                execution_group_id=eg5.id, successful=True, output_fragment="out5", dataset_hash="hash5"
            )
        )

        # Add an execution group with no executions (not-started)
        eg6 = ExecutionGroup(
            key="key6", diagnostic_id=diag_4.id, selectors={"cmip6": [["experiment_id", "ssp126"]]}
        )
        db_seeded.session.add(eg6)
    return db_seeded


def test_execution_help(invoke_cli):
    result = invoke_cli(["executions", "--help"])

    assert "View execution groups" in result.stdout


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


class TestListGroupsFiltering:
    def test_filter_by_diagnostic(self, db_with_groups, invoke_cli):
        result = invoke_cli(["executions", "list-groups", "--diagnostic", "enso"])
        assert "enso" in result.stdout
        assert "extratropical-modes-of-variability-nao" not in result.stdout
        assert "sea-ice-area-basic" not in result.stdout

    def test_filter_by_provider(self, db_with_groups, invoke_cli):
        result = invoke_cli(["executions", "list-groups", "--provider", "pmp"])

        print(result.stdout)
        assert "pmp" in result.stdout
        assert "esmvaltool" not in result.stdout

    def test_filter_by_facet(self, db_with_groups, invoke_cli):
        result = invoke_cli(["executions", "list-groups", "--filter", "source_id=GFDL-ESM4"])
        assert "key1" in result.stdout
        assert "key2" not in result.stdout
        assert "key3" not in result.stdout

    def test_filter_combined(self, db_with_groups, invoke_cli):
        result = invoke_cli(
            [
                "executions",
                "list-groups",
                "--diagnostic",
                "enso",
                "--provider",
                "pmp",
                "--filter",
                "source_id=GFDL-ESM4",
                "--filter",
                "variable_id=tas",
            ]
        )
        assert "key1" in result.stdout
        assert "key2" not in result.stdout
        assert "key3" not in result.stdout
        assert "key4" not in result.stdout

    def test_filter_multiple_diagnostic_or(self, db_with_groups, invoke_cli):
        result = invoke_cli(
            [
                "executions",
                "list-groups",
                "--diagnostic",
                "enso",
                "--diagnostic",
                "extratropical-modes-of-variability-nao",
            ]
        )
        assert "enso" in result.stdout
        assert "extratropical-modes-of-variability-nao" in result.stdout
        assert "sea-ice-area-basic" not in result.stdout

    def test_filter_multiple_provider_or(self, db_with_groups, invoke_cli):
        result = invoke_cli(["executions", "list-groups", "--provider", "pmp", "--provider", "esmvaltool"])
        assert "pmp" in result.stdout
        assert "esmvaltool" in result.stdout

    def test_filter_multiple_facet_and(self, db_with_groups, invoke_cli):
        result = invoke_cli(
            [
                "executions",
                "list-groups",
                "--filter",
                "source_id=GFDL-ESM4",
                "--filter",
                "variable_id=tas",
            ]
        )
        assert "key1" in result.stdout
        assert "key2" not in result.stdout
        assert "key3" not in result.stdout
        assert "key4" not in result.stdout

    def test_filter_invalid_syntax(self, invoke_cli):
        result = invoke_cli(
            ["executions", "list-groups", "--filter", "invalid_no_equals"], expected_exit_code=1
        )
        assert "Invalid filter format" in result.stderr

    def test_filter_empty_results_warning(self, db_with_groups, invoke_cli):
        # Warn if no results after filtering
        result = invoke_cli(["executions", "list-groups", "--filter", "source_id=NONEXISTENT"])
        assert "No execution groups match the specified filters." in result.stderr
        assert "Total execution groups in database:" in result.stderr
        assert "Applied filters: facet filters: ['source_id=NONEXISTENT']" in result.stderr
        assert "id" in result.stdout  # Ensure empty table headers are still printed

    def test_facet_warning_multiple_same_key(self, db_with_groups, invoke_cli):
        # This functionality might be useful in future, but not today
        result = invoke_cli(
            [
                "executions",
                "list-groups",
                "--filter",
                "source_id=GFDL-ESM4",
                "--filter",
                "source_id=ACCESS-ESM1-5",
            ]
        )
        assert (
            "Filter key 'source_id' specified multiple times. Using last value: 'ACCESS-ESM1-5'"
            in result.stderr
        )
        assert "ACCESS-ESM1-5" in result.stdout
        assert "GFDL-ESM4" not in result.stdout

    def test_filter_successful(self, db_with_groups, invoke_cli):
        result = invoke_cli(["executions", "list-groups", "--successful"])
        # Should include key1, key2, key4, key5 (successful=True),
        # exclude key3 (successful=False), exclude key6 (no executions)
        assert "key1" in result.stdout
        assert "key2" in result.stdout
        assert "key3" not in result.stdout
        assert "key4" in result.stdout
        assert "key5" in result.stdout
        assert "key6" not in result.stdout

    def test_filter_not_successful(self, db_with_groups, invoke_cli):
        result = invoke_cli(["executions", "list-groups", "--not-successful"])
        # Should include key3 (successful=False) and key6 (no executions), exclude successful ones
        assert "key1" not in result.stdout
        assert "key2" not in result.stdout
        assert "key3" in result.stdout
        assert "key4" not in result.stdout
        assert "key5" not in result.stdout
        assert "key6" in result.stdout

    def test_filter_dirty(self, db_with_groups, invoke_cli):
        result = invoke_cli(["executions", "list-groups", "--dirty"])
        # Should include key5 (dirty=True), exclude others (dirty=False by default)
        assert "key1" not in result.stdout
        assert "key2" not in result.stdout
        assert "key3" not in result.stdout
        assert "key4" not in result.stdout
        assert "key5" in result.stdout
        assert "key6" not in result.stdout

    def test_filter_not_dirty(self, db_with_groups, invoke_cli):
        result = invoke_cli(["executions", "list-groups", "--not-dirty"])
        # Should include key1, key2, key3, key4, key6 (dirty=False), exclude key5 (dirty=True)
        assert "key1" in result.stdout
        assert "key2" in result.stdout
        assert "key3" in result.stdout
        assert "key4" in result.stdout
        assert "key5" not in result.stdout
        assert "key6" in result.stdout


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

    def test_flag_dirty(self, sample_data_dir, db_seeded, invoke_cli, config):
        config.paths.results = pathlib.Path("/results")
        config.save()
        execution_group = ExecutionGroup(
            key="key1",
            diagnostic_id=1,
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
        assert "Dirty: False" in result.stdout
        result = invoke_cli(["executions", "flag-dirty", str(execution_group.id)])
        assert "Dirty: True" in result.stdout

    def test_flag_dirty_missing(self, db_seeded, invoke_cli):
        invoke_cli(["executions", "flag-dirty", "123"], expected_exit_code=1)
