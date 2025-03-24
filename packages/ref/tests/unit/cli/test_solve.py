def test_solve_help(invoke_cli):
    result = invoke_cli(["solve", "--help"])

    assert "Solve for metrics that require recalculation" in result.stdout


class TestSolve:
    def test_solve(self, sample_data_dir, db, invoke_cli, mocker):
        mock_solve = mocker.patch("cmip_ref.cli.solve.solve_metrics")
        invoke_cli(["solve"])

        assert mock_solve.call_count == 1
        args, kwargs = mock_solve.call_args

        assert kwargs["timeout"] == 60
        assert not kwargs["dry_run"]

    def test_solve_with_timeout(self, sample_data_dir, db, invoke_cli, mocker):
        mock_solve = mocker.patch("cmip_ref.cli.solve.solve_metrics")
        invoke_cli(["solve", "--timeout", "10"])

        args, kwargs = mock_solve.call_args
        assert kwargs["timeout"] == 10

    def test_solve_with_dryrun(self, sample_data_dir, db, invoke_cli, mocker):
        mock_solve = mocker.patch("cmip_ref.cli.solve.solve_metrics")
        invoke_cli(["solve", "--dry-run"])

        args, kwargs = mock_solve.call_args
        assert kwargs["dry_run"]
