def test_solve_help(invoke_cli):
    result = invoke_cli(["solve", "--help"])

    assert "Solve for metrics that require recalculation" in result.output


class TestSolve:
    def test_solve_without_datasets(self, esgf_data_dir, db, invoke_cli):
        # TODO: Implement this test
        result = invoke_cli(["solve"])  # noqa
