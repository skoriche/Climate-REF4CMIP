from cmip_ref.models import MetricExecution


def test_execution_help(invoke_cli):
    result = invoke_cli(["executions", "--help"])

    assert "View metric executions" in result.stdout


class TestExecutionList:
    def _setup_db(self, db):
        with db.session.begin():
            db.session.add(MetricExecution(key="key1", metric_id=1))
            db.session.add(MetricExecution(key="key2", metric_id=1))

    def test_list(self, sample_data_dir, db_seeded, invoke_cli):
        self._setup_db(db_seeded)

        result = invoke_cli(["executions", "list"])

        assert "key1" in result.stdout
        assert "key2" in result.stdout

    def test_list_limit(self, sample_data_dir, db_seeded, invoke_cli):
        self._setup_db(db_seeded)

        result = invoke_cli(["executions", "list", "--limit", "1"])

        assert "key1" in result.stdout
        assert "key2" not in result.stdout


class TestExecutionInspect:
    def test_inspect_no_results(self, sample_data_dir, db_seeded, invoke_cli, data_regression):
        metric_execution = MetricExecution(key="key1", metric_id=1)
        with db_seeded.session.begin():
            db_seeded.session.add(metric_execution)

        result = invoke_cli(["executions", "inspect", str(metric_execution.id)])

        assert "not-started" in result.stdout
