from ref_core.metrics import Metric, MetricManager


class TestMetricManager:
    def test_manager_register(self, mock_metric):
        manager = MetricManager()
        manager.register(mock_metric)

        assert len(manager._metrics) == 1
        assert "mock" in manager._metrics
        assert isinstance(manager.get("mock"), Metric)
