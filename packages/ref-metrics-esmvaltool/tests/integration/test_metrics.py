import pytest
from cmip_ref_metrics_esmvaltool import provider

from cmip_ref.models import MetricExecutionResult as MetricExecutionResultModel
from cmip_ref.solver import solve_metric_executions
from cmip_ref.testing import validate_result
from cmip_ref_core.metrics import Metric

metrics = [pytest.param(metric, id=metric.slug) for metric in provider.metrics()]


@pytest.mark.slow
@pytest.mark.parametrize("metric", metrics)
def test_metrics(metric: Metric, data_catalog, tmp_path, config, mocker):
    mocker.patch.object(MetricExecutionResultModel, "metric_execution_group")
    # Ensure the conda prefix is set
    provider.configure(config)

    # Get the first match from the data catalog
    execution = next(
        solve_metric_executions(
            data_catalog=data_catalog,
            metric=metric,
            provider=provider,
        )
    )

    # Run the metric
    definition = execution.build_metric_execution_info(output_root=config.paths.scratch)
    definition.output_directory.mkdir(parents=True, exist_ok=True)
    result = metric.run(definition)

    # Check the result
    validate_result(config, result)
