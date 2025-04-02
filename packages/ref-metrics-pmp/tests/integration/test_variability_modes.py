import pytest
from cmip_ref_metrics_pmp import provider
from cmip_ref_metrics_pmp.variability_modes import ExtratropicalModesOfVariability

from cmip_ref.solver import solve_metric_executions

variability_metrics = [
    pytest.param(metric, id=metric.slug)
    for metric in provider.metrics()
    if isinstance(metric, ExtratropicalModesOfVariability)
]


@pytest.mark.parametrize("metric", variability_metrics[2:])
def test_variability_modes(metric: ExtratropicalModesOfVariability, data_catalog, tmp_path, config):
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
    definition = execution.build_metric_execution_info(output_root=tmp_path)
    result = metric.run(definition)

    # Check the result
    assert result.successful
