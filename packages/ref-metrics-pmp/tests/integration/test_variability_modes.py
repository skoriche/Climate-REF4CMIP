import pytest
from cmip_ref_metrics_pmp import provider
from cmip_ref_metrics_pmp.variability_modes import ExtratropicalModesOfVariability

from cmip_ref.models import MetricExecutionResult as MetricExecutionResultModel
from cmip_ref.solver import solve_metric_executions
from cmip_ref.testing import validate_result

variability_metrics = [
    pytest.param(
        metric,
        id=metric.slug,
        marks=(
            pytest.mark.xfail(reason="https://github.com/Climate-REF/climate-ref/issues/258")
            if metric.slug
            in {
                "pmp-extratropical-modes-of-variability-pdo",
                "pmp-extratropical-modes-of-variability-npgo",
            }
            else ()
        ),
    )
    for metric in provider.metrics()
    if isinstance(metric, ExtratropicalModesOfVariability)
]


@pytest.mark.slow
@pytest.mark.parametrize("metric", variability_metrics)
def test_variability_modes(metric: ExtratropicalModesOfVariability, data_catalog, tmp_path, config, mocker):
    mocker.patch.object(MetricExecutionResultModel, "metric_execution_group")

    # Ensure the conda prefix is set
    provider.configure(config)

    if metric.mode_id in ExtratropicalModesOfVariability.psl_modes:
        pytest.xfail("Missing PSL sample data")

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
    result = metric.run(definition)

    # Check the result
    validate_result(config, result)
