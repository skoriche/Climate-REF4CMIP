from unittest import mock

import pandas as pd
import pytest
from ref_core.constraints import RequireFacets, SelectParentExperiment
from ref_core.datasets import SourceDatasetType
from ref_core.metrics import DataRequirement, FacetFilter
from ref_metrics_example import provider

from ref.provider_registry import ProviderRegistry
from ref.solver import MetricSolver, extract_covered_datasets, solve_metrics


@pytest.fixture
def solver(db_seeded) -> MetricSolver:
    registry = ProviderRegistry(providers=[provider])

    # Use a fixed set of providers for the test suite until we can pull from the DB
    metric_solver = MetricSolver.build_from_db(db_seeded)
    metric_solver.provider_registry = registry
    return metric_solver


class TestMetricSolver:
    def test_solver_build_from_db(self, solver):
        assert isinstance(solver, MetricSolver)
        assert isinstance(solver.provider_registry, ProviderRegistry)
        assert solver.data_catalog == {}

    def test_solver_solve_empty(self, solver):
        assert len(list(solver.solve())) == 0


@pytest.mark.parametrize(
    "requirement,data_catalog,expected",
    [
        pytest.param(
            DataRequirement(
                source_type=SourceDatasetType.CMIP6,
                filters=(FacetFilter(facets={"variable_id": "missing"}),),
                group_by=("variable_id", "experiment_id"),
            ),
            pd.DataFrame(
                {
                    "variable_id": ["tas", "tas", "pr"],
                    "experiment_id": ["ssp119", "ssp126", "ssp119"],
                    "variant_label": ["r1i1p1f1", "r1i1p1f1", "r1i1p1f1"],
                }
            ),
            [],
            id="empty",
        ),
        pytest.param(
            DataRequirement(
                source_type=SourceDatasetType.CMIP6,
                filters=(FacetFilter(facets={"variable_id": "tas"}),),
                group_by=("variable_id", "experiment_id"),
            ),
            pd.DataFrame(
                {
                    "variable_id": ["tas", "tas", "pr"],
                    "experiment_id": ["ssp119", "ssp126", "ssp119"],
                    "variant_label": ["r1i1p1f1", "r1i1p1f1", "r1i1p1f1"],
                }
            ),
            [
                pd.DataFrame(
                    {
                        "variable_id": ["tas"],
                        "experiment_id": ["ssp119"],
                        "variant_label": ["r1i1p1f1"],
                    },
                    index=[0],
                ),
                pd.DataFrame(
                    {
                        "variable_id": ["tas"],
                        "experiment_id": ["ssp126"],
                        "variant_label": ["r1i1p1f1"],
                    },
                    index=[1],
                ),
            ],
            id="simple-filter",
        ),
        pytest.param(
            DataRequirement(
                source_type=SourceDatasetType.CMIP6,
                filters=(FacetFilter(facets={"variable_id": ("tas", "pr")}),),
                group_by=("experiment_id",),
            ),
            pd.DataFrame(
                {
                    "variable_id": ["tas", "tas", "pr"],
                    "experiment_id": ["ssp119", "ssp126", "ssp119"],
                }
            ),
            [
                pd.DataFrame(
                    {
                        "variable_id": ["tas", "pr"],
                        "experiment_id": ["ssp119", "ssp119"],
                    },
                    index=[0, 2],
                ),
                pd.DataFrame(
                    {
                        "variable_id": ["tas"],
                        "experiment_id": ["ssp126"],
                    },
                    index=[1],
                ),
            ],
            id="simple-or",
        ),
        pytest.param(
            DataRequirement(
                source_type=SourceDatasetType.CMIP6,
                filters=(FacetFilter(facets={"variable_id": ("tas", "pr")}),),
                constraints=(SelectParentExperiment(),),
                group_by=("variable_id", "experiment_id"),
            ),
            pd.DataFrame(
                {
                    "variable_id": ["tas", "tas"],
                    "experiment_id": ["ssp119", "historical"],
                    "parent_experiment_id": ["historical", "none"],
                }
            ),
            [
                pd.DataFrame(
                    {
                        "variable_id": ["tas", "tas"],
                        "experiment_id": ["historical", "ssp119"],
                    },
                    # The order of the rows is not guaranteed
                    index=[1, 0],
                ),
            ],
            marks=[pytest.mark.xfail(reason="Parent experiment not implemented")],
            id="parent",
        ),
        pytest.param(
            DataRequirement(
                source_type=SourceDatasetType.CMIP6,
                filters=(FacetFilter(facets={"variable_id": ("tas", "pr")}),),
                constraints=(RequireFacets(dimension="variable_id", required_facets=["tas", "pr"]),),
                group_by=("experiment_id",),
            ),
            pd.DataFrame(
                {
                    "variable_id": ["tas", "tas", "pr"],
                    "experiment_id": ["ssp119", "ssp126", "ssp119"],
                }
            ),
            [
                pd.DataFrame(
                    {
                        "variable_id": ["tas", "pr"],
                        "experiment_id": ["ssp119", "ssp119"],
                    },
                    index=[0, 2],
                ),
            ],
            id="simple-validation",
        ),
    ],
)
def test_data_coverage(requirement, data_catalog, expected):
    result = extract_covered_datasets(data_catalog, requirement)

    for res, exp in zip(result, expected):
        pd.testing.assert_frame_equal(res, exp)
    assert len(result) == len(expected)


@mock.patch("ref.solver.get_executor")
def test_solve_metrics_default_solver(mock_executor, db_seeded, solver):
    solve_metrics(db_seeded)

    assert mock_executor.return_value.run_metric.call_count == 0


@mock.patch("ref.solver.get_executor")
def test_solve_metrics(mock_executor, db_seeded, solver):
    solve_metrics(db_seeded, dry_run=False, solver=solver)

    assert mock_executor.return_value.run_metric.call_count == 1


def test_solve_metrics_dry_run(db_seeded):
    solve_metrics(db_seeded, dry_run=True)

    # TODO: Check that no new metrics were added to the db
