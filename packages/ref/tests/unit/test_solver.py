import pytest

from ref.provider_registry import ProviderRegistry
from ref.solver import MetricSolver


@pytest.fixture
def solver(db) -> MetricSolver:
    return MetricSolver.build_from_db(db)


class TestMetricSolver:
    def test_solver_build_from_db(self, solver):
        assert isinstance(solver, MetricSolver)
        assert isinstance(solver.provider_registry, ProviderRegistry)
        assert solver.data_catalog == {}

    def test_solver_dry_run(self, solver):
        solver.solve(dry_run=True)

        # TODO: Check that nothing was solved

    def test_solver_solve_empty(self, solver):
        solver.solve()

        # TODO: Check that nothing was solved
