from ref.provider_registry import ProviderRegistry
from ref.solver import MetricSolver


def test_solver_build_from_db(db):
    solver = MetricSolver.build_from_db(db)

    assert isinstance(solver, MetricSolver)
    assert isinstance(solver.provider_registry, ProviderRegistry)
    assert solver.data_catalog == {}


def test_solver_solve_empty(db):
    solver = MetricSolver.build_from_db(db)
    solver.solve()


def test_solver_solve_with_datasets(db):
    solver = MetricSolver.build_from_db(db)
    solver.solve()
