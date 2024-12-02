"""
Solver to determine which metrics need to be calculated

This module provides a solver to determine which metrics need to be calculated.

This module is still a work in progress and is not yet fully implemented.
"""

import typing

import pandas as pd
from attrs import define
from loguru import logger
from ref_core.constraints import apply_constraint
from ref_core.datasets import SourceDatasetType
from ref_core.metrics import DataRequirement, Metric
from ref_core.providers import MetricsProvider

from ref.database import Database
from ref.provider_registry import ProviderRegistry


def extract_covered_datasets(data_catalog: pd.DataFrame, requirement: DataRequirement) -> list[pd.DataFrame]:
    """
    Determine the different metric executions that should be performed with the current data catalog
    """
    subset = requirement.apply_filters(data_catalog)

    if len(subset) == 0:
        logger.debug(f"No datasets found for requirement {requirement}")
        return []

    if requirement.group_by is None:
        # Use a single group
        groups = [(None, subset)]
    else:
        groups = subset.groupby(list(requirement.group_by))  # type: ignore

    results = []

    for name, group in groups:
        constrained_group = _process_group_constraints(data_catalog, group, requirement)

        if constrained_group is not None:
            results.append(constrained_group)

    return results


def _process_group_constraints(
    data_catalog: pd.DataFrame, group: pd.DataFrame, requirement: DataRequirement
) -> pd.DataFrame | None:
    for constraint in requirement.constraints or []:
        constrained_group = apply_constraint(group, constraint, data_catalog)
        if constrained_group is None:
            return None

        group = constrained_group
    return group


@define
class MetricSolver:
    """
    A solver to determine which metrics need to be calculated
    """

    provider_registry: ProviderRegistry
    data_catalog: dict[SourceDatasetType, pd.DataFrame]

    @staticmethod
    def build_from_db(db: Database) -> "MetricSolver":
        """
        Initialise the solver using information from the database

        Parameters
        ----------
        db
            Database instance

        Returns
        -------
        :
            A new MetricSolver instance
        """
        return MetricSolver(provider_registry=ProviderRegistry.build_from_db(db), data_catalog={})

    def _can_solve(self, metric: Metric) -> bool:
        """
        Determine if a metric can be solved

        This should probably be passed via DI
        """
        # TODO: Implement this method
        # TODO: wrap the result in a class representing a metric run
        return True

    def _find_solvable(self) -> typing.Generator[tuple[MetricsProvider, Metric], None, None]:
        """
        Find metrics that can be solved

        Returns
        -------
        :
            List of metrics that can be solved
        """
        for provider in self.provider_registry.providers:
            for metric in provider.metrics():
                if self._can_solve(metric):
                    yield (provider, metric)

    def solve(self, dry_run: bool = False, max_iterations: int = 10) -> None:
        """
        Solve which metrics need to be calculated for a dataset

        The solving scheme is iterative,
        for each iteration we find all metrics that can be solved and calculate them.
        After each iteration we check if there are any more metrics to solve.
        This may not be the most efficient way to solve the metrics, but it's a start.

        Parameters
        ----------
        dry_run
            If true, don't actually calculate the metrics instead just log what would be calculated
        max_iterations
            The maximum number of solving iterations to run
        """
        if dry_run:
            max_iterations = 1

        # Solve iteratively for now
        for iteration in range(max_iterations):
            logger.debug(f"Iteration {iteration}")
            solve_count = 0

            for provider, metric in self._find_solvable():
                logger.info(f"Calculating {metric}")

                if not dry_run:
                    pass
                    # run_metric(provider, metric, data_catalog=self.data_catalog)
                solve_count += 1

            logger.info(f"{solve_count} metrics calculated in iteration: {iteration}")
            if solve_count == 0:
                logger.info("No more metrics to solve")
                break


def solve_metrics(db: Database, dry_run: bool = False) -> None:
    """
    Solve for metrics that require recalculation

    This may trigger a number of additional calculations depending on what data has been ingested
    since the last solve.
    """
    solver = MetricSolver.build_from_db(db)

    logger.info("Solving for metrics that require recalculation...")
    solver.solve(dry_run=dry_run)
