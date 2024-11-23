import typing

import pandas as pd
from attrs import define
from loguru import logger
from ref_core.datasets import SourceDatasetType
from ref_core.metrics import Metric
from ref_core.providers import MetricsProvider

from ref.database import Database
from ref.provider_registry import ProviderRegistry


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
                    yield provider, metric

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
