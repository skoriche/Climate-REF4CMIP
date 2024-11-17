import typing

import pandas as pd
from attrs import define

from ref.database import Database


@define
class MetricSolver:
    """
    A class that can solve which metrics need to be calculated
    """

    @staticmethod
    def build_from_db(db: Database) -> "MetricSolver":
        """
        Create a MetricSolver instance using information from the database

        Parameters
        ----------
        db
            Database instance

        Returns
        -------
        :
            A new MetricSolver instance
        """
        return MetricSolver()

    @staticmethod
    def build_from_dataframe(dataframe: pd.DataFrame) -> "MetricSolver":
        """
        Create a MetricSolver instance using information from a data catalog.

        This is useful in the cases where a database is not available or for testing.

        Parameters
        ----------
        db
            Database instance

        Returns
        -------
        :
            A new MetricSolver instance
        """
        return MetricSolver()

    def solve(self) -> list[typing.Any]:
        """
        Solve which metrics need to be calculated for a dataset

        Parameters
        ----------
        dataset
            Dataset to calculate metrics for

        Returns
        -------
        :
            List of metrics that need to be calculated
        """
        return []
