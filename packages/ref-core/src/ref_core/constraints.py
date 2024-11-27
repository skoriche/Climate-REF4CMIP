from typing import Protocol

import pandas as pd
from attrs import frozen


class GroupValidator(Protocol):
    """
    A constraint that must be satisfied when executing a given metric run.

    All constraints must be satisfied for a given group to be run.
    """

    def validate(self, group: pd.DataFrame) -> bool:
        """
        Validate if the constraint is satisfied by the dataset.

        This is executed after the apply method to determine if the constraint is satisfied.
        If the constraint is not satisfied, the group will not be executed.

        Parameters
        ----------
        group
            A group of datasets that is being validated.

        Returns
        -------
        :
            Whether the constraint is satisfied
        """
        ...


class GroupOperation(Protocol):
    """
    An operation to perform on a group of datasets resulting in a new group of datasets.
    """

    def apply(self, group: pd.DataFrame, data_catalog: pd.DataFrame) -> pd.DataFrame:
        """
        Perform an operation on the group of datasets.

        If this operation modifies the group, the modified group should be returned.
        If this operation fails, a ConstraintNotSatisfied exception should be raised.

        Parameters
        ----------
        group
            A group of datasets that is being validated.
        data_catalog
            The data catalog of datasets

        Raises
        ------
        ConstraintNotSatisfied
            The operation was not successful

        Returns
        -------
        :
            The updated group of datasets
        """
        ...


Constraint = GroupOperation | GroupValidator
"""
A constraint that must be satisfied when executing a given metric run.

This can include operations that are applied to a group of datasets which may modify the group,
but may also include validators that check if the group satisfies a certain condition.

All constraints must be satisfied for a given group to be run.
If any constraint is not satisfied, the group will not be executed.
"""


@frozen
class RequiredFacets:
    """
    A constraint that requires a dataset to have certain facets.
    """

    dimension: str
    required_facets: list[str]

    def validate(self, group: pd.DataFrame) -> bool:
        """
        Check that the required facets are present in the group
        """
        return group[self.dimension].isin(self.required_facets).all()


@frozen
class SelectParentExperiment:
    """
    Include a dataset's parent experiment in the selection
    """

    def apply(self, group: pd.DataFrame, data_catalog: pd.DataFrame) -> pd.DataFrame:
        """
        Include a dataset's parent experiment in the selection

        Not yet implemented
        """
        raise NotImplementedError("This is not implemented yet")
