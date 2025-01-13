from typing import Protocol, runtime_checkable

import pandas as pd
from attrs import frozen
from loguru import logger

from cmip_ref_core.exceptions import ConstraintNotSatisfied


@runtime_checkable
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


@runtime_checkable
class GroupOperation(Protocol):
    """
    An operation to perform on a group of datasets resulting in a new group of datasets.

    !! warning

        Operations should not mutate the input group, but instead return a new group.
        Mutating the input group may result in unexpected behaviour.
    """

    def apply(self, group: pd.DataFrame, data_catalog: pd.DataFrame) -> pd.DataFrame:
        """
        Perform an operation on the group of datasets.

        A new group of datasets should be returned if modifications are required,
        and the input group should not be modified. If no modifications are required,
        return the input group unchanged.
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


GroupConstraint = GroupOperation | GroupValidator
"""
A constraint that must be satisfied when executing a given metric run.

This is applied to a group of datasets representing the inputs to a potential metric execution.
The group must satisfy all constraints to be processed.

This can include operations that are applied to a group of datasets which may modify the group,
but may also include validators that check if the group satisfies a certain condition.
"""


def apply_constraint(
    dataframe: pd.DataFrame, constraint: GroupConstraint, data_catalog: pd.DataFrame
) -> pd.DataFrame | None:
    """
    Apply a constraint to a group of datasets

    Parameters
    ----------
    constraint
        The constraint to apply
    data_catalog
        The data catalog of datasets

    Returns
    -------
    :
        The updated group of datasets or None if the constraint was not satisfied
    """
    try:
        updated_group = (
            constraint.apply(dataframe, data_catalog) if isinstance(constraint, GroupOperation) else dataframe
        )

        valid = constraint.validate(updated_group) if isinstance(constraint, GroupValidator) else True
        if not valid:
            logger.debug(f"Constraint {constraint} not satisfied for {dataframe}")
            raise ConstraintNotSatisfied(f"Constraint {constraint} not satisfied for {dataframe}")
    except ConstraintNotSatisfied:
        logger.debug(f"Constraint {constraint} not satisfied for {dataframe}")
        return None

    return updated_group


@frozen
class RequireFacets:
    """
    A constraint that requires a dataset to have certain facets.
    """

    dimension: str
    required_facets: list[str]

    def validate(self, group: pd.DataFrame) -> bool:
        """
        Check that the required facets are present in the group
        """
        if self.dimension not in group:
            logger.warning(f"Dimension {self.dimension} not present in group {group}")
            return False
        return all(value in group[self.dimension].values for value in self.required_facets)


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
        raise NotImplementedError("This is not implemented yet")  # pragma: no cover
