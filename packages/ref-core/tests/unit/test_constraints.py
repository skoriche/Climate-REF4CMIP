import pandas as pd
import pytest
from ref_core.constraints import (
    GroupOperation,
    GroupValidator,
    RequiredFacets,
    SelectParentExperiment,
    apply_constraint,
)


class TestRequiredFacets:
    validator = RequiredFacets(dimension="variable_id", required_facets=["tas", "pr"])

    def test_is_group_validator(self):
        assert isinstance(self.validator, GroupValidator)
        assert not isinstance(self.validator, GroupOperation)

    @pytest.mark.parametrize(
        "data, expected",
        [
            (pd.DataFrame({}), False),
            (pd.DataFrame({"invalid": ["tas", "pr"]}), False),
            (pd.DataFrame({"variable_id": ["tas", "pr"]}), True),
            (pd.DataFrame({"variable_id": ["tas", "pr"], "extra": ["a", "b"]}), True),
            (pd.DataFrame({"variable_id": ["tas"]}), False),
            (pd.DataFrame({"variable_id": ["tas"], "extra": ["a"]}), False),
        ],
    )
    def test_validate(self, data, expected):
        assert self.validator.validate(data) == expected


class TestSelectParentExperiment:
    def test_is_group_validator(self):
        validator = SelectParentExperiment()

        assert isinstance(validator, GroupOperation)
        assert not isinstance(validator, GroupValidator)


def test_apply_constraint_empty():
    assert (
        apply_constraint(
            pd.DataFrame(),
            RequiredFacets(dimension="variable_id", required_facets=["tas", "pr"]),
            pd.DataFrame(),
        )
        is None
    )
