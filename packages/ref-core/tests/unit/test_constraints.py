import pandas as pd
import pytest
from ref_core.constraints import (
    GroupOperation,
    GroupValidator,
    RequireFacets,
    SelectParentExperiment,
    apply_constraint,
)
from ref_core.exceptions import ConstraintNotSatisfied


@pytest.fixture
def data_catalog():
    return pd.DataFrame(
        {
            "variable": ["tas", "pr", "rsut", "tas", "tas"],
            "source_id": ["CESM2", "CESM2", "CESM2", "ACCESS", "CAS"],
        }
    )


class TestRequireFacets:
    validator = RequireFacets(dimension="variable_id", required_facets=["tas", "pr"])

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


def test_apply_constraint_operation(data_catalog):
    #  operation that appends the "rsut" variable to the group
    class ExampleOperation(GroupOperation):
        def apply(self, group: pd.DataFrame, data_catalog: pd.DataFrame) -> pd.DataFrame:
            return pd.concat([group, data_catalog[data_catalog["variable"] == "rsut"]])

    result = apply_constraint(
        data_catalog[data_catalog["variable"] == "tas"],
        ExampleOperation(),
        data_catalog,
    )

    pd.testing.assert_frame_equal(
        result,
        pd.DataFrame(
            {
                "variable": ["tas", "tas", "tas", "rsut"],
                "source_id": ["CESM2", "ACCESS", "CAS", "CESM2"],
            },
            index=[0, 3, 4, 2],
        ),
    )


def test_apply_constraint_operation_mutable(data_catalog):
    class MutableOperation(GroupOperation):
        def apply(self, group: pd.DataFrame, data_catalog: pd.DataFrame) -> pd.DataFrame:
            group["variable"] = "new"
            return group

    orig_data_catalog = data_catalog.copy()
    result = apply_constraint(
        data_catalog,
        MutableOperation(),
        None,
    )

    assert (result["variable"] == "new").all()

    # Mutating the group impacts the original data catalog
    with pytest.raises(AssertionError):
        pd.testing.assert_frame_equal(data_catalog, orig_data_catalog)


def test_apply_constraint_operation_raises():
    class RaisesOperation(GroupOperation):
        def apply(self, group: pd.DataFrame, data_catalog: pd.DataFrame) -> pd.DataFrame:
            raise ConstraintNotSatisfied("Test exception")

    assert (
        apply_constraint(
            pd.DataFrame(),
            RaisesOperation(),
            pd.DataFrame(),
        )
        is None
    )


def test_apply_constraint_empty():
    assert (
        apply_constraint(
            pd.DataFrame(),
            RequireFacets(dimension="variable_id", required_facets=["tas", "pr"]),
            pd.DataFrame(),
        )
        is None
    )


def test_apply_constraint_validate(data_catalog):
    result = apply_constraint(
        data_catalog,
        RequireFacets(dimension="variable", required_facets=["tas", "pr"]),
        pd.DataFrame(),
    )
    pd.testing.assert_frame_equal(result, data_catalog)


def test_apply_constraint_validate_invalid(data_catalog):
    assert (
        apply_constraint(
            data_catalog,
            RequireFacets(dimension="variable", required_facets=["missing", "pr"]),
            pd.DataFrame(),
        )
        is None
    )
