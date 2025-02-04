import pytest

from cmip_ref_core.pycmec.output import (
    CMECOutput,
)


@pytest.fixture
def cmec_right_output_dict():
    return {
        "provenance": {
            "environment": {
                "OS": "LINUX",
                "Package": "ILAMB",
                "Machine": "Frontier",
                "Variable": "Biomass",
            },
            "modeldata": ['./modeldata", "./othermodels'],
            "obsdata": {
                "GlobalCarbon": {
                    "version": "5.1",
                    "title": "Global forest live biomass carbon",
                },
                "NBCD2000": {
                    "version": "unknown",
                    "name": "National Biomass and Carbon data set for the Year 2000",
                },
            },
            "log": "cmec_output.log",
        },
        "index": "index.html",
    }


def test_output_right(cmec_right_output_dict):
    try:
        CMECOutput.model_validate(cmec_right_output_dict)
    except Exception:
        assert False, "test output object should be passed"
