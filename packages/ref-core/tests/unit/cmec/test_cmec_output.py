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
            "modeldata": ["./modeldata", "./othermodels"],
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
        "data": None,
        "html": None,
        "metrics": None,
        "plots": None,
    }


def test_output_right(cmec_right_output_dict):
    CMECOutput.model_validate(cmec_right_output_dict)


def test_output_read_json(cmec_right_output_dict):
    import json
    import os

    dir_path = os.path.dirname(os.path.realpath(__file__))

    cmec_output_json = CMECOutput.load_from_json(
        jsonfile=dir_path + "/../test_datasets/cmec_output_sample.json"
    )

    assert cmec_right_output_dict == json.loads(cmec_output_json.model_dump_json(indent=2))


def test_output_update(cmec_right_output_dict):
    add_sname = "awesome_fig1"
    add_plots = {
        "long_name": "awesome figure",
        "filename": "fig_1.jpg",
        "description": "test add plots",
    }

    cmec_output = CMECOutput(**cmec_right_output_dict)

    cmec_output.update("plots", short_name=add_sname, dict_content=add_plots)

    assert cmec_output["plots"]["awesome_fig1"]["long_name"] == "awesome figure"
    assert cmec_output["plots"]["awesome_fig1"]["filename"] == "fig_1.jpg"
    assert cmec_output["plots"]["awesome_fig1"]["description"] == "test add plots"


def test_output_create_template():
    assert CMECOutput.create_template() == {
        "index": "index.html",
        "provenance": {
            "environment": {},
            "modeldata": [],
            "obsdata": {},
            "log": "cmec_output.log",
        },
        "data": {},
        "html": {},
        "metrics": {},
        "plots": {},
    }
