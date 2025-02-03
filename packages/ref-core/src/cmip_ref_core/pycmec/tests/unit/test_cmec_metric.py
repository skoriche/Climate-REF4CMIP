from cmip_ref_core.pycmec.metric import (
    MetricCV,
    MetricDimensions,
    MetricResults,
    CMECMetric,
)

import pytest


@pytest.fixture
def cmec_right_metric_dict():
    return {
        "SCHEMA": {"name": "cmec", "version": "v1", "package": "ilamb v3"},
        "DIMENSIONS": {
            "json_structure": ["model", "metric"],
            "model": {
                "E3SM": {"name": "E3SM"},
                "CESM2": {"name": "CESM2"},
                "IPSL-CM5A-LR": {"name": "IPSL-CM5A-LR"},
            },
            "metric": {
                "Ecosystem and Carbon Cycle": {"name": "Ecosystem and Carbon Cycle"},
                "Hydrology Cycle": {"name": "Hydrology Cycle"},
            },
        },
        "RESULTS": {
            "E3SM": {
                "Ecosystem and Carbon Cycle": {"overall score": 0.11, "bias": 0.56},
                "Hydrology Cycle": {"overall score": 0.26, "bias": 0.70},
            },
            "CESM2": {
                "Ecosystem and Carbon Cycle": {"overall score": 0.05, "bias": 0.72},
                "Hydrology Cycle": {"overall score": 0.61, "bias": 0.18},
            },
            "IPSL-CM5A-LR": {
                "Ecosystem and Carbon Cycle": {
                    "overall score": 0.08,
                    "bias": 0.92,
                    "rmse": 0.34,
                },
                "Hydrology Cycle": {"overall score": 0.67, "rmse": 0.68},
            },
        },
    }


@pytest.fixture(params=["dict", "CMECMetric"])
def cmec_right_metric_data(request, cmec_right_metric_dict):
    if request.param == "dict":
        return cmec_right_metric_dict
    elif request.param == "CMECMetric":
        return CMECMetric(**cmec_right_metric_dict)


@pytest.fixture(params=["dict", "MetricDimensions"])
def cmec_right_dimen_data(request, cmec_right_metric_dict):
    if request.param == "dict":
        return cmec_right_metric_dict["DIMENSIONS"]
    elif request.param == "MetricDimensions":
        return MetricDimensions(**cmec_right_metric_dict["DIMENSIONS"])


@pytest.fixture
def cmec_right_result_dimobj(cmec_right_metric_dict):
    return cmec_right_metric_dict["RESULTS"], MetricDimensions(
        **cmec_right_metric_dict["DIMENSIONS"]
    )


@pytest.fixture
def cmec_wrongkw_metric_dict(cmec_right_metric_dict):
    return {
        "DIMENSIONS": {
            "xJSON": cmec_right_metric_dict["DIMENSIONS"]["json_structure"],
            "xmodel": cmec_right_metric_dict["DIMENSIONS"]["model"],
            "xmetric": cmec_right_metric_dict["DIMENSIONS"]["metric"],
        },
        "RESULTS": cmec_right_metric_dict["RESULTS"],
    }


@pytest.fixture
def cmec_wrongattr_metric_dict(cmec_right_metric_dict):
    return {
        "xDIMENSIONS": cmec_right_metric_dict["DIMENSIONS"],
        "xRESULTS": cmec_right_metric_dict["RESULTS"],
    }


@pytest.fixture
def cmec_wrongdim_metric_dict(cmec_right_metric_dict):
    cmec_right_metric_dict["DIMENSIONS"]["json_structure"] = ["model"]
    return cmec_right_metric_dict


def test_metric_right(cmec_right_metric_data):
    try:
        CMECMetric.model_validate(cmec_right_metric_data)
    except Exception as ex:
        assert False, "test metric object should be passed"


def test_dimen_right(cmec_right_dimen_data):
    try:
        MetricDimensions.model_validate(cmec_right_dimen_data)
    except Exception as ex:
        assert False, "test dimension object should be passed"


def test_metric_wrongkm(cmec_wrongkw_metric_dict):
    with pytest.raises(ValidationError):
        CMECMetric.model_validate(cmec_wrongkw_metric_dict)
        CMECMetric(**cmec_wrongkw_metric_dict)


def test_metric_wrongattr(cmec_wrongattr_metric_dict):
    with pytest.raises(ValidationError):
        CMECMetric.model_validate(cmec_wrongattr_metric_dict)
        CMECMetric(**cmec_wrongattr_metric_dict)


def test_metric_wrongdim(cmec_wrongdim_metric_dict):
    with pytest.raises(ValidationError):
        CMECMetric.model_validate(cmec_wrongdim_metric_dict)
        CMECMetric(**cmec_wrongdim_metric_dict)


def test_dimen_wrongdim(cmec_wrongdim_metric_dict):
    with pytest.raises(ValidationError):
        MetricDimensions.model_validate(cmec_wrongdim_metric_dict["DIMENSIONS"])
        MetricDimensions(DIMENSIONS=cmec_wrongdim_metric_dict["DIMENSIONS"])
