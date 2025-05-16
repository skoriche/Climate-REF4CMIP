import re

import pytest

from climate_ref_core.metric_values.typing import SeriesMetricValue


class TestSeriesMetricValue:
    def test_matching_lengths(self):
        value = SeriesMetricValue(
            dimensions={"model": "test"},
            values=[1.0, 2.0, 3.0],
            index=[0, 1, 2],
            index_name="time",
            attributes={"attr": "value"},
        )
        assert value.values == [1.0, 2.0, 3.0]
        assert value.index == [0, 1, 2]
        assert value.index_name == "time"
        assert value.dimensions == {"model": "test"}
        assert value.attributes == {"attr": "value"}

    def test_mismatched_lengths(self):
        with pytest.raises(ValueError, match=re.escape("Index length (2) must match values length (3)")):
            SeriesMetricValue(
                dimensions={"model": "test"},
                values=[1.0, 2.0, 3.0],
                index=[0, 1],
                index_name="time",
                attributes=None,
            )

    @pytest.mark.parametrize(
        "index",
        [
            [0, "1", 2.0],
            ["apr", "may", "jun"],
        ],
    )
    def test_index_types(self, index):
        value = SeriesMetricValue(
            dimensions={"model": "test"},
            values=[1.0, 2.0, 3.0],
            index=index,
            index_name="time",
        )
        assert value.values == [1.0, 2.0, 3.0]
        assert value.index == index

    def test_str_values(self):
        value = SeriesMetricValue(
            dimensions={"model": "test"},
            values=["1"],
            index=[1.0],
            index_name="time",
        )
        assert value.values == [1.0]

        with pytest.raises(
            ValueError, match="Input should be a valid number, unable to parse string as a number"
        ):
            SeriesMetricValue(
                dimensions={"model": "test"},
                values=["a"],
                index=[1.0],
                index_name="time",
            )
