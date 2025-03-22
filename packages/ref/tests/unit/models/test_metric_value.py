import pytest

from cmip_ref.models import MetricValue


class TestMetricValue:
    @pytest.mark.parametrize(
        "attributes",
        (
            {
                "key": "value",
            },
            None,
        ),
    )
    def test_build(self, db_seeded, attributes):
        item_orig = MetricValue.build(
            metric_execution_result_id=1, value=1.0, attributes=attributes, dimensions={"model": "test"}
        )
        db_seeded.session.add(item_orig)
        db_seeded.session.commit()

        item = db_seeded.session.query(MetricValue).get(item_orig.id)
        assert item.attributes == attributes
        assert item.value == 1.0

        assert item.dimensions == {"model": "test"}

    def test_invalid_dimension(self, db_seeded):
        exp_msg = "'not_a_dimension' is an invalid keyword argument for MetricValue"
        with pytest.raises(TypeError, match=exp_msg):
            MetricValue.build(
                metric_execution_result_id=1,
                value=1.0,
                attributes=None,
                dimensions={"not_a_dimension": "test"},
            )
