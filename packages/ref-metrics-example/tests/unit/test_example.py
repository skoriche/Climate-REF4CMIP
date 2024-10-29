from ref_core.providers import Configuration
from ref_metrics_example.example import ExampleMetric


def test_example_metric(tmp_path):
    metric = ExampleMetric()

    configuration = Configuration(
        output_directory=tmp_path,
    )

    result = metric.run(configuration)

    assert result.successful
    assert result.output_bundle.exists()
    assert result.output_bundle.is_file()
    assert result.output_bundle.name == "output.json"
