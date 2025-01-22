from cmip_ref_metrics_example import __version__, provider


def test_provider():
    assert provider.name == "Example"
    assert provider.slug == "example"
    assert provider.version == __version__

    assert len(provider) == 1
