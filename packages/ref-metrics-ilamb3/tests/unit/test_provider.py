from cmip_ref_metrics_ilamb3 import __version__, provider


def test_provider():
    assert provider.name == "ilamb3"
    assert provider.slug == "ilamb3"
    assert provider.version == __version__

    assert len(provider) == 1
