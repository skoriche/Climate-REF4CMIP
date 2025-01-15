from cmip_ref_metrics_esmvaltool import __version__, provider


def test_provider():
    assert provider.name == "ESMValTool"
    assert provider.slug == "esmvaltool"
    assert provider.version == __version__

    assert len(provider) == 2
