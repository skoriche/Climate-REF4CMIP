from climate_ref_pmp import __version__, provider


def test_provider():
    assert provider.name == "PMP"
    assert provider.slug == "pmp"
    assert provider.version == __version__

    assert len(provider) >= 1
