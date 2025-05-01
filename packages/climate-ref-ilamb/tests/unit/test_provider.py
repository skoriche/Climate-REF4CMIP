import importlib.resources

from climate_ref_ilamb import __version__, provider


def test_provider():
    assert provider.name == "ILAMB"
    assert provider.slug == "ilamb"
    assert provider.version == __version__

    counts = []
    for f in importlib.resources.files("climate_ref_ilamb.configure").iterdir():
        with open(f) as fin:
            counts.append(fin.read().count("sources"))
    assert len(provider) == sum(counts)
