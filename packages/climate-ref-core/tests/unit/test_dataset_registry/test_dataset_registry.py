import importlib.resources
from pathlib import Path

import pytest

from climate_ref_core.dataset_registry import (
    DatasetRegistryManager,
    _verify_hash_matches,
    dataset_registry_manager,
    fetch_all_files,
)

NUM_OBS4REF_FILES = 58


@pytest.fixture
def fake_registry_file():
    file_path = Path(importlib.resources.files("climate_ref_core") / "fake_registry.txt")

    yield file_path, "climate_ref_core", "fake_registry.txt"

    # Clean up the fake registry file after the test
    if file_path.exists():
        file_path.unlink()


class TestDatasetRegistry:
    def setup_registry_file(self, fake_registry_file):
        registry_path, package, resource = fake_registry_file
        # Create a dummy resource file
        with registry_path.open("w") as f:
            f.write("file1.txt sha256:checksum1\n")
            f.write("file2.txt sha256:checksum2\n")
        return package, resource

    def test_dataset_registry(self):
        registry = DatasetRegistryManager()
        assert isinstance(registry, DatasetRegistryManager)
        assert len(registry._registries) == 0

    def test_register(self, fake_registry_file):
        registry = DatasetRegistryManager()
        name = "test_registry"
        base_url = "http://example.com"

        package, resource = self.setup_registry_file(fake_registry_file)

        registry.register(name, base_url, package, resource)

        assert name in registry._registries
        r = registry._registries[name]
        assert r.base_url == base_url + "/"
        assert len(r.registry_files) == 2

    def test_register_invalid(self, fake_registry_file):
        registry = DatasetRegistryManager()
        name = "test_registry"
        base_url = "http://example.com"

        package, resource = self.setup_registry_file(fake_registry_file)
        with fake_registry_file[0].open("a") as f:
            f.write("invalid-line\n")

        with pytest.raises(OSError):
            registry.register(name, base_url, package, resource)

    def test_getitem_missing(self):
        registry = DatasetRegistryManager()
        with pytest.raises(KeyError):
            registry["missing_registry"]

    def test_getitem(self, mocker, fake_registry_file):
        registry = DatasetRegistryManager()
        name = "test_registry"
        base_url = "http://example.com"

        mock_pooch = mocker.patch("climate_ref_core.dataset_registry.pooch")
        package, resource = self.setup_registry_file(fake_registry_file)

        mock_pooch_instance = mock_pooch.create.return_value
        registry.register(name, base_url, package, resource)
        retrieved_registry = registry[name]

        assert retrieved_registry == mock_pooch_instance

    @pytest.mark.parametrize(
        "cache_name, expected", [(None, "climate_ref"), ("custom_cache", "custom_cache")]
    )
    def test_with_cache_name(self, mocker, fake_registry_file, cache_name, expected):
        registry = DatasetRegistryManager()
        name = "test_registry"
        base_url = "http://example.com"

        mock_pooch = mocker.patch("climate_ref_core.dataset_registry.pooch")
        package, resource = self.setup_registry_file(fake_registry_file)

        registry.register(name, base_url, package, resource, cache_name=cache_name)

        mock_pooch.os_cache.assert_called_with(expected)
        assert name in registry._registries
        mock_pooch.create.assert_called_once()


@pytest.mark.parametrize("symlink", [True, False])
@pytest.mark.parametrize("verify", [True, False])
def test_fetch_all_files(mocker, tmp_path, symlink, verify):
    mock_verify = mocker.patch("climate_ref_core.dataset_registry._verify_hash_matches")

    downloaded_file = tmp_path / "out.txt"
    downloaded_file.write_text("foo")

    registry = dataset_registry_manager["obs4ref"]
    registry.fetch = mocker.MagicMock(return_value=downloaded_file)

    fetch_all_files(registry, "obs4ref", tmp_path, symlink=symlink, verify=verify)
    assert registry.fetch.call_count == NUM_OBS4REF_FILES

    key = "obs4REF/MOHC/HadISST-1-1/mon/ts/gn/v20210727/ts_mon_HadISST-1-1_PCMDI_gn_187001-201907.nc"
    expected_file = tmp_path / key

    assert expected_file.exists()
    assert expected_file.is_symlink() == symlink
    assert expected_file.read_text() == "foo"

    if verify:
        mock_verify.assert_any_call(expected_file, registry.registry[key])
    else:
        mock_verify.assert_not_called()


def test_verify_hash_matches(mocker, tmp_path):
    expected_hash = "sha256:expectedhashvalue"

    mock_hashes = mocker.patch("climate_ref_core.dataset_registry.pooch.hashes")
    mock_hashes.hash_algorithm.return_value = "sha256"
    mock_hashes.file_hash.return_value = "expectedhashvalue"

    file_path = tmp_path / "file.txt"
    file_path.touch()

    _verify_hash_matches(file_path, expected_hash)


def test_verify_hash_missing_file(tmp_path):
    expected_hash = "sha256:expectedhashvalue"

    file_path = tmp_path / "file.txt"

    with pytest.raises(FileNotFoundError, match=r"file.txt does not exist. Cannot verify hash"):
        _verify_hash_matches(file_path, expected_hash)


def test_verify_hash_differs(mocker, tmp_path):
    expected_hash = "sha256:expectedhashvalue"

    mock_hashes = mocker.patch("climate_ref_core.dataset_registry.pooch.hashes")
    mock_hashes.hash_algorithm.return_value = "sha256"
    mock_hashes.file_hash.return_value = "opps"

    file_path = tmp_path / "file.txt"
    file_path.touch()

    with pytest.raises(
        ValueError, match=f"does not match the known hash. expected {expected_hash} but got opps."
    ):
        _verify_hash_matches(file_path, expected_hash)


def test_fetch_all_files_no_output(mocker):
    registry = dataset_registry_manager["obs4ref"]
    registry.fetch = mocker.MagicMock()

    fetch_all_files(registry, "obs4ref", None)
    assert registry.fetch.call_count == NUM_OBS4REF_FILES
