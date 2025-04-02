import pytest

from cmip_ref_core.dataset_registry import build_reference_data_registry, fetch_all_files


@pytest.mark.parametrize("symlink", [True, False])
def test_fetch_all_files(mocker, tmp_path, symlink):
    downloaded_file = tmp_path / "out.txt"
    downloaded_file.write_text("foo")

    registry = build_reference_data_registry()
    registry.fetch = mocker.MagicMock(return_value=downloaded_file)

    fetch_all_files(registry, tmp_path, symlink=symlink)
    assert registry.fetch.call_count == 2

    expected_file = (
        tmp_path
        / "obs4MIPs_PCMDI_monthly/MOHC/HadISST-1-1/mon/ts/gn/v20210727/ts_mon_HadISST-1-1_PCMDI_gn_187001-201907.nc"  # noqa: E501
    )

    assert expected_file.exists()
    assert expected_file.is_symlink() == symlink
    assert expected_file.read_text() == "foo"
