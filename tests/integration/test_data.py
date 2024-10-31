def test_esgf_data_available(esgf_data_dir):
    top_level_files = list(esgf_data_dir.rglob("*.nc"))
    assert top_level_files, "Run `make fetch-test-data` to download test data"

    print(top_level_files)
