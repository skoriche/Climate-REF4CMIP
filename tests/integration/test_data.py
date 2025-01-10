def test_sample_data_available(sample_data_dir):
    top_level_files = list(sample_data_dir.rglob("*.nc"))
    assert top_level_files, "Run `make fetch-test-data` to download test data"

    print(top_level_files)
