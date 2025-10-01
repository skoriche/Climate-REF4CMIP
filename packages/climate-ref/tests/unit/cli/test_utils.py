import pytest

from climate_ref.cli._utils import parse_facet_filters


def test_parse_facet_filters_valid_input():
    filters = ["source_id=GFDL-ESM4", "variable_id=tas"]
    expected = {"source_id": "GFDL-ESM4", "variable_id": "tas"}
    assert parse_facet_filters(filters) == expected


def test_parse_facet_filters_empty_list():
    assert parse_facet_filters([]) == {}


def test_parse_facet_filters_none_input():
    assert parse_facet_filters(None) == {}


def test_parse_facet_filters_with_whitespace():
    filters = ["  key1 = value1  ", "key2=value2 "]
    expected = {"key1": "value1", "key2": "value2"}
    assert parse_facet_filters(filters) == expected


def test_parse_facet_filters_duplicate_key(caplog):
    filters = ["key=value1", "key=value2"]
    expected = {"key": "value2"}
    with caplog.at_level("WARNING"):
        result = parse_facet_filters(filters)
    assert result == expected
    assert "Filter key 'key' specified multiple times. Using last value: 'value2'" in caplog.text


def test_parse_facet_filters_invalid_format_no_equals():
    with pytest.raises(ValueError, match="Invalid filter format: 'no_equals_sign'"):
        parse_facet_filters(["no_equals_sign"])


def test_parse_facet_filters_empty_key():
    with pytest.raises(ValueError, match="Empty key in filter: '=value'"):
        parse_facet_filters(["=value"])


def test_parse_facet_filters_empty_value():
    with pytest.raises(ValueError, match="Empty value in filter: 'key='"):
        parse_facet_filters(["key="])


def test_parse_facet_filters_value_with_equals():
    filters = ["query=some_key=some_value"]
    expected = {"query": "some_key=some_value"}
    assert parse_facet_filters(filters) == expected


def test_parse_facet_filters_mixed_valid_and_invalid(caplog):
    filters = ["key1=value1", "invalid", "key2=value2"]
    with pytest.raises(ValueError, match="Invalid filter format: 'invalid'"):
        parse_facet_filters(filters)
    assert not caplog.text
