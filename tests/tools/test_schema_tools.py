import logging

from arche.tools.schema import infer_schema, JsonFields, set_item_no
import pytest


def test_set_item_no():
    assert set_item_no(4) == [0, 1, 2, 3]
    assert set_item_no(1) == [0]
    assert len(set_item_no(5)) == 4
    assert len(set_item_no(124112414)) == 4


def test_infer_schema():
    item1 = {"url": "https://example.com", "_key": 0}
    item2 = {"url": "https://example.com", "_key": 1}
    schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "definitions": {
            "float": {"pattern": r"^-?[0-9]+\.[0-9]{2}$"},
            "url": {
                "pattern": (
                    r"^https?://(www\.)?[a-z0-9.-]*\.[a-z]{2,}"
                    r"([^<>%\x20\x00-\x1f\x7F]|%[0-9a-fA-F]{2})*$"
                )
            },
        },
        "type": "object",
        "properties": {"url": {"type": "string"}, "_key": {"type": "integer"}},
        "additionalProperties": False,
        "required": ["_key", "url"],
    }
    assert infer_schema([item1, item2]) == schema
    assert infer_schema([item2]) == schema


@pytest.mark.parametrize(
    "schema, expected_tags",
    [
        (
            {"properties": {"id": {"tag": "unique"}, "url": {"tag": "unique"}}},
            {"unique": ["id", "url"]},
        ),
        ({}, {}),
        ({"properties": {"id": {"tag": "unique"}, "url": {}}}, {"unique": ["id"]}),
    ],
)
def test_get_tags(schema, expected_tags):
    jf = JsonFields(schema)
    assert jf.schema == schema
    assert jf.tagged == expected_tags


@pytest.mark.parametrize(
    "tags, field, expected_tags, expected_warning",
    [
        ("name_field", "name", {"name_field": ["name"]}, []),
        (
            ["name_field", "unique"],
            "name",
            {"name_field": ["name"], "unique": ["name"]},
            [],
        ),
        (None, None, {}, ["'None' tag value is invalid, should be str or list[str]"]),
        (
            ["name_field", "t"],
            "name",
            {"name_field": ["name"]},
            [
                (
                    "{'t'} tag(s) are unsupported, valid tags are:\ncategory, category_field, "
                    "name_field, product_price_field, product_price_was_field, "
                    "product_url_field, unique"
                )
            ],
        ),
        ("category", "state", {"category": ["state"]}, []),
    ],
)
def test_get_field_tags(caplog, tags, field, expected_tags, expected_warning):
    jf = JsonFields({})
    caplog.clear()
    jf.get_field_tags(tags, field)
    assert jf.tagged == expected_tags
    for m in expected_warning:
        assert caplog.record_tuples == [("arche.tools.schema", logging.WARNING, m)]


@pytest.mark.parametrize(
    "value, expected",
    [
        ("name_field", {"name_field"}),
        (["name_field", "unique"], {"name_field", "unique"}),
        (3, None),
    ],
)
def test_parse_tag(value, expected):
    assert JsonFields({}).parse_tag(value) == expected
