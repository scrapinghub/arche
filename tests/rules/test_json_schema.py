from arche.rules.json_schema import check_tags
from arche.rules.result import Level
from conftest import create_result
import pytest


tags_inputs = [
    (
        ["id"],
        None,
        {"category": ["id"]},
        {
            Level.INFO: [
                ("Used - category",),
                (
                    "Not used - name_field, product_price_field, "
                    "product_price_was_field, product_url_field, unique",
                ),
            ]
        },
    ),
    (
        ["key"],
        ["key"],
        {"unique": ["id"]},
        {
            Level.INFO: [
                ("Used - unique",),
                (
                    "Not used - category, name_field, product_price_field, "
                    "product_price_was_field, product_url_field",
                ),
            ],
            Level.ERROR: [
                ("'id' field(s) was not found in source, but specified in schema",),
                ("'id' field(s) was not found in target, but specified in schema",),
                ("Skipping tag rules",),
            ],
        },
    ),
    (
        ["key", "id"],
        ["key"],
        {"unique": ["id"]},
        {
            Level.INFO: [
                ("Used - unique",),
                (
                    "Not used - category, name_field, product_price_field, "
                    "product_price_was_field, product_url_field",
                ),
            ],
            Level.ERROR: [
                ("'id' field(s) was not found in target, but specified in schema",),
                ("Skipping tag rules",),
            ],
        },
    ),
    (
        ["key"],
        None,
        {"unique": ["id"]},
        {
            Level.INFO: [
                ("Used - unique",),
                (
                    "Not used - category, name_field, product_price_field, "
                    "product_price_was_field, product_url_field",
                ),
            ],
            Level.ERROR: [
                ("'id' field(s) was not found in source, but specified in schema",),
                ("Skipping tag rules",),
            ],
        },
    ),
    (
        ["_key"],
        None,
        {},
        {
            Level.INFO: [
                (
                    "Not used - category, name_field, product_price_field, "
                    "product_price_was_field, product_url_field, unique",
                )
            ]
        },
    ),
    (
        ["_key"],
        None,
        {
            "category": ["_key"],
            "name_field": ["_key"],
            "product_price_field": ["_key"],
            "product_price_was_field": ["_key"],
            "product_url_field": ["_key"],
            "unique": ["_key"],
        },
        {
            Level.INFO: [
                (
                    "Used - category, name_field, product_price_field, "
                    "product_price_was_field, product_url_field, unique",
                )
            ]
        },
    ),
]


@pytest.mark.parametrize(
    "source_columns, target_columns, tags, expected_messages", tags_inputs
)
def test_check_tags(source_columns, target_columns, tags, expected_messages):
    result = check_tags(source_columns, target_columns, tags)
    assert result == create_result("Tags", expected_messages)
