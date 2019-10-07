from arche.rules.json_schema import check_tags, validate
from arche.rules.result import Level
from conftest import *
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
        ["name"],
        ["name"],
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
        ["name", "id"],
        ["name"],
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
        ["name"],
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
        ["name"],
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
        ["name"],
        None,
        {
            "category": ["name"],
            "name_field": ["name"],
            "product_price_field": ["name"],
            "product_price_was_field": ["name"],
            "product_url_field": ["name"],
            "unique": ["name"],
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
    assert_results_equal(
        check_tags(source_columns, target_columns, tags),
        create_result("Tags", expected_messages),
    )


@pytest.mark.parametrize(
    "schema, expected_messages",
    [
        (
            {"type": "number"},
            {
                Level.ERROR: [
                    (
                        "4 (100%) items have 4 errors",
                        None,
                        {
                            "{'price': 0, 'name': 'Elizabeth'} is not of type 'number'": {
                                0
                            },
                            "{'name': 'Margaret'} is not of type 'number'": {1},
                            "{'price': 10, 'name': 'Yulia'} is not of type 'number'": {
                                2
                            },
                            "{'price': 11, 'name': 'Vivien'} is not of type 'number'": {
                                3
                            },
                        },
                    )
                ]
            },
        )
    ],
)
def test_validate(get_raw_items, schema, expected_messages):
    assert_results_equal(
        validate(schema, get_raw_items, range(len(get_raw_items))),
        create_result("JSON Schema Validation", expected_messages),
    )


def test_validate_passed(get_schema, get_raw_items):
    assert_results_equal(
        validate(get_schema, get_raw_items, range(len(get_raw_items))),
        create_result("JSON Schema Validation", {}),
    )
