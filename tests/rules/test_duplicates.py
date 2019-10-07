import arche.rules.duplicates as duplicates
from arche.rules.result import Level, Outcome
from conftest import *
import numpy as np
import pandas as pd
import pytest


unique_inputs = [
    ({}, {}, {Level.INFO: [(Outcome.SKIPPED,)]}),
    (
        {"id": ["0", "0", "1"]},
        {"unique": ["id"]},
        {
            Level.ERROR: [
                ("id contains 1 duplicated value(s)", None, {"same '0' `id`": [0, 1]})
            ]
        },
    ),
    (
        {
            "id": ["47" for x in range(6)],
            "name": ["Walt", "Juan", "Juan", "Walt", "Walt", "John"],
        },
        {"unique": ["id", "name"]},
        {
            Level.ERROR: [
                (
                    "id contains 1 duplicated value(s)",
                    None,
                    {"same '47' `id`": [i for i in range(6)]},
                ),
                (
                    "name contains 2 duplicated value(s)",
                    None,
                    {"same 'Juan' `name`": [1, 2], "same 'Walt' `name`": [0, 3, 4]},
                ),
            ]
        },
    ),
    ({"name": ["a", "b"]}, {"unique": ["name"]}, {}),
]


@pytest.mark.parametrize("data, tagged_fields, expected_messages", unique_inputs)
def test_find_by_unique(data, tagged_fields, expected_messages):
    df = pd.DataFrame(data)
    assert_results_equal(
        duplicates.find_by_unique(df, tagged_fields),
        create_result(
            "Duplicates By **unique** Tag", expected_messages, items_count=len(df)
        ),
    )


@pytest.mark.parametrize(
    "data, columns, expected_messages",
    [
        (
            {"id": ["0", "0", "1"]},
            ["id"],
            {
                Level.ERROR: [
                    ("2 duplicate(s) with same id", None, {"same '0' `id`": [0, 1]})
                ]
            },
        ),
        ({"id": ["0", "1", "2"]}, ["id"], {}),
        (
            {"id": [np.nan, "9", "9"], "city": [np.nan, "Talca", "Talca"]},
            ["id", "city"],
            {
                Level.ERROR: [
                    (
                        "2 duplicate(s) with same id, city",
                        None,
                        {"same '9' `id`, 'Talca' `city`": [1, 2]},
                    )
                ]
            },
        ),
    ],
)
def test_find_by(data, columns, expected_messages):
    df = pd.DataFrame(data)
    assert_results_equal(
        duplicates.find_by(df, columns),
        create_result("Duplicates", expected_messages, items_count=len(df)),
    )


@pytest.mark.parametrize(
    "data, tagged_fields, expected_messages",
    [
        ({}, {}, {Level.INFO: [(Outcome.SKIPPED,)]}),
        (
            {"name": ["bob", "bob", "bob", "bob"], "url": ["u1", "u1", "2", "u1"]},
            {"name_field": ["name"], "product_url_field": ["url"]},
            {
                Level.ERROR: [
                    (
                        "3 duplicate(s) with same name, url",
                        None,
                        {"same 'bob' `name`, 'u1' `url`": [0, 1, 3]},
                    )
                ]
            },
        ),
        (
            {"name": ["john", "bob"], "url": ["url1", "url1"]},
            {"name_field": ["name"], "product_url_field": ["url"]},
            {},
        ),
    ],
)
def test_find_by_name_url(data, tagged_fields, expected_messages):
    df = pd.DataFrame(data)
    assert_results_equal(
        duplicates.find_by_name_url(df, tagged_fields),
        create_result(
            "Duplicates By **name_field, product_url_field** Tags",
            expected_messages,
            items_count=len(df),
        ),
    )
