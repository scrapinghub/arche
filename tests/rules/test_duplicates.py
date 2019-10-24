import arche.rules.duplicates as duplicates
from arche.rules.result import Level
from conftest import *
import numpy as np
import pandas as pd
import pytest


@pytest.mark.parametrize(
    "data, columns, expected_messages",
    [
        ({"id": ["0", "1", "2"]}, ["id"], {}),
        (
            {"id": ["0", "0", "1"]},
            ["id"],
            {
                Level.ERROR: [
                    (
                        "id contains 1 duplicated value(s)",
                        None,
                        {"same '0' `id`": [0, 1]},
                    )
                ]
            },
        ),
        (
            {
                "id": [np.nan, "9", "9"],
                "city": [np.nan, "Talca", "Talca"],
                "id2": ["47" for x in range(3)],
                "name": ["Walt", "Juan", "Juan"],
            },
            [["id", "city"], "id2", "name"],
            {
                Level.ERROR: [
                    (
                        "id, city contains 1 duplicated value(s)",
                        None,
                        {"same '9' `id`, 'Talca' `city`": [1, 2]},
                    ),
                    (
                        "id2 contains 1 duplicated value(s)",
                        None,
                        {"same '47' `id2`": [0, 1, 2]},
                    ),
                    (
                        "name contains 1 duplicated value(s)",
                        None,
                        {"same 'Juan' `name`": [1, 2]},
                    ),
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
        (
            {
                "name": ["bob", "bob", "bob", "bob"],
                "url": ["u1", "u1", "2", "u1"],
                "id": [np.nan, "9", "9", None],
            },
            {"name_field": ["name"], "product_url_field": ["url"], "unique": ["id"]},
            {
                Level.ERROR: [
                    (
                        "id contains 1 duplicated value(s)",
                        None,
                        {"same '9' `id`": [1, 2]},
                    ),
                    (
                        "name, url contains 1 duplicated value(s)",
                        None,
                        {"same 'bob' `name`, 'u1' `url`": [0, 1, 3]},
                    ),
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
def test_find_by_tags(data, tagged_fields, expected_messages):
    df = pd.DataFrame(data)
    assert_results_equal(
        duplicates.find_by_tags(df, tagged_fields),
        create_result("Duplicates", expected_messages, items_count=len(df)),
    )
