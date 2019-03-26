import arche.rules.duplicates as duplicates
from arche.rules.result import Level
from conftest import create_result
import numpy as np
import pandas as pd
import pytest


check_items_inputs = [
    (
        {},
        {},
        {
            Level.INFO: [
                ("'name_field' and 'product_url_field' tags were not found in schema",)
            ]
        },
        0,
    ),
    (
        {
            "_key": ["0", "1", "2", "3"],
            "name": ["bob", "bob", "bob", "bob"],
            "url": ["u1", "u1", "2", "u1"],
        },
        {"name_field": ["name"], "product_url_field": ["url"]},
        {
            Level.ERROR: [
                (
                    "3 duplicate(s) with same name and url",
                    None,
                    {"same 'bob' name and 'u1' url": ["0", "1", "3"]},
                )
            ]
        },
        3,
    ),
    (
        {"_key": ["0", "1"], "name": ["john", "bob"], "url": ["url1", "url1"]},
        {"name_field": ["name"], "product_url_field": ["url"]},
        {},
        0,
    ),
]


@pytest.mark.parametrize(
    "data, tagged_fields, expected_messages, expected_err_items_count",
    check_items_inputs,
)
def test_check_items(data, tagged_fields, expected_messages, expected_err_items_count):
    df = pd.DataFrame(data)
    result = duplicates.check_items(df, tagged_fields)
    assert result == create_result(
        "Duplicated Items",
        expected_messages,
        items_count=len(df),
        err_items_count=expected_err_items_count,
    )


check_uniqueness_inputs = [
    ({}, {}, {Level.INFO: [("'unique' tag was not found in schema",)]}, 0),
    (
        {"_key": ["0", "1", "2"], "id": ["0", "0", "1"]},
        {"unique": ["id"]},
        {
            Level.ERROR: [
                (
                    "'id' contains 1 duplicated value(s)",
                    None,
                    {"same '0' id": ["0", "1"]},
                )
            ]
        },
        2,
    ),
    (
        {
            "_key": [str(i) for i in range(6)],
            "id": ["47" for x in range(6)],
            "name": ["Walt", "Juan", "Juan", "Walt", "Walt", "John"],
        },
        {"unique": ["id", "name"]},
        {
            Level.ERROR: [
                (
                    "'id' contains 1 duplicated value(s)",
                    None,
                    {"same '47' id": [str(i) for i in range(6)]},
                ),
                (
                    "'name' contains 2 duplicated value(s)",
                    None,
                    {
                        "same 'Juan' name": ["1", "2"],
                        "same 'Walt' name": ["0", "3", "4"],
                    },
                ),
            ]
        },
        6,
    ),
    ({"_key": ["0", "1"], "name": ["a", "b"]}, {"unique": ["name"]}, {}, 0),
]


@pytest.mark.parametrize(
    "data, tagged_fields, expected_messages, expected_err_items_count",
    check_uniqueness_inputs,
)
def test_check_uniqueness(
    data, tagged_fields, expected_messages, expected_err_items_count
):
    df = pd.DataFrame(data)
    assert duplicates.check_uniqueness(df, tagged_fields) == create_result(
        "Uniqueness",
        expected_messages,
        items_count=len(df),
        err_items_count=expected_err_items_count,
    )


find_by_inputs = [
    (
        {"_key": ["0", "1", "2"], "id": ["0", "0", "1"]},
        ["id"],
        {
            Level.ERROR: [
                ("2 duplicate(s) with same id", None, {"same '0' id": ["0", "1"]})
            ]
        },
        2,
    ),
    ({"_key": ["0", "1", "2"], "id": ["0", "1", "2"]}, ["id"], {}, 0),
    (
        {
            "_key": ["0", "1", "2"],
            "id": [np.nan, "9", "9"],
            "city": [np.nan, "Talca", "Talca"],
        },
        ["id", "city"],
        {
            Level.ERROR: [
                (
                    "2 duplicate(s) with same id, city",
                    None,
                    {"same '9' id 'Talca' city": ["1", "2"]},
                )
            ]
        },
        2,
    ),
]


@pytest.mark.parametrize(
    "data, columns, expected_messages, expected_err_items_count", find_by_inputs
)
def test_find_by(data, columns, expected_messages, expected_err_items_count):
    df = pd.DataFrame(data)
    assert duplicates.find_by(df, columns) == create_result(
        "Items Uniqueness By Columns",
        expected_messages,
        items_count=len(df),
        err_items_count=expected_err_items_count,
    )
