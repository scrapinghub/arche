import arche.rules.category as c
from arche.rules.result import Level
from conftest import create_result, create_named_df
import numpy as np
import pandas as pd
import pytest


@pytest.mark.parametrize(
    "data, cat_names, expected_messages, expected_stats",
    [
        (
            {"sex": ["male", "female", "male"], "country": ["uk", "uk", "uk"]},
            ["sex", "country"],
            {Level.INFO: [("2 categories in 'sex'",), ("1 categories in 'country'",)]},
            [
                pd.Series({"female": 1, "male": 2}, name="sex"),
                pd.Series({"uk": 3}, name="country"),
            ],
        )
    ],
)
def test_get_coverage_per_category(data, cat_names, expected_messages, expected_stats):
    assert c.get_coverage_per_category(pd.DataFrame(data), cat_names) == create_result(
        "Coverage For Scraped Categories", expected_messages, expected_stats
    )


@pytest.mark.parametrize(
    "source, target, categories, expected_messages, expected_stats",
    [
        (
            {
                "sex": ["male", "female", "male", np.nan],
                "country": ["uk", "uk", "uk", "us"],
                "age": [26, 26, 26, 26],
            },
            {
                "sex": ["male", "female", "male"],
                "country": ["uk", "uk", "uk"],
                "age": [26, 26, 26],
            },
            ["sex", "country", "age"],
            {
                Level.WARNING: [
                    ("The difference is greater than 20% for 1 value(s) of sex",),
                    ("The difference is greater than 20% for 2 value(s) of country",),
                ]
            },
            [
                create_named_df(
                    {
                        "source": [0.25, 0.25, 0.5],
                        "target": [0.000000, 0.333333, 0.666667],
                    },
                    index=[np.nan, "female", "male"],
                    name="Coverage for sex",
                ),
                pd.Series(
                    [0.250000, -0.166667],
                    index=[np.nan, "male"],
                    name="Coverage difference more than 10% for sex",
                ),
                create_named_df(
                    {"source": [0.25, 0.75], "target": [0.0, 1.0]},
                    index=["us", "uk"],
                    name="Coverage for country",
                ),
                pd.Series(
                    [0.25, -0.25],
                    index=["us", "uk"],
                    name="Coverage difference more than 10% for country",
                ),
                create_named_df(
                    {"source": [1.0], "target": [1.0]},
                    index=[26],
                    name="Coverage for age",
                ),
            ],
        )
    ],
)
def test_get_difference(source, target, categories, expected_messages, expected_stats):
    assert c.get_difference(
        pd.DataFrame(source), pd.DataFrame(target), categories
    ) == create_result(
        "Category Coverage Difference", expected_messages, stats=expected_stats
    )


@pytest.mark.parametrize(
    "data, expected_message",
    [
        (np.random.rand(100), "Categories were not found"),
        (None, "Categories were not found"),
    ],
)
def test_get_no_categories(data, expected_message):
    result = c.get_categories(pd.DataFrame(data))
    assert result == create_result("Categories", {Level.INFO: [(expected_message,)]})


@pytest.mark.parametrize(
    "data, max_uniques, expected_stats, expected_message",
    [
        (np.zeros(10), 2, [pd.Series(10, index=[0.0], name=0)], "1 category field(s)"),
        (
            [[np.nan]] * 10,
            2,
            [pd.Series(10, index=[np.nan], name=0)],
            "1 category field(s)",
        ),
        (
            {"a": [True] * 10, "b": [i for i in range(10)]},
            2,
            [pd.Series(10, index=[True], name="a")],
            "1 category field(s)",
        ),
        (
            {"b": [i for i in range(10)], "c": [np.nan] * 10},
            10,
            [
                pd.Series([1] * 10, index=[i for i in range(10)][::-1], name="b"),
                pd.Series(10, index=[np.nan], name="c"),
            ],
            "2 category field(s)",
        ),
        ({"a": range(100)}, 1, [], "Categories were not found"),
    ],
)
def test_get_categories(data, max_uniques, expected_stats, expected_message):
    result = c.get_categories(pd.DataFrame(data), max_uniques)
    assert result == create_result(
        "Categories", {Level.INFO: [(expected_message,)]}, stats=expected_stats
    )


@pytest.mark.parametrize(
    "data, max_uniques, sample, expected_cats",
    [
        ({"a": np.zeros(100)}, 1, 100, ["a"]),
        ({"a": np.zeros(200)}, 1, 100, ["a"]),
        (
            {"a": np.concatenate([np.zeros(199), [np.nan]]), "b": list(range(200))},
            2,
            100,
            ["a"],
        ),
        (
            {
                "a": np.repeat({"k": "v"}, 10_000),
                "b": pd.Series([[{"x": 0}]]).repeat(10_000),
            },
            10,
            5000,
            ["a", "b"],
        ),
        ({"a": range(100)}, 1, 10, []),
    ],
)
def test_find_likely_cats(data, max_uniques, sample, expected_cats):
    assert c.find_likely_cats(pd.DataFrame(data), max_uniques, sample) == expected_cats
