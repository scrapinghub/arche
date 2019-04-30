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
                    {"s": [0.25, 0.25, 0.5], "t": [0.000000, 0.333333, 0.666667]},
                    index=[np.nan, "female", "male"],
                    name="Coverage for sex",
                ),
                pd.Series(
                    [0.250000, 0.166667],
                    index=[np.nan, "male"],
                    name="Coverage difference more than 10% for sex",
                ),
                create_named_df(
                    {"s": [0.25, 0.75], "t": [0.0, 1.0]},
                    index=["us", "uk"],
                    name="Coverage for country",
                ),
                pd.Series(
                    [0.25, 0.25],
                    index=["us", "uk"],
                    name="Coverage difference more than 10% for country",
                ),
                create_named_df(
                    {"s": [1.0], "t": [1.0]}, index=[26], name="Coverage for age"
                ),
            ],
        )
    ],
)
def test_get_difference(source, target, categories, expected_messages, expected_stats):
    assert c.get_difference(
        pd.DataFrame(source), pd.DataFrame(target), categories, "s", "t"
    ) == create_result(
        "Category Coverage Difference", expected_messages, stats=expected_stats
    )
