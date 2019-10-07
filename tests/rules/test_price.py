import arche.rules.price as p
from arche.rules.result import Level, Outcome
from conftest import *
import numpy as np
import pandas as pd
import pytest


was_now_inputs = [
    (
        {
            "original_price": [10, 15, 40, None, 30, None, "60", "56.6", "30.2"],
            "sale_price": [20, 15, 30, 30, None, None, "30", "30", "56.6"],
        },
        {
            "product_price_field": ["sale_price"],
            "product_price_was_field": ["original_price"],
        },
        {
            Level.ERROR: [
                (
                    "22.22% (2) of items with original_price < sale_price",
                    None,
                    {"Past price is less than current for 2 items": {0, 8}},
                )
            ],
            Level.WARNING: [
                (
                    "11.11% (1) of items with original_price = sale_price",
                    None,
                    {"Prices equal for 1 items": {1}},
                )
            ],
        },
    ),
    ({}, {"product_price_field": ["name"]}, {Level.INFO: [(Outcome.SKIPPED,)]}),
    (
        {
            "original_price": [10, 15, 40, None, 30, None, "60", "56.6", "30.2"],
            "sale_price": [9, 14, 30, 30, None, None, "30", "30", "20.6"],
        },
        {
            "product_price_field": ["sale_price"],
            "product_price_was_field": ["original_price"],
        },
        {},
    ),
]


@pytest.mark.parametrize("data, tagged_fields, expected_messages", was_now_inputs)
def test_compare_was_now(data, tagged_fields, expected_messages):
    df = pd.DataFrame(data)
    assert_results_equal(
        p.compare_was_now(df, tagged_fields),
        create_result(
            "Compare Price Was And Now", expected_messages, items_count=len(df)
        ),
    )


compare_prices_inputs = [
    (
        {"price": ["2", 1, 5], "url": ["http://2", "http://1", np.nan]},
        {"price": [1.15, "2.3", 6], "url": ["http://1", "http://2", np.nan]},
        {"product_price_field": ["price"], "product_url_field": ["url"]},
        {
            Level.ERROR: [
                (
                    "2 checked, 2 errors",
                    (
                        "different prices for url: http://2\nsource price is 2 for 0\n"
                        "target price is 2.3 for 1\n"
                        "different prices for url: http://1\nsource price is 1 for 1\n"
                        "target price is 1.15 for 0"
                    ),
                )
            ]
        },
    )
]


@pytest.mark.parametrize(
    "source_data, target_data, tagged_fields, expected_messages", compare_prices_inputs
)
def test_compare_prices_for_same_urls(
    source_data, target_data, tagged_fields, expected_messages
):
    result = p.compare_prices_for_same_urls(
        pd.DataFrame(source_data), pd.DataFrame(target_data), tagged_fields
    )
    assert_results_equal(
        result, create_result("Compare Prices For Same Urls", expected_messages)
    )


compare_names_inputs = [
    (
        {"name": ["John", "Carl"], "url": ["http://1", "http://2"]},
        {"name": ["Ted", "Johnny"], "url": ["http://2", "http://1"]},
        {"name_field": ["name"], "product_url_field": ["url"]},
        {
            Level.ERROR: [
                (
                    "2 checked, 2 errors",
                    (
                        "different names for url: http://1\nsource name is John for 0\n"
                        "target name is Johnny for 1\n"
                        "different names for url: http://2\nsource name is Carl for 1\n"
                        "target name is Ted for 0"
                    ),
                )
            ]
        },
    )
]


@pytest.mark.parametrize(
    "source_data, target_data, tagged_fields, expected_messages", compare_names_inputs
)
def test_compare_names_for_same_urls(
    source_data, target_data, tagged_fields, expected_messages
):
    result = p.compare_names_for_same_urls(
        pd.DataFrame(source_data), pd.DataFrame(target_data), tagged_fields
    )
    assert_results_equal(
        result, create_result("Compare Names Per Url", expected_messages)
    )


@pytest.mark.parametrize(
    "source_data, target_data, tagged_fields, expected_messages",
    [
        (
            {"name": ["Coffee", "Tea", "Juice"], "price": [3.0, 5.0, 2.0]},
            {"name": ["Coffee", "Tea", "Wine"], "price": [4.0, 4.8, 20.0]},
            {"name_field": ["name"], "product_price_field": ["price"]},
            {
                Level.ERROR: [
                    (
                        "2 checked, 1 errors",
                        (
                            "different price for Coffee\nsource price is 3.0 for 0\n"
                            "target price is 4.0 for 0"
                        ),
                    )
                ]
            },
        )
    ],
)
def test_compare_prices_for_same_names(
    source_data, target_data, tagged_fields, expected_messages
):
    result = p.compare_prices_for_same_names(
        pd.DataFrame(source_data), pd.DataFrame(target_data), tagged_fields
    )
    assert_results_equal(
        result, create_result("Compare Prices For Same Names", expected_messages)
    )
