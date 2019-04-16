import arche.rules.price as p
from arche.rules.result import Level
from conftest import create_result
import numpy as np
import pandas as pd
import pytest


was_now_inputs = [
    (
        {
            "_key": [str(i) for i in range(9)],
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
                    "Past price is less than current for 2 items:\n['0', '8']",
                )
            ],
            Level.WARNING: [
                (
                    "11.11% (1) of items with original_price = sale_price",
                    "Prices equal for 1 items:\n['1']",
                )
            ],
        },
        3,
    ),
    (
        {},
        {"product_price_field": ["_key"]},
        {
            Level.INFO: [
                (
                    (
                        "product_price_field or product_price_was_field tags were not found "
                        "in schema"
                    ),
                )
            ]
        },
        0,
    ),
    (
        {
            "_key": [str(i) for i in range(9)],
            "original_price": [10, 15, 40, None, 30, None, "60", "56.6", "30.2"],
            "sale_price": [9, 14, 30, 30, None, None, "30", "30", "20.6"],
        },
        {
            "product_price_field": ["sale_price"],
            "product_price_was_field": ["original_price"],
        },
        {},
        0,
    ),
]


@pytest.mark.parametrize(
    "data, tagged_fields, expected_messages, expected_err_items_count", was_now_inputs
)
def test_compare_was_now(
    data, tagged_fields, expected_messages, expected_err_items_count
):
    df = pd.DataFrame(data)
    result = p.compare_was_now(df, tagged_fields)
    assert result == create_result(
        "Compare Price Was And Now",
        expected_messages,
        err_items_count=expected_err_items_count,
        items_count=len(df),
    )


compare_prices_inputs = [
    (
        {
            "_key": ["1", "0", "2"],
            "price": [1, "2", 5],
            "url": ["http://1", "http://2", np.nan],
        },
        {
            "_key": ["0", "1", "2"],
            "price": [1.15, "2.3", 6],
            "url": ["http://1", "http://2", np.nan],
        },
        {"product_price_field": ["price"], "product_url_field": ["url"]},
        {
            Level.INFO: [
                ("0 urls missing from the tested job", ""),
                ("0 new urls in the tested job",),
                ("2 same urls in both jobs",),
            ],
            Level.ERROR: [
                (
                    "2 checked, 2 errors",
                    (
                        "different prices for url: http://1\nsource price is 1 for 1\n"
                        "target price is 1.15 for 0\n"
                        "different prices for url: http://2\nsource price is 2 for 0\n"
                        "target price is 2.3 for 1"
                    ),
                )
            ],
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
    assert result == create_result("Compare Prices For Same Urls", expected_messages)


compare_names_inputs = [
    (
        {"_key": ["1", "0"], "name": ["John", "Carl"], "url": ["http://1", "http://2"]},
        {
            "_key": ["0", "1"],
            "name": ["Johnny", "Ted"],
            "url": ["http://1", "http://2"],
        },
        {"name_field": ["name"], "product_url_field": ["url"]},
        {
            Level.ERROR: [
                (
                    "2 checked, 2 errors",
                    (
                        "different names for url: http://1\nsource name is John for 1\n"
                        "target name is Johnny for 0\n"
                        "different names for url: http://2\nsource name is Carl for 0\n"
                        "target name is Ted for 1"
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
    assert result == create_result("Compare Names Per Url", expected_messages)
