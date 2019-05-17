from functools import partial

from arche import SH_URL
from arche.rules.others import compare_boolean_fields, garbage_symbols
from arche.rules.result import Level, Outcome
from conftest import create_named_df, create_result
import pandas as pd
import pytest

create_df = partial(create_named_df, name="Coverage for boolean fields")

compare_bool_data = [
    (
        {"b": [True, True]},
        {"b": [False, False]},
        {Level.ERROR: [(("b relative frequencies differ by more than 10%"),)]},
        [
            create_df(
                {True: [1.0, 0.0], False: [0.0, 1.0]}, index=["b_source", "b_target"]
            )
        ],
    ),
    (
        {"bool_f": [True], "bool_f2": [False]},
        {"diff_bool_field": [False]},
        {Level.INFO: [(Outcome.SKIPPED,)]},
        [],
    ),
    (
        {"bool_f": [True]},
        {"str_f": ["True", "True"]},
        {Level.INFO: [(Outcome.SKIPPED,)]},
        [],
    ),
    ({"str_f": ["True"]}, {"bool_f": [True]}, {Level.INFO: [(Outcome.SKIPPED,)]}, []),
    (
        {"b": [True, True, True]},
        {"b": [True]},
        {},
        [
            create_df(
                {True: [1.0, 1.0], False: [0.0, 0.0]}, index=["b_source", "b_target"]
            )
        ],
    ),
    (
        {"b": [True for x in range(9)] + [False]},
        {"b": [True for x in range(9)]},
        {Level.WARNING: [("b relative frequencies differ by 5%-10%",)]},
        [
            create_df(
                {True: [0.9, 1.0], False: [0.1, 0.0]}, index=["b_source", "b_target"]
            )
        ],
    ),
]


@pytest.mark.parametrize(
    "source_data, target_data, expected_messages, expected_stats", compare_bool_data
)
def test_compare_boolean_fields(
    source_data, target_data, expected_messages, expected_stats
):
    source_df = pd.DataFrame(source_data)
    target_df = pd.DataFrame(target_data)
    rule_result = compare_boolean_fields(source_df, target_df)
    assert rule_result == create_result(
        "Boolean Fields", expected_messages, expected_stats
    )


dirty_inputs = [
    (
        [
            {
                "_key": f"{SH_URL}/112358/13/21/item/0",
                "name": " Blacky Robeburned",
                "address": "here goes &AMP",
                "phone": "<h1>144</h1>.sx-prime-pricing-row { float: left; }",
                "rank": 14441,
            },
            {
                "_key": f"{SH_URL}/112358/13/21/item/1",
                "name": "<!--Leprous Jim-->",
                "address": "Some street",
                "phone": "1144",
                "rank": 2_039_857,
            },
        ],
        {
            Level.ERROR: [
                (
                    "100.0% (2) items affected",
                    None,
                    {
                        "100.0% of 'name' values contain [' ', '-->', '<!--']": [
                            f"{SH_URL}/112358/13/21/item/0",
                            f"{SH_URL}/112358/13/21/item/1",
                        ],
                        "50.0% of 'address' values contain ['&AMP']": [
                            f"{SH_URL}/112358/13/21/item/0"
                        ],
                        (
                            "50.0% of 'phone' values contain "
                            "['.sx-prime-pricing-ro', '</h1>', '<h1>']"
                        ): [f"{SH_URL}/112358/13/21/item/0"],
                    },
                )
            ]
        },
        2,
        2,
    ),
    ([{"_key": "{SH_URL}/112358/13/21/item/0"}], {}, 1, 0),
]


@pytest.mark.parametrize(
    "get_job_items, expected_messages, expected_items_count, expected_err_items_count",
    dirty_inputs,
    indirect=["get_job_items"],
)
def test_garbage_symbols(
    get_job_items, expected_messages, expected_items_count, expected_err_items_count
):
    assert garbage_symbols(get_job_items) == create_result(
        "Garbage Symbols",
        expected_messages,
        items_count=expected_items_count,
        err_items_count=expected_err_items_count,
    )
