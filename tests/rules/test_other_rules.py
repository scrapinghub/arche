from arche.rules.other_rules import compare_boolean_fields
from arche.rules.result import Level
from conftest import create_result
import pandas as pd
import pytest


compare_bool_data = [
    (
        {"boolean_field": [True, True]},
        {"boolean_field": [False, False]},
        {
            Level.ERROR: [
                (
                    "['boolean_field'] relative frequencies differ by more than 10%",
                    pd.DataFrame(
                        {False: [100.0], True: [100.0]}, index=["boolean_field"]
                    ).to_string(),
                )
            ]
        },
    ),
    (
        {"bool_f": [True], "bool_f2": [False]},
        {"diff_bool_field": [False]},
        {Level.INFO: [("No fields to compare",)]},
    ),
    (
        {"bool_f": [True]},
        {"str_f": ["True", "True"]},
        {Level.INFO: [("No fields to compare",)]},
    ),
    (
        {"str_f": ["True"]},
        {"bool_f": [True]},
        {Level.INFO: [("No fields to compare",)]},
    ),
    (
        {"boolean_field": [True, True, True]},
        {"boolean_field": [True]},
        {
            Level.INFO: [
                (
                    "['boolean_field'] relative frequencies are equal "
                    "or differ by less than 5%",
                    (
                        pd.DataFrame(
                            {True: [0.0], False: [0.0]}, index=["boolean_field"]
                        ).to_string(
                            header=["Difference in False, %", "Difference in True, %"]
                        )
                    ),
                )
            ]
        },
    ),
    (
        {"b1": [True, True, True, True, True, True, True, True, True, False]},
        {"b1": [True, True, True, True, True, True, True, True, True]},
        {
            Level.WARNING: [
                (
                    "['b1'] relative frequencies differ by 5-10%",
                    pd.DataFrame(
                        {True: [10.0], False: [10.0]}, index=["b1"]
                    ).to_string(),
                )
            ]
        },
    ),
]


@pytest.mark.parametrize(
    "source_data, target_data, expected_messages", compare_bool_data
)
def test_compare_boolean_fields(source_data, target_data, expected_messages):
    source_df = pd.DataFrame(source_data)
    target_df = pd.DataFrame(target_data)
    rule_result = compare_boolean_fields(source_df, target_df)
    assert rule_result == create_result("Boolean Fields", expected_messages)
