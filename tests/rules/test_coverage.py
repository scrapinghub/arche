from arche.rules.coverage import check_fields_coverage, compare_fields_counts
from arche.rules.result import Level
from conftest import create_result, Job
import pandas as pd
import pytest


@pytest.mark.parametrize(
    "df, expected_messages",
    [
        (
            pd.DataFrame([{"_key": 0}]),
            {
                Level.INFO: [
                    (
                        "PASSED",
                        None,
                        None,
                        pd.Series([1], index=["_key"], name="Fields Coverage"),
                    )
                ]
            },
        ),
        (
            pd.DataFrame([("Jordan", None)], columns=["Name", "Field"]),
            {
                Level.ERROR: [
                    (
                        "1 empty field(s)",
                        None,
                        None,
                        pd.Series(
                            [1, 0], index=["Name", "Field"], name="Fields Coverage"
                        ),
                    )
                ]
            },
        ),
        (
            pd.DataFrame([(0, "")], columns=["Name", "Field"]),
            {
                Level.INFO: [
                    (
                        "PASSED",
                        None,
                        None,
                        pd.Series(
                            [1, 1], index=["Field", "Name"], name="Fields Coverage"
                        ),
                    )
                ]
            },
        ),
    ],
)
def test_check_fields_coverage(df, expected_messages):
    result = check_fields_coverage(df)
    assert result == create_result("Fields Coverage", expected_messages)


@pytest.mark.parametrize(
    "source_stats, target_stats, expected_messages",
    [
        (
            {"counts": {"f1": 100, "f2": 150}, "totals": {"input_values": 100}},
            {"counts": {"f2": 100, "f3": 150}, "totals": {"input_values": 100}},
            {
                Level.ERROR: [
                    (
                        "Coverage difference is greater than 10% for 3 field(s)",
                        (
                            pd.DataFrame([100, 33, 100], columns=["Difference, %"])
                            .rename(index={0: "f1", 1: "f2", 2: "f3"})
                            .to_string()
                        ),
                    )
                ]
            },
        ),
        (
            {"counts": {"f1": 100, "f2": 150}, "totals": {"input_values": 100}},
            {"counts": {"f1": 106, "f2": 200}, "totals": {"input_values": 100}},
            {
                Level.ERROR: [
                    (
                        ("Coverage difference is greater than 10% for 1 field(s)"),
                        (
                            pd.DataFrame([25], columns=["Difference, %"])
                            .rename(index={0: "f2"})
                            .to_string()
                        ),
                    )
                ],
                Level.WARNING: [
                    (
                        ("Coverage difference is between 5% and 10% for 1 field(s)"),
                        (
                            pd.DataFrame([6], columns=["Difference, %"])
                            .rename(index={0: "f1"})
                            .to_string()
                        ),
                    )
                ],
            },
        ),
        (
            {"counts": {"state": 100}, "totals": {"input_values": 100}},
            {"counts": {"state": 100}, "totals": {"input_values": 100}},
            {},
        ),
    ],
)
def test_compare_fields_counts(source_stats, target_stats, expected_messages):
    result = compare_fields_counts(Job(stats=source_stats), Job(stats=target_stats))
    assert result == create_result("Fields Counts", expected_messages)
