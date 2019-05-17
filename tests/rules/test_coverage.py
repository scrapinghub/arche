import arche.rules.coverage as cov
from arche.rules.result import Level
from conftest import create_result, create_named_df, Job
import pandas as pd
import pytest


@pytest.mark.parametrize(
    "df, expected_messages, expected_stats",
    [
        (
            pd.DataFrame({"_key": [n for n in range(1000)]}),
            {},
            [pd.Series([1000], index=["_key"], name="Fields coverage for 1_000 items")],
        ),
        (
            pd.DataFrame([("Jordan", None)], columns=["Name", "Field"]),
            {Level.ERROR: [("1 empty field(s)",)]},
            [
                pd.Series(
                    [1, 0], index=["Name", "Field"], name="Fields coverage for 1 items"
                )
            ],
        ),
        (
            pd.DataFrame([(0, "")], columns=["Name", "Field"]),
            {},
            [
                pd.Series(
                    [1, 1], index=["Field", "Name"], name="Fields coverage for 1 items"
                )
            ],
        ),
    ],
)
def test_check_fields_coverage(df, expected_messages, expected_stats):
    result = cov.check_fields_coverage(df)
    assert result == create_result("Fields Coverage", expected_messages, expected_stats)


@pytest.mark.parametrize(
    "source_stats, target_stats, expected_messages, expected_stats",
    [
        (
            {"counts": {"f1": 100, "f2": 150}, "totals": {"input_values": 100}},
            {"counts": {"f2": 100, "f3": 150}, "totals": {"input_values": 100}},
            {Level.ERROR: [("The difference is greater than 10% for 3 field(s)",)]},
            [
                create_named_df(
                    {"s": [0.0, 1.0, 1.5], "t": [1.5, 0.0, 1.0]},
                    index=["f3", "f1", "f2"],
                    name="Coverage from job stats fields counts",
                ),
                pd.Series(
                    [0.5, 1.0, 1.5],
                    index=["f2", "f1", "f3"],
                    name="Coverage difference more than 5%",
                ),
            ],
        ),
        (
            {"counts": {"f1": 100, "f2": 150}, "totals": {"input_values": 100}},
            {"counts": {"f1": 106, "f2": 289}, "totals": {"input_values": 200}},
            {
                Level.ERROR: [("The difference is greater than 10% for 1 field(s)",)],
                Level.WARNING: [
                    ("The difference is between 5% and 10% for 1 field(s)",)
                ],
            },
            [
                create_named_df(
                    {"s": [1.0, 1.5], "t": [0.53, 1.445]},
                    index=["f1", "f2"],
                    name="Coverage from job stats fields counts",
                ),
                pd.Series(
                    [0.055, 0.47],
                    index=["f2", "f1"],
                    name="Coverage difference more than 5%",
                ),
            ],
        ),
        (
            {"counts": {"f1": 100, "f2": 150}, "totals": {"input_values": 100}},
            {"counts": {"f1": 94, "f2": 141}, "totals": {"input_values": 100}},
            {Level.WARNING: [("The difference is between 5% and 10% for 2 field(s)",)]},
            [
                create_named_df(
                    {"s": [1.0, 1.5], "t": [0.94, 1.41]},
                    index=["f1", "f2"],
                    name="Coverage from job stats fields counts",
                ),
                pd.Series(
                    [0.06, 0.09],
                    index=["f1", "f2"],
                    name="Coverage difference more than 5%",
                ),
            ],
        ),
        (
            {"counts": {"state": 100}, "totals": {"input_values": 100}},
            {"counts": {"state": 100}, "totals": {"input_values": 100}},
            {},
            [
                create_named_df(
                    {"s": [1.0], "t": [1.0]},
                    index=["state"],
                    name="Coverage from job stats fields counts",
                )
            ],
        ),
    ],
)
def test_get_difference(source_stats, target_stats, expected_messages, expected_stats):
    result = cov.get_difference(
        Job(stats=source_stats, key="s"), Job(stats=target_stats, key="t")
    )
    assert result == create_result(
        "Coverage Difference", expected_messages, stats=expected_stats
    )


@pytest.mark.parametrize(
    "source_cols, target_cols, expected_messages",
    [
        (["range", "name"], ["name"], {Level.INFO: [("New - range",)]}),
        (["name"], ["name", "sex"], {Level.ERROR: [("Missing - sex",)]}),
    ],
)
def test_compare_scraped_fields(source_cols, target_cols, expected_messages):
    result = cov.compare_scraped_fields(
        pd.DataFrame([], columns=source_cols), pd.DataFrame([], columns=target_cols)
    )
    assert result == create_result("Scraped Fields", expected_messages)
