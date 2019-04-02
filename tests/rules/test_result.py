from arche.rules.result import Level, Message
from conftest import create_result
import pandas as pd
import pytest


@pytest.mark.parametrize(
    "source, target",
    [
        (
            ("summary", "details", {"err": ["1"]}, None),
            ("summary", "details", {"err": ["1"]}, None),
        )
    ],
)
def test_message_eq(source, target):
    assert Message(*source) == Message(*target)


@pytest.mark.parametrize(
    "source, target",
    [
        (
            ("summary", "details", {"err": ["0"]}, pd.Series(["row1", "row2"])),
            ("summary", "details", {"err": ["0"]}),
        ),
        (
            ("summary", "details", {"err": ["0"]}),
            ("summary", "details", {"err": ["0"]}, pd.Series(["row1", "row2"])),
        ),
        (
            ("summary", "details", {"err": ["0"]}),
            ("summary", "details", {"err": ["1"]}),
        ),
        (("summary", "details"), ("summary", "other")),
        (
            (
                "summary",
                "details",
                {"err": ["0"]},
                pd.Series(["row1", "row2"], index=[1, 2]),
            ),
            ("summary", "details", {"err": ["0"]}, pd.Series(["row1", "row2"])),
        ),
    ],
)
def test_message_not_eq(source, target):
    assert Message(*source) != Message(*target)


@pytest.mark.parametrize(
    "message, expected_details",
    [
        (
            {Level.INFO: [("summary", "very detailed message")]},
            "\nrule name here:\n\tsummary\nvery detailed message\n",
        ),
        ({Level.INFO: [("summary",)]}, "\nrule name here:\n\tsummary\n"),
    ],
)
def test_show(capsys, message, expected_details):
    r = create_result("rule name here", message)
    r.show()
    assert capsys.readouterr().out == expected_details
