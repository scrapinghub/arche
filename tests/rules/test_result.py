from arche.rules.result import Message
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
    ],
)
def test_message_not_eq(source, target):
    assert Message(*source) != Message(*target)
