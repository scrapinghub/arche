from arche.rules.result import Level, Message, Result, Outcome
from conftest import create_named_df, create_result, get_report_from_iframe
import pandas as pd
import pytest


@pytest.mark.parametrize(
    "source, target",
    [(("summary", "details", {"err": ["1"]}), ("summary", "details", {"err": ["1"]}))],
)
def test_message_eq(source, target):
    assert Message(*source) == Message(*target)


@pytest.mark.parametrize(
    "source, target",
    [
        (
            ("summary", "details", {"err": ["0"]}),
            ("summary", "details", {"err": ["1"]}),
        ),
        (("summary", "details"), ("summary", "other")),
    ],
)
def test_message_not_eq(source, target):
    assert Message(*source) != Message(*target)


@pytest.mark.parametrize(
    "errors, true_err_keys",
    [
        ({"a": {1, 2, 3}, "b": {2, 3, 4}}, {1, 2, 3, 4}),
        ({"a": {"2"}, "b": {"3"}}, {"2", "3"}),
        (None, set()),
    ],
)
def test_message_err_keys(errors, true_err_keys):
    assert Message("x", errors=errors).err_keys == true_err_keys


@pytest.mark.parametrize(
    "messages, true_err_keys",
    [
        (
            {
                Level.ERROR: [
                    Message("x", errors={"a": {1, 2, 3}, "b": {2, 3}}),
                    Message("x", errors={"a": {3, 5}, "b": {3}}),
                ]
            },
            {1, 2, 3, 5},
        ),
        (dict(), set()),
    ],
)
def test_result_err_keys(messages, true_err_keys):
    assert Result("x", messages=messages).err_keys == true_err_keys


@pytest.mark.parametrize(
    "message, stats",
    [
        (
            {Level.INFO: [("summary", "very detailed message")]},
            [pd.Series([1, 2], name="Fields coverage")],
        ),
        (
            {Level.INFO: [("summary",)]},
            [
                create_named_df(
                    {"s": [0.25]}, index=["us"], name="Coverage for boolean fields"
                )
            ],
        ),
    ],
)
def test_show(mocker, capsys, message, stats):
    mocked_display = mocker.patch("arche.report.display_html", autospec=True)
    res = create_result("test show", message, stats=stats)
    res.show()
    report_html = get_report_from_iframe(mocked_display.mock_calls[0][1][0])
    assert report_html.count("Plotly.newPlot") == 1


def test_outcome_equality():
    assert Outcome.SKIPPED == Outcome.SKIPPED
    assert Outcome.WARNING != Outcome.PASSED
