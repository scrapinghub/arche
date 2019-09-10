from arche import SH_URL
from arche.report import Report
from arche.rules.result import Level
from conftest import create_result
import pandas as pd
import pytest


@pytest.mark.parametrize(
    "messages, expected_details",
    [
        (
            [
                (
                    "rule name here",
                    {Level.INFO: [("summary", "very detailed message")]},
                ),
                (
                    "other result there",
                    {Level.INFO: [("summary", "other detailed message")]},
                ),
            ],
            [
                "<h2>Details</h2>",
                "rule name here (1 message(s)):",
                "very detailed message",
                "<br>",
                "other result there (1 message(s)):",
                "other detailed message",
                "<br>",
                "<h2>Plots</h2>",
            ],
        ),
        (
            [("everything is fine", {Level.INFO: [("summary",)]})],
            ["<h2>Details</h2>", "<h2>Plots</h2>"],
        ),
    ],
)
def test_write_details(mocker, get_df, capsys, messages, expected_details):
    mock_pio_show = mocker.patch("plotly.io.show", autospec=True)
    md_mock = mocker.patch("arche.report.display_markdown", autospec=True)

    r = Report()
    for m in messages:
        result = create_result(*m, stats=[get_df])
        r.save(result)
    r.write_details()
    mock_pio_show.assert_called_with(result.figures[0])
    calls = [mocker.call(e) for e in expected_details]
    md_mock.assert_has_calls(calls, any_order=True)


@pytest.mark.parametrize(
    "message, expected_details",
    [({Level.INFO: [("summary", "very detailed message")]}, "very detailed message")],
)
def test_write_rule_details(capsys, message, expected_details):
    outcome = create_result("rule name here", message)
    Report.write_rule_details(outcome)
    assert capsys.readouterr().out == f"{{'text/markdown': '{expected_details}'}}\n"


def test_write_none_rule_details(capsys):
    outcome = create_result("rule name here", {Level.INFO: [("summary",)]})
    Report.write_rule_details(outcome)
    assert not capsys.readouterr().out


@pytest.mark.parametrize(
    "errors, short, keys_limit, expected_messages",
    [
        (
            {"something happened": pd.Series(["1", "2"])},
            False,
            10,
            ["2 items affected - something happened: 1, 2"],
        ),
        (
            {"something went bad": [i for i in range(10)]},
            False,
            1,
            ["10 items affected - something went bad: 5"],
        ),
        (
            {
                "err1": [i for i in range(15)],
                "err2": [i for i in range(15)],
                "err3": [i for i in range(15)],
                "err4": [i for i in range(15)],
                "err5": [i for i in range(15)],
                "err6": [i for i in range(15)],
            },
            False,
            1,
            [f"15 items affected - err{i}: 5" for i in range(1, 7)],
        ),
        (
            {
                "err1": ["1"],
                "err2": ["2"],
                "err3": ["3", "4"],
                "err4": ["5", "6", "7"],
                "err5": ["7", "8"],
            },
            True,
            1,
            [
                "3 items affected - err4: 5, 6, 7",
                "2 items affected - err3: 3, 4",
                "2 items affected - err5: 7, 8",
                "1 items affected - err1: 1",
                "1 items affected - err2: 2",
            ],
        ),
        (
            {"something happened": set(["https://app.scrapinghub.com/p/1/1/1/item/0"])},
            False,
            10,
            [f"1 items affected - something happened: [0]({SH_URL}/1/1/1/item/0)"],
        ),
        ({"html '</br>'": [1]}, False, 10, ["1 items affected - html '</br>': 1"]),
    ],
)
def test_write_detailed_errors(mocker, errors, short, keys_limit, expected_messages):
    mocker.patch("pandas.Series.sample", return_value=pd.Series("5"), autospec=True)
    md_mock = mocker.patch("arche.report.display_markdown", autospec=True)
    Report.write_detailed_errors(errors, short, keys_limit)
    md_mock.assert_has_calls(mocker.call(m) for m in expected_messages)


@pytest.mark.parametrize(
    "keys, limit, sample_mock, expected_sample",
    [
        (
            pd.Series(f"{SH_URL}/112358/13/21/item/0"),
            5,
            pd.Series(f"{SH_URL}/112358/13/21/item/5"),
            f"[0]({SH_URL}/112358/13/21/item/0)",
        ),
        (
            pd.Series([f"{SH_URL}/112358/13/21/item/{i}" for i in range(20)]),
            10,
            pd.Series(f"{SH_URL}/112358/13/21/item/5"),
            f"[5]({SH_URL}/112358/13/21/item/5)",
        ),
        (
            pd.Series("112358/13/21/0"),
            1,
            pd.Series("112358/13/21/0"),
            f"[0]({SH_URL}/112358/13/21/item/0)",
        ),
        (pd.Series([0, 1]), 1, pd.Series([0, 1]), "0, 1"),
    ],
)
def test_sample_keys(mocker, keys, limit, sample_mock, expected_sample):
    mocker.patch("pandas.Series.sample", return_value=sample_mock, autospec=True)
    assert Report.sample_keys(keys, limit) == expected_sample


def test_save():
    r = Report()
    dummy_result = create_result("dummy", {Level.INFO: [("outcome",)]})
    r.save(dummy_result)
    assert r.results == {dummy_result.name: dummy_result}
