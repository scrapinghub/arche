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
            (
                "\nrule name here (1 message(s)):\nvery detailed message\n"
                "\nother result there (1 message(s)):\nother detailed message\n"
            ),
        ),
        ([("everything fine", {Level.INFO: [("summary",)]})], ""),
    ],
)
def test_write_details(capsys, messages, expected_details):
    r = Report()
    for m in messages:
        r.save(create_result(*m))
    r.write_details()
    assert capsys.readouterr().out == expected_details


@pytest.mark.parametrize(
    "message, expected_details",
    [
        (
            {Level.INFO: [("summary", "very detailed message")]},
            "very detailed message\n",
        ),
        ({Level.INFO: [("summary",)]}, ""),
    ],
)
def test_write_rule_details(capsys, message, expected_details):
    outcome = create_result("rule name here", message)
    Report.write_rule_details(outcome)
    assert capsys.readouterr().out == expected_details


@pytest.mark.parametrize(
    "errors, short, keys_limit, expected_messages",
    [
        (
            {"something happened": pd.Series(["1", "2"])},
            False,
            10,
            [
                "2 items affected - something happened: <a href='1'>1</a> <a href='2'>2</a>"
            ],
        ),
        (
            {"something went bad": [i for i in range(10)]},
            False,
            1,
            ["10 items affected - something went bad: <a href='5'>5</a>"],
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
            [f"15 items affected - err{i}: <a href='5'>5</a>" for i in range(1, 7)],
        ),
        (
            {
                "err1": ["1"],
                "err2": ["2"],
                "err3": ["3"],
                "err4": ["4"],
                "err5": ["5", "7"],
                "err6": ["6", "7"],
            },
            True,
            1,
            [
                "1 items affected - err1: <a href='1'>1</a>",
                "1 items affected - err2: <a href='2'>2</a>",
                "1 items affected - err3: <a href='3'>3</a>",
                "1 items affected - err4: <a href='4'>4</a>",
                "2 items affected - err5: <a href='5'>5</a> <a href='7'>7</a>",
            ],
        ),
        (
            {"something happened": set(["1"])},
            False,
            10,
            ["1 items affected - something happened: <a href='1'>1</a>"],
        ),
    ],
)
def test_write_detailed_errors(mocker, errors, short, keys_limit, expected_messages):
    mocker.patch("pandas.Series.sample", return_value=["5"], autospec=True)
    html_mock = mocker.patch("arche.report.HTML", autospec=True)
    Report.write_detailed_errors(errors, short, keys_limit)
    calls = []
    for m in expected_messages:
        calls.append(mocker.call(m))
    html_mock.assert_has_calls(calls, any_order=True)


@pytest.mark.parametrize(
    "keys, limit, expected_sample",
    [
        (
            pd.Series(f"{SH_URL}/112358/13/21/item/0"),
            5,
            f"<a href='{SH_URL}/112358/13/21/item/0'>0</a>",
        ),
        (
            pd.Series([f"{SH_URL}/112358/13/21/item/{i}" for i in range(20)]),
            10,
            f"<a href='{SH_URL}/112358/13/21/item/5'>5</a>",
        ),
    ],
)
def test_sample_keys(mocker, keys, limit, expected_sample):
    mocker.patch(
        "pandas.Series.sample",
        return_value=[f"{SH_URL}/112358/13/21/item/5"],
        autospec=True,
    )
    assert Report.sample_keys(keys, limit) == expected_sample


def test_save():
    r = Report()
    dummy_result = create_result("dummy", {Level.INFO: [("outcome",)]})
    r.save(dummy_result)
    assert r.results == {dummy_result.name: dummy_result}
