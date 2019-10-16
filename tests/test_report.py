from arche import Arche
from arche import SH_URL
from arche.report import Report
from arche.rules.result import Level
from conftest import create_result, get_report_from_iframe
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
        )
    ],
)
def test_report_call(mocker, get_df, capsys, messages, expected_details):
    mocked_display = mocker.patch("arche.report.display_html", autospec=True)

    r = Report()
    for m in messages:
        result = create_result(*m, stats=[get_df])
        r.save(result)
    r()

    report_html = get_report_from_iframe(mocked_display.mock_calls[0][1][0])
    assert report_html.count("Plotly.newPlot") == 2
    assert report_html.count("rule name here - INFO") == 2
    assert report_html.count("other result there - INFO") == 2


def test_report_call_arguments(mocker):
    message = {Level.INFO: [("summary", "very detailed message")]}

    mocked_display = mocker.patch("arche.report.display_html", autospec=True)
    outcome = create_result("rule name here", message)

    Report()(outcome)
    report_html = get_report_from_iframe(mocked_display.mock_calls[0][1][0])
    assert report_html.count("very detailed message") == 1


def test_report_call_with_errors(mocker, get_job_items, get_schema):
    mocked_display = mocker.patch("arche.report.display_html", autospec=True)
    url = f"{SH_URL}/112358/13/21/item/1"
    schema = {"type": "object", "required": ["price"], "properties": {"price": {}}}
    g = Arche("source", schema=schema)
    g._source_items = get_job_items
    g.report_all()
    report_html = get_report_from_iframe(mocked_display.mock_calls[0][1][0])
    mocked_display.assert_called_once()
    assert "JSON Schema Validation - FAILED" in report_html
    assert report_html.count('href="{}"'.format(url)) == 1
    assert report_html.count("&#39;price&#39; is a required property") == 1


def test_report_iframe(mocker, get_job_items, get_schema):
    mocked_display = mocker.patch("arche.report.display_html", autospec=True)
    schema = {"type": "object", "required": ["price"], "properties": {"price": {}}}
    g = Arche("source", schema=schema)
    g._source_items = get_job_items
    g.run_all_rules()
    g.report()
    mocked_display.assert_called_once()
    assert "<iframe" in mocked_display.mock_calls[0][1][0]


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
