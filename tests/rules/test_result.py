from arche.rules.result import Level, Message, Result
from conftest import create_named_df, create_result
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
    "source, target",
    [
        (
            pd.Series([0, 1], index=["f", "l"], name="n"),
            pd.Series([0, 1], index=["f", "l"], name="n"),
        ),
        (pd.DataFrame([0, 1]), pd.DataFrame([0, 1])),
    ],
)
def test_tensors_equal(source, target):
    assert Result.tensors_equal(source, target)


@pytest.mark.parametrize(
    "source, target",
    [
        (
            pd.Series([0, 1], index=["f", "l"], name="s"),
            pd.Series([0, 1], index=["f", "l"], name="n"),
        ),
        (pd.DataFrame([0, 1]), pd.DataFrame([0, 1], index=["m", "s"])),
    ],
)
def test_tensors_not_equal(source, target):
    assert not Result.tensors_equal(source, target)


@pytest.mark.parametrize(
    "message, stats, exp_md_output, exp_txt_outputs",
    [
        (
            {Level.INFO: [("summary", "very detailed message")]},
            [pd.Series([1, 2], name="Fields coverage")],
            "rule name here:",
            ["\tsummary", "very detailed message"],
        ),
        (
            {Level.INFO: [("summary",)]},
            [
                create_named_df(
                    {"s": [0.25]}, index=["us"], name="Coverage for boolean fields"
                )
            ],
            "rule name here:",
            ["\tsummary"],
        ),
    ],
)
def test_show(mocker, capsys, message, stats, exp_md_output, exp_txt_outputs):
    mock_pio_show = mocker.patch("plotly.io.show", autospec=True)
    mocked_md = mocker.patch("arche.report.display_markdown", autospec=True)
    mocked_print = mocker.patch("builtins.print", autospec=True)
    res = create_result("rule name here", message, stats=stats)
    res.show()
    mock_pio_show.assert_called_once_with(res.figures[0])
    mocked_md.assert_called_with(exp_md_output)
    mocked_print.assert_has_calls(mocker.call(o) for o in exp_txt_outputs)


@pytest.mark.parametrize(
    "left_params, right_params",
    [
        (
            (
                "s",
                {Level.INFO: ["sum", "det", {"err1": [0, 1]}]},
                [pd.Series([0], name="s"), pd.DataFrame({"s": [0]})],
                2,
                ["err1"],
                1,
            ),
            (
                "s",
                {Level.INFO: ["sum", "det", {"err1": [0, 1]}]},
                [pd.Series([0], name="s"), pd.DataFrame({"s": [0]})],
                2,
                ["err1"],
                1,
            ),
        ),
        (("s",), ("s",)),
    ],
)
def test_result_equal(left_params, right_params):
    assert Result(*left_params) == Result(*right_params)


@pytest.mark.parametrize(
    "left_params, right_params",
    [
        (
            (
                "s",
                {Level.INFO: ["sum", "det", {"err1": [0, 1]}]},
                [pd.Series([0], name="A name"), pd.DataFrame([0])],
                2,
                ["err1"],
                1,
            ),
            (
                "s",
                {Level.INFO: ["sum", "det", {"err1": [0, 1]}]},
                [pd.Series([0], name="A series name"), pd.DataFrame([0])],
                2,
                ["err1"],
                1,
            ),
        ),
        (("s",), ("t",)),
    ],
)
def test_result_not_equal(left_params, right_params):
    assert Result(*left_params) != Result(*right_params)
