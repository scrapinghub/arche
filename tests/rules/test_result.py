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
    "message, stats, expected_details",
    [
        (
            {Level.INFO: [("summary", "very detailed message")]},
            [pd.Series([1, 2], name="Fields coverage")],
            "\nrule name here:\n\tsummary\nvery detailed message\n",
        ),
        (
            {Level.INFO: [("summary",)]},
            [
                create_named_df(
                    {"s": [0.25]}, index=["us"], name="Coverage for boolean fields"
                )
            ],
            "\nrule name here:\n\tsummary\n",
        ),
    ],
)
def test_show(mocker, capsys, message, stats, expected_details):
    mock_pio_show = mocker.patch("plotly.io.show", autospec=True)
    r = create_result("rule name here", message, stats=stats)
    r.show()
    mock_pio_show.assert_called_once_with(r.figures[0])
    assert capsys.readouterr().out == expected_details


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
