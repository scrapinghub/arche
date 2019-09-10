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
    "message, stats, outputs",
    [
        (
            {Level.INFO: [("summary", "very detailed message")]},
            [pd.Series([1, 2], name="Fields coverage")],
            ["<h4>test show</h4>", "summary", "very detailed message"],
        ),
        (
            {Level.INFO: [("summary",)]},
            [
                create_named_df(
                    {"s": [0.25]}, index=["us"], name="Coverage for boolean fields"
                )
            ],
            ["<h4>test show</h4>", "summary"],
        ),
    ],
)
def test_show(mocker, capsys, message, stats, outputs):
    mock_pio_show = mocker.patch("plotly.io.show", autospec=True)
    mocked_md = mocker.patch("arche.report.display_markdown", autospec=True)
    res = create_result("test show", message, stats=stats)
    res.show()
    mock_pio_show.assert_called_once_with(res.figures[0])
    mocked_md.assert_has_calls(mocker.call(o) for o in outputs)


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
