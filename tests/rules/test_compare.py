import arche.rules.compare as compare
from arche.rules.result import Level
from conftest import *
import pytest


@pytest.mark.parametrize(
    ["source", "target", "fields", "normalize", "expected", "more_stats"],
    [
        (
            {
                "one": list(range(5)) + ["42"] * 5,
                "two": list(range(10)),
                "three": [np.nan] * 5 + list(range(5)),
            },
            {
                "one": list(range(5, 10)) + [4] * 6,
                "two": list(range(11)),
                "three": [np.nan] * 10 + [1],
            },
            ["one", "two", "three"],
            False,
            {
                Level.INFO: [
                    ("10 `non NaN ones` - 9 new, 1 same",),
                    ("10 `non NaN twos` - 0 new, 10 same",),
                    ("1 `twos` are missing", None, {"10 `twos` are missing": {10}}),
                    ("5 `non NaN threes` - 4 new, 1 same",),
                ],
                Level.ERROR: [
                    (
                        "5 `ones` are missing",
                        None,
                        {"5, 6, 7, 8, 9 `ones` are missing": set(range(5))},
                    )
                ],
            },
            {
                "one": {
                    "same": pd.Series([4], index=[4], dtype="object"),
                    "new": pd.Series(
                        [0, 1, 2, 3] + ["42"] * 5, index=[0, 1, 2, 3, 5, 6, 7, 8, 9]
                    ),
                    "missing": pd.Series(list(range(5, 10))),
                },
                "two": {
                    "same": pd.Series(list(range(10))),
                    "new": pd.Series(dtype=np.int64),
                    "missing": pd.Series([10], index=[10]),
                },
                "three": {
                    "same": pd.Series([1.0], index=[6]),
                    "new": pd.Series([0.0, 2.0, 3.0, 4.0], index=[5, 7, 8, 9]),
                    "missing": pd.Series(),
                },
            },
        ),
        (
            {
                "four": [{i} for i in range(2)]
                + [{"K": {"k": i}} for i in range(2)]
                + ["l"] * 6
            },
            {
                "four": [{i} for i in range(4)]
                + [{"k": {"k": i}} for i in range(4)]
                + ["L"] * 20
            },
            ["four"],
            True,
            {
                Level.INFO: [
                    ("10 `non NaN fours` - 0 new, 10 same",),
                    (
                        "4 `fours` are missing",
                        None,
                        {
                            "{2}, {3}, {'k': {'k': 2}}, {'k': {'k': 3}} `fours` are missing": {
                                2,
                                3,
                                6,
                                7,
                            }
                        },
                    ),
                ]
            },
            {
                "four": {
                    "same": pd.Series(
                        [str({i}) for i in range(2)]
                        + [str({"k": {"k": i}}) for i in range(2)]
                        + ["l"] * 6
                    ),
                    "new": pd.Series(dtype=object),
                    "missing": pd.Series(
                        ["{2}", "{3}", "{'k': {'k': 2}}", "{'k': {'k': 3}}"],
                        index={2, 3, 6, 7},
                    ),
                }
            },
        ),
    ],
)
def test_fields(source, target, fields, normalize, expected, more_stats):
    assert_results_equal(
        compare.fields(pd.DataFrame(source), pd.DataFrame(target), fields, normalize),
        create_result("Fields Difference", expected, more_stats=more_stats),
        check_index_type=False,
    )
