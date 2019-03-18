import uuid

from arche.tools import pandas
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest


flatten_df_data = [
    (
        {"name": ["Bob"], "alive": [True], "_key": [0], "following": [None]},
        {"name": ["Bob"], "alive": [True], "_key": [0], "following": [None]},
        {},
    ),
    (
        {"tags": [["western", "comedy"], ["drama", "history"]]},
        {"tags_0": ["western", "drama"], "tags_1": ["comedy", "history"]},
        {"tags_0": "tags", "tags_1": "tags"},
    ),
    (
        {"f": [[0], [0, 1]], "ff": ["k", None], "f_0": [2, None]},
        {
            "f_0": [2.0, None],
            "f_1": [None, 1],
            "ff": ["k", None],
            "f_5ecd5": [0.0, 0.0],
        },
        {"f_1": "f", "f_5ecd5": "f"},
    ),
    (
        {"f": [[0], [0, [2, 3]]], "ff": ["k", "s"], "f_0": [5, 6]},
        {
            "f_0": [5, 6],
            "ff": ["k", "s"],
            "f_5ecd5": [0.0, 0.0],
            "f_1_0": [None, 2],
            "f_1_1": [None, 3],
        },
        {"f_5ecd5": "f", "f_1_0": "f", "f_1_1": "f"},
    ),
    (
        {
            "links": [
                [
                    {"Instagram": "http://www.instagram.com/illinoistoolworks"},
                    {"ITW website": "http://www.itw.com"},
                ]
            ]
        },
        {
            "links_0_Instagram": ["http://www.instagram.com/illinoistoolworks"],
            "links_1_ITW website": ["http://www.itw.com"],
        },
        {"links_0_Instagram": "links", "links_1_ITW website": "links"},
    ),
    (
        {
            "links": [
                [
                    {"Instagram": ["http://www.instagram.com/ill"]},
                    {"ITW website": ["http://www.itw.com"]},
                ]
            ]
        },
        {
            "links_0_Instagram_0": ["http://www.instagram.com/ill"],
            "links_1_ITW website_0": ["http://www.itw.com"],
        },
        {"links_0_Instagram_0": "links", "links_1_ITW website_0": "links"},
    ),
]


@pytest.mark.parametrize("data, expected_data, expected_map", flatten_df_data)
def test_flatten_df(data, expected_data, expected_map, mocker):
    mock_uuid = mocker.patch("uuid.uuid1", autospec=True)
    mock_uuid.return_value = uuid.UUID(hex="5ecd5827b6ef4067b5ac3ceac07dde9f")
    df, columns_map = pandas.flatten_df(pd.DataFrame(data))
    assert_frame_equal(df, pd.DataFrame(expected_data), check_like=True)
    assert columns_map == expected_map
