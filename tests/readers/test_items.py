from arche import SH_URL
from arche.readers.items import Items, CollectionItems, JobItems
from conftest import Collection, Job
import numpy as np
import pandas as pd
import pytest


@pytest.mark.parametrize(
    "name, expected_name", [("price", "price"), ("name_0", "name")]
)
def test_origin_column_name(get_cloud_items, name, expected_name):
    items = Items.from_df(pd.DataFrame(get_cloud_items))
    assert items.origin_column_name(name) == expected_name


@pytest.mark.parametrize(
    "df, expected_raw, expected_df",
    [
        (pd.DataFrame({"0": [0]}), [{"0": 0}], pd.DataFrame({"0": [0]})),
        (
            pd.DataFrame({"0": [0], "_key": ["0"]}),
            [{"0": 0, "_key": "0"}],
            pd.DataFrame({"0": [0], "_key": ["0"]}),
        ),
    ],
)
def test_items_from_df(df, expected_raw, expected_df):
    items = Items.from_df(df)
    np.testing.assert_array_equal(items.raw, expected_raw)
    pd.testing.assert_frame_equal(items.df, expected_df)


@pytest.mark.parametrize(
    "raw",
    [
        [{"0": 0, "_key": "0"}, {"_key": "1"}],
        np.array([{"0": 0, "_key": "0"}, {"_key": "1"}]),
    ],
)
def test_items_from_array(raw):
    items = Items.from_array(raw)
    np.testing.assert_array_equal(items.raw, np.array(raw))
    pd.testing.assert_frame_equal(items.df, pd.DataFrame(list(raw)))


collection_items = np.array(
    [
        {"_key": "0", "name": "Book", "_type": "Book"},
        {"_key": "1", "name": "Movie", "_type": "Book"},
        {"_key": "2", "name": "Guitar", "_type": "Book"},
        {"_key": "3", "name": "Dog", "_type": "Book"},
    ]
)
expected_col_df = pd.DataFrame(
    {"name": ["Book", "Movie", "Guitar", "Dog"]},
    index=[f"{SH_URL}/key/{i}" for i in range(4)],
)


@pytest.mark.parametrize(
    "count, filters, expand, expected_count",
    [(1, None, False, 1), (None, None, True, 4)],
)
def test_collection_items(mocker, count, filters, expand, expected_count):
    mocker.patch(
        "arche.tools.api.get_collection",
        return_value=Collection(len(collection_items)),
        autospec=True,
    )
    get_items_mock = mocker.patch(
        "arche.tools.api.get_items",
        return_value=collection_items[:expected_count],
        autospec=True,
    )
    items = CollectionItems("key", count, filters, expand)
    assert items.key == "key"
    assert items.filters == filters
    assert items.expand == expand
    np.testing.assert_array_equal(items.raw, collection_items[:expected_count])
    pd.testing.assert_frame_equal(items.df, expected_col_df.iloc[:expected_count])
    pd.testing.assert_frame_equal(items.flat_df, items.df)

    assert len(items) == len(expected_col_df.iloc[:expected_count])
    assert items.limit == len(collection_items)
    assert items.count == expected_count
    get_items_mock.assert_called_once_with("key", expected_count, 0, filters)


job_items = np.array(
    [
        {"_key": "112358/13/21/0", "name": "Elizabeth"},
        {"_key": "112358/13/21/1", "name": "Margaret"},
        {"_key": "112358/13/21/2", "name": "Yulia"},
        {"_key": "112358/13/21/3", "name": "Vivien"},
    ]
)
expected_job_df = pd.DataFrame(
    {"name": ["Elizabeth", "Margaret", "Yulia", "Vivien"]},
    index=[f"{SH_URL}/112358/13/21/item/{i}" for i in range(4)],
)


@pytest.mark.parametrize("start, count, expected_count", [(1, 2, 2)])
def test_job_items(mocker, start, count, expected_count):
    mocker.patch("arche.readers.items.JobItems.job", return_value=Job(), autospec=True)
    mocker.patch(
        "arche.tools.api.get_items",
        return_value=job_items[start:expected_count],
        autospec=True,
    )
    items = JobItems(
        key="112358/13/21", start=start, count=count, filters=None, expand=False
    )
    np.testing.assert_array_equal(items.raw, job_items[start:count])
    pd.testing.assert_frame_equal(items.df, expected_job_df.iloc[start:count])
    assert items.count == count


def test_process_df():
    df = Items.process_df(
        pd.DataFrame([[dict(), list(), [10]]], columns=["a", "b", "ages"])
    )
    exp_df = pd.DataFrame([[np.nan, np.nan, [10]]], columns=["a", "b", "ages"])
    pd.testing.assert_frame_equal(df, exp_df)


@pytest.mark.parametrize(
    "data, expected_cats",
    [
        (
            {
                "a": [i for i in range(100)],
                "b": [False for i in range(100)],
                "c": [[False] for i in range(100)],
                "d": [{0} for i in range(100)],
            },
            ["b"],
        )
    ],
)
def test_categorize(data, expected_cats):
    df = pd.DataFrame(data)
    Items.categorize(df)
    np.testing.assert_array_equal(
        df.select_dtypes(["category"]).columns.values, expected_cats
    )


def test_no_categorize():
    df = pd.DataFrame({"a": [i for i in range(99)]})
    Items.categorize(df)
    assert df.select_dtypes(["category"]).empty


flat_df_inputs = [
    (
        [{"name": "Bob", "alive": True, "following": None}],
        {"alive": [True], "following": [None], "name": ["Bob"]},
        {"name": "name", "alive": "alive", "following": "following"},
    ),
    (
        [{"tags": ["western", "comedy"]}, {"tags": ["drama", "history"]}],
        {"tags_0": ["western", "drama"], "tags_1": ["comedy", "history"]},
        {"tags_0": "tags", "tags_1": "tags"},
    ),
    (
        [
            {
                "links": [
                    {"Instagram": "http://www.instagram.com"},
                    {"ITW website": "http://www.itw.com"},
                ]
            }
        ],
        {
            "links_0_Instagram": ["http://www.instagram.com"],
            "links_1_ITW website": ["http://www.itw.com"],
        },
        {"links_0_Instagram": "links", "links_1_ITW website": "links"},
    ),
    (
        [
            {
                "links": [
                    {"Instagram": ["http://www.instagram.com"]},
                    {"ITW website": ["http://www.itw.com"]},
                ]
            }
        ],
        {
            "links_0_Instagram_0": ["http://www.instagram.com"],
            "links_1_ITW website_0": ["http://www.itw.com"],
        },
        {"links_0_Instagram_0": "links", "links_1_ITW website_0": "links"},
    ),
    # Corner case https://github.com/amirziai/flatten/issues/48
    (
        [
            {"type": [0], "str": "k", "type_0": 5},
            {"type": [0, [2, 3]], "str": "s", "type_0": 6},
        ],
        {
            "str": ["k", "s"],
            "type_0": [5, 6],
            "type_1_0": [np.nan, 2.0],
            "type_1_1": [np.nan, 3.0],
        },
        {"type_0": "type_0", "str": "str", "type_1_0": "type", "type_1_1": "type"},
    ),
    ([{"_type": "XItem", "_key": "0/0/0", "a": 0}], {"a": [0]}, {}),
]


@pytest.mark.parametrize("data, expected_data, expected_map", flat_df_inputs)
def test_flat_df(data, expected_data, expected_map):
    i = Items.from_array(data, expand=True)
    pd.testing.assert_frame_equal(i.flat_df, pd.DataFrame(expected_data))
    for new, old in expected_map.items():
        assert i.origin_column_name(new) == old
