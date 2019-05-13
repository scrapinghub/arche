from arche import SH_URL
from arche.readers.items import Items, CollectionItems, JobItems
from conftest import Collection, Job
import numpy as np
import pandas as pd
import pytest


@pytest.mark.parametrize(
    "name, expected_name", [("address", "address"), ("address_0", "address")]
)
def test_get_origin_column_name(get_cloud_items, name, expected_name):
    items = Items.from_df(pd.DataFrame(get_cloud_items))
    items._columns_map = {"address_0": "address"}
    assert items.get_origin_column_name(name) == expected_name


@pytest.mark.parametrize(
    "df, expected_raw, expected_df",
    [
        (
            pd.DataFrame({"0": [0]}),
            [{"0": 0, "_key": "0"}],
            pd.DataFrame({"0": [0], "_key": ["0"]}),
        )
    ],
)
def test_items_from_df(df, expected_raw, expected_df):
    items = Items.from_df(df)
    np.testing.assert_array_equal(items.raw, expected_raw)
    pd.testing.assert_frame_equal(items.df, expected_df)


@pytest.mark.parametrize(
    "raw",
    [
        ([{"0": 0, "_key": "0"}, {"_key": "1"}]),
        (np.array([{"0": 0, "_key": "0"}, {"_key": "1"}])),
    ],
)
def test_items_from_array(raw):
    items = Items.from_array(raw)
    np.testing.assert_array_equal(items.raw, np.array(raw))
    pd.testing.assert_frame_equal(items.df, pd.DataFrame(list(raw)))


collection_items = np.array(
    [
        {"_key": "0", "name": "Book"},
        {"_key": "1", "name": "Movie"},
        {"_key": "2", "name": "Guitar"},
        {"_key": "3", "name": "Dog"},
    ]
)
expected_col_items = pd.DataFrame(
    [
        {"_key": f"{SH_URL}/key/0", "name": "Book"},
        {"_key": f"{SH_URL}/key/1", "name": "Movie"},
        {"_key": f"{SH_URL}/key/2", "name": "Guitar"},
        {"_key": f"{SH_URL}/key/3", "name": "Dog"},
    ]
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
    pd.testing.assert_frame_equal(items.df, expected_col_items.iloc[:expected_count])
    pd.testing.assert_frame_equal(items.flat_df, items.df)

    assert len(items) == len(expected_col_items.iloc[:expected_count])
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
expected_job_items = pd.DataFrame(
    [
        {"_key": f"{SH_URL}/112358/13/21/item/0", "name": "Elizabeth"},
        {"_key": f"{SH_URL}/112358/13/21/item/1", "name": "Margaret"},
        {"_key": f"{SH_URL}/112358/13/21/item/2", "name": "Yulia"},
        {"_key": f"{SH_URL}/112358/13/21/item/3", "name": "Vivien"},
    ]
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
    pd.testing.assert_frame_equal(
        items.df, expected_job_items.iloc[start:count].reset_index(drop=True)
    )
    assert items.count == count


def test_process_df():
    df = Items.process_df(
        pd.DataFrame([[dict(), list(), "NameItem"]], columns=["a", "b", "_type"])
    )
    exp_df = pd.DataFrame([[np.nan, np.nan, "NameItem"]], columns=["a", "b", "_type"])
    exp_df["_type"] = exp_df["_type"].astype("category")
    pd.testing.assert_frame_equal(df, exp_df)
