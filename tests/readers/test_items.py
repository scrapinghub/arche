from arche import SH_URL
from arche.readers.items import Items, CollectionItems, JobItems
from conftest import Collection, Job
import pandas as pd
import pytest


@pytest.mark.parametrize(
    "name, expected_name", [("address", "address"), ("address_0", "address")]
)
def test_get_origin_column_name(get_job_items, name, expected_name):
    items = get_job_items
    items._columns_map = {"address_0": "address"}
    assert items.get_origin_column_name(name) == expected_name


collection_items = pd.DataFrame(
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
    "df, expected_df",
    [(pd.DataFrame({"0": [0]}), pd.DataFrame({"0": [0], "_key": ["0"]}))],
)
def test_items_from_df(df, expected_df):
    items = Items.from_df(df)
    pd.testing.assert_frame_equal(items.df, expected_df)


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
        return_value=collection_items.copy().iloc[:expected_count],
        autospec=True,
    )
    items = CollectionItems("key", count, filters, expand)
    assert items.key == "key"
    assert items.filters == filters
    assert items.expand == expand
    pd.testing.assert_frame_equal(items.df, expected_col_items.iloc[:expected_count])
    pd.testing.assert_frame_equal(items.flat_df, items.df)

    assert items.size == len(expected_col_items.iloc[:expected_count])

    assert items.limit == len(collection_items)
    assert items.count == expected_count
    get_items_mock.assert_called_once_with("key", expected_count, 0, filters)


job_items = pd.DataFrame(
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
        return_value=job_items.iloc[start:expected_count].reset_index(drop=True),
        autospec=True,
    )
    items = JobItems(
        key="112358/13/21", start=start, count=count, filters=None, expand=False
    )
    pd.testing.assert_frame_equal(
        items.df, expected_job_items.iloc[start:count].reset_index(drop=True)
    )
    assert items.count == count
