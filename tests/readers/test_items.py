from arche.readers.items import CollectionItems
from conftest import Collection
import pytest


@pytest.mark.parametrize(
    "name, expected_name", [("address", "address"), ("address_0", "address")]
)
def test_get_origin_column_name(get_job_items, name, expected_name):
    items = get_job_items
    items._columns_map = {"address_0": "address"}
    assert items.get_origin_column_name(name) == expected_name


collection_items = [
    {"_key": "0", "_type": "NameItem", "name": "Book"},
    {"_key": "1", "_type": "NameItem", "name": "Movie"},
    {"_key": "2", "_type": "NameItem", "name": "Guitar"},
    {"_key": "3", "_type": "NameItem", "name": "Dog"},
]


@pytest.mark.parametrize(
    "count, filters, expand, expected_count",
    [(1, None, False, 1), (None, None, True, 4)],
)
def test_collection_items(mocker, count, filters, expand, expected_count):
    mocker.patch(
        "arche.tools.api.get_collection",
        return_value=Collection(collection_items),
        autospec=True,
    )
    get_items_mock = mocker.patch(
        "arche.tools.api.get_items",
        return_value=collection_items[:expected_count],
        autospec=True,
    )
    items = CollectionItems("key", count, filters, expand)
    assert items.key == "key"
    assert items._count == count
    assert items.filters == filters
    assert items.expand == expand
    assert items.dicts == collection_items[:expected_count]
    assert items._flat_df is None

    assert items.limit == len(collection_items)
    assert items.count == expected_count
    get_items_mock.assert_called_once_with("key", expected_count, 0, filters)


job_items = [
    {"_key": "112358/13/21/0", "_type": "NameItem", "name": "Elizabeth"},
    {"_key": "112358/13/21/1", "_type": "NameItem", "name": "Margaret"},
    {"_key": "112358/13/21/2", "_type": "NameItem", "name": "Yulia"},
    {"_key": "112358/13/21/3", "_type": "NameItem", "name": "Vivien"},
]
