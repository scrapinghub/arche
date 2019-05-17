from arche.tools import api
from conftest import Job, Source
import numpy as np
import pytest


@pytest.mark.parametrize(
    "metadata, stats, expected_count",
    [
        ({"state": "deleted"}, None, 0),
        (None, {}, 0),
        (None, {"totals": {"input_values": 0}}, 0),
        (None, {"totals": {"input_values": 10}}, 10),
    ],
)
def test_get_items_count(metadata, stats, expected_count):
    assert api.get_items_count(Job(metadata=metadata, stats=stats)) == expected_count


@pytest.mark.parametrize(
    "mocked_items, start, count, filters, expected_items",
    [
        (
            [
                {"_key": "112358/13/21/0", "_type": "NameItem", "name": "Elizabeth"},
                {"_key": "112358/13/21/1", "_type": "NameItem", "name": "Margaret"},
            ],
            0,
            1,
            None,
            np.array(
                [{"_key": "112358/13/21/0", "_type": "NameItem", "name": "Elizabeth"}]
            ),
        ),
        (
            [
                {"_key": "124fdfs20", "_type": "NameItem", "name": "Elizabeth"},
                {"_key": "124fdfs23", "_type": "CityItem", "name": "Margaret"},
            ],
            0,
            100,
            [("_type", ["NameItem"])],
            np.array([{"_key": "124fdfs20", "_type": "NameItem", "name": "Elizabeth"}]),
        ),
    ],
)
def test_get_items(mocker, mocked_items, start, count, filters, expected_items):
    mocker.patch(
        "arche.tools.api.get_source", return_value=Source(mocked_items), autospec=True
    )
    items = api.get_items("source_key", start_index=start, count=count, filters=filters)
    np.testing.assert_array_equal(items, expected_items)


source_items = [
    {"_key": "0", "_type": "NameItem", "name": "Elizabeth"},
    {"_key": "1", "_type": "AddressItem", "zip": "84568"},
    {"_key": "2", "_type": "NameItem", "name": "Yulia"},
    {"_key": "3", "_type": "NameItem", "name": "Vivien"},
    {"_key": "4", "_type": "AddressItem", "zip": "1332"},
    {"_key": "5", "_type": "NameItem", "name": "Color"},
]


@pytest.mark.parametrize(
    "count, start_index, expected_items",
    [
        (1000, 0, np.array(source_items)),
        (1, 0, np.array([{"_key": "0", "_type": "NameItem", "name": "Elizabeth"}])),
        (1, 3, np.array([{"_key": "3", "_type": "NameItem", "name": "Vivien"}])),
        (4, 2, np.array(source_items)[2:]),
    ],
)
def test_get_items_with_pool(mocker, count, start_index, expected_items):
    mocker.patch(
        "arche.tools.api.get_source", return_value=Source(source_items), autospec=True
    )
    mocker.patch("arche.tools.api.helpers.cpus_count", return_value=1, autospec=True)
    np.testing.assert_array_equal(
        api.get_items_with_pool("k", count, start_index), expected_items
    )
