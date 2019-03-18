from arche import SH_URL
from arche.tools import api
from conftest import Job, Source
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
    "mocked_items, expected_items, start, count, filters",
    [
        (
            [
                {"_key": "112358/13/21/0", "_type": "NameItem", "name": "Elizabeth"},
                {"_key": "112358/13/21/1", "_type": "NameItem", "name": "Margaret"},
            ],
            [
                {
                    "_key": f"{SH_URL}/source_key/item/0",
                    "_type": "NameItem",
                    "name": "Elizabeth",
                },
                {
                    "_key": f"{SH_URL}/source_key/item/1",
                    "_type": "NameItem",
                    "name": "Margaret",
                },
            ],
            0,
            1,
            None,
        ),
        (
            [
                {"_key": "124fdfs20", "_type": "NameItem", "name": "Elizabeth"},
                {"_key": "124fdfs23", "_type": "CityItem", "name": "Margaret"},
            ],
            [
                {
                    "_key": f"{SH_URL}/source_key/124fdfs20",
                    "_type": "NameItem",
                    "name": "Elizabeth",
                }
            ],
            0,
            100,
            [("_type", ["NameItem"])],
        ),
    ],
)
def test_get_items(mocker, mocked_items, expected_items, start, count, filters):
    mocker.patch(
        "arche.tools.api.get_source", return_value=Source(mocked_items), autospec=True
    )
    items = api.get_items("source_key", start_index=start, count=count, filters=filters)

    assert items == expected_items[start : start + count]


source_items = [
    {"_key": "0", "_type": "NameItem", "name": "Elizabeth"},
    {"_key": "1", "_type": "AddressItem", "zip": "84568"},
    {"_key": "2", "_type": "NameItem", "name": "Yulia"},
    {"_key": "3", "_type": "NameItem", "name": "Vivien"},
    {"_key": "4", "_type": "AddressItem", "zip": "1332"},
    {"_key": "5", "_type": "NameItem", "name": "Color"},
]

expected_items = [
    {"_key": f"{SH_URL}/k/0", "_type": "NameItem", "name": "Elizabeth"},
    {"_key": f"{SH_URL}/k/1", "_type": "AddressItem", "zip": "84568"},
    {"_key": f"{SH_URL}/k/2", "_type": "NameItem", "name": "Yulia"},
    {"_key": f"{SH_URL}/k/3", "_type": "NameItem", "name": "Vivien"},
    {"_key": f"{SH_URL}/k/4", "_type": "AddressItem", "zip": "1332"},
    {"_key": f"{SH_URL}/k/5", "_type": "NameItem", "name": "Color"},
]


@pytest.mark.parametrize(
    "count, start_index, expected_items",
    [
        (1000, 0, expected_items),
        (1, 0, [expected_items[0]]),
        (1, 3, [expected_items[3]]),
        (4, 2, expected_items[2:]),
    ],
)
def test_get_items_with_pool(mocker, count, start_index, expected_items):
    mocker.patch(
        "arche.tools.api.get_source", return_value=Source(source_items), autospec=True
    )
    mocker.patch("arche.tools.api.helpers.cpus_count", return_value=1, autospec=True)
    assert api.get_items_with_pool("k", count, start_index) == expected_items


input_data = [
    ("112358/13/21/0", "112358/13/21", f"{SH_URL}/112358/13/21/item/0"),
    (
        "be-006c631313cfc_e1670777293a94b6",
        "112358/collections/s/pages",
        f"{SH_URL}/112358/collections/s/pages/be-006c631313cfc_e1670777293a94b6",
    ),
]


@pytest.mark.parametrize("key, source, expected_url", input_data)
def test_key_to_url(key, source, expected_url):
    assert api.key_to_url(key, source) == expected_url
