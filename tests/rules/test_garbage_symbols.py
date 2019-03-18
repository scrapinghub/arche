from arche import SH_URL
from arche.rules.garbage_symbols import garbage_symbols
from arche.rules.result import Level
from conftest import create_result
import pytest


dirty_inputs = [
    (
        [
            {
                "_key": f"{SH_URL}/112358/13/21/item/0",
                "name": " Blacky Robeburned",
                "address": "here goes &AMP",
                "phone": "<h1>144</h1>.sx-prime-pricing-row { float: left; }",
                "rank": 14441,
            },
            {
                "_key": f"{SH_URL}/112358/13/21/item/1",
                "name": "<!--Leprous Jim-->",
                "address": "Some street",
                "phone": "1144",
                "rank": 2_039_857,
            },
        ],
        {
            Level.ERROR: [
                (
                    "100.0% (2) items affected",
                    None,
                    {
                        "100.0% of 'name' values contain [' ', '-->', '<!--']": [
                            f"{SH_URL}/112358/13/21/item/0",
                            f"{SH_URL}/112358/13/21/item/1",
                        ],
                        "50.0% of 'address' values contain ['&AMP']": [
                            f"{SH_URL}/112358/13/21/item/0"
                        ],
                        (
                            "50.0% of 'phone' values contain "
                            "['.sx-prime-pricing-ro', '</h1>', '<h1>']"
                        ): [f"{SH_URL}/112358/13/21/item/0"],
                    },
                )
            ]
        },
        2,
        2,
    ),
    ([{"_key": "0"}], {}, 1, 0),
]


@pytest.mark.parametrize(
    "get_job_items, expected_messages, expected_items_count, expected_err_items_count",
    dirty_inputs,
    indirect=["get_job_items"],
)
def test_garbage_symbols(
    get_job_items, expected_messages, expected_items_count, expected_err_items_count
):
    assert garbage_symbols(get_job_items) == create_result(
        "Garbage Symbols",
        expected_messages,
        items_count=expected_items_count,
        err_items_count=expected_err_items_count,
    )
