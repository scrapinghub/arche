from arche.rules.json_schema import check_tags
from arche.rules.result import Level
from conftest import create_result
import numpy as np
import pytest


tags_inputs = [
    (["id"], [], {"category": ["id"]}, {Level.INFO: [("category",)]}),
    (
        ["key"],
        ["key"],
        {"unique": ["id"]},
        {
            Level.INFO: [("unique",)],
            Level.ERROR: [
                ("'id' field(s) was not found in source, but specified in schema",),
                ("'id' field(s) was not found in target, but specified in schema",),
                ("Skipping tag rules",),
            ],
        },
    ),
    (
        ["key", "id"],
        ["key"],
        {"unique": ["id"]},
        {
            Level.INFO: [("unique",)],
            Level.ERROR: [
                ("'id' field(s) was not found in target, but specified in schema",),
                ("Skipping tag rules",),
            ],
        },
    ),
    (
        ["key"],
        np.array([]),
        {"unique": ["id"]},
        {
            Level.INFO: [("unique",)],
            Level.ERROR: [
                ("'id' field(s) was not found in source, but specified in schema",),
                ("Skipping tag rules",),
            ],
        },
    ),
]


@pytest.mark.parametrize(
    "source_columns, target_columns, tags, expected_messages", tags_inputs
)
def test_check_tags(source_columns, target_columns, tags, expected_messages):
    result = check_tags(np.array(source_columns), np.array(target_columns), tags)
    print(result)

    assert result == create_result("Tags", expected_messages)
