from collections import deque

import arche.tools.schema as schema_tools
import numpy as np
import pytest

schema = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "definitions": {
        "url": {
            "pattern": (
                r"^https?://(www\.)?[a-z0-9.-]*\.[a-z]{2,}"
                r"([^<>%\x20\x00-\x1f\x7F]|%[0-9a-fA-F]{2})*$"
            )
        }
    },
    "type": "object",
    "properties": {"url": {"type": "string"}, "id": {"type": "integer"}},
    "additionalProperties": False,
    "required": ["id", "url"],
}


def test_set_item_no():
    assert schema_tools.set_item_no(4) == [0, 1, 2, 3]
    assert schema_tools.set_item_no(1) == [0]
    assert len(schema_tools.set_item_no(5)) == 4
    assert len(schema_tools.set_item_no(124112414)) == 4


def test_infer_schema():
    item1 = {"url": "https://example.com", "id": 0}
    item2 = {"url": "https://example.com", "id": 1}

    assert schema_tools.infer_schema([item1, item2]) == schema
    assert schema_tools.infer_schema([item2]) == schema


def test_basic_json_schema(mocker):
    mocked_create_js = mocker.patch(
        "arche.tools.schema.create_json_schema", return_value=schema, autospec=True
    )
    assert schema_tools.basic_json_schema("235801/1/15", [0, 5]).raw == schema
    mocked_create_js.assert_called_once_with("235801/1/15", [0, 5])


def test_create_json_schema(mocker, get_job, get_raw_items):
    mocker.patch("arche.tools.api.get_job", return_value=get_job, autospec=True)
    mocker.patch("arche.tools.api.get_items", return_value=get_raw_items, autospec=True)
    schema_tools.create_json_schema(get_job.key, [2])
    assert schema_tools.create_json_schema(get_job.key, [2, 3]) == {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "definitions": {
            "url": {
                "pattern": (
                    r"^https?://(www\.)?[a-z0-9.-]*\.[a-z]{2,}"
                    r"([^<>%\x20\x00-\x1f\x7F]|%[0-9a-fA-F]{2})*$"
                )
            }
        },
        "additionalProperties": False,
        "type": "object",
        "properties": {"name": {"type": "string"}, "price": {"type": "integer"}},
        "required": ["name", "price"],
    }


@pytest.mark.parametrize(
    "error_msg, path, schema_path, validator, formatted_msg",
    [
        (
            "is not of type 'string'",
            ["root_field", 0, "deep", "deeper", 100],
            [],
            "type",
            "root_field/deep/deeper is not of type 'string'",
        ),
        ("other error", ["field"], [], "", "field - other error"),
        (
            "'biography' is a required property",
            [],
            [],
            "required",
            "'biography' is a required property",
        ),
        (
            "'s' does not match any of the regexes: '^spec(_\\w.+){1,}$'",
            [],
            [],
            "pattern",
            "'s' does not match any of the regexes: '^spec(_\\w.+){1,}$'",
        ),
        (
            "Additional properties are not allowed ('delivery_options')",
            [],
            [],
            "additionalProperties",
            "Additional properties are not allowed",
        ),
        (
            "https://sto' is not a 'date-time'",
            ["url"],
            [],
            "format",
            "url is not a 'date-time'",
        ),
        ("a very long error message", [], ["anyOf"], "anyOf", "'schema/anyOf' failed"),
        (
            "a very very long error message",
            ["options", 0],
            ["properties", "options", "items", "anyOf"],
            "anyOf",
            "'options' does not satisfy 'schema/properties/options/items/anyOf'",
        ),
    ],
)
def test_format_validation_message(
    error_msg, path, schema_path, validator, formatted_msg
):
    assert (
        schema_tools.format_validation_message(
            error_msg, deque(path), deque(schema_path), validator
        )
        == formatted_msg
    )


def test_full_validate_fails():
    errors = schema_tools.full_validate(
        {"properties": {"NAME": {"type": "string"}}},
        np.array([{"NAME": None, "_key": "0"}, {"NAME": None}]),
        keys=[0, 1],
    )
    assert errors == {"NAME is not of type 'string'": {0, 1}}


def test_full_validate_type_key():
    errors = schema_tools.full_validate(
        {"properties": {"A": {"type": "number"}}, "additionalProperties": False},
        np.array([{"A": 0, "_key": "0", "_type": "Some"}, {"A": 1}]),
        [0, 1],
    )
    assert not errors


def test_fast_validate_fails():
    errors = schema_tools.fast_validate(
        {"type": "object", "properties": {"v": {"type": "number"}}},
        [{"v": "0"}, {"v": "1"}],
        keys=[0, 1],
    )
    assert errors == {"data.v must be number": {0, 1}}


def test_fast_validate(get_schema, get_raw_items):
    errors = schema_tools.fast_validate(
        get_schema, get_raw_items, keys=list(range(len(get_raw_items)))
    )
    assert not errors
