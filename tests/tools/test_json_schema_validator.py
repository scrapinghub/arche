from collections import deque

from arche.tools.json_schema_validator import JsonSchemaValidator as JSV
import numpy as np
import pytest


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
        JSV.format_validation_message(
            error_msg, deque(path), deque(schema_path), validator
        )
        == formatted_msg
    )


def test_validate():
    jsv = JSV({"properties": {"NAME": {"type": "string"}}})
    jsv.validate(np.array([{"NAME": None, "_key": "0"}]))
    assert jsv.errors == {"NAME is not of type 'string'": {"0"}}


def test_fast_validate():
    jsv = JSV({"type": "number"})
    jsv.fast_validate(np.array([{"_key": "0"}, {"_key": "1"}]))
    assert jsv.errors == {"data must be number": {"0", "1"}}


def test_run():
    schema = {"properties": {"NAME": {"type": "string"}}}
    jsv = JSV(schema)
    assert jsv.schema == schema
    jsv.run(np.array([{"NAME": None, "_key": "0", "__DEBUG": None}]), fast=False)

    assert jsv.errors == {"NAME is not of type 'string'": {"0"}}
