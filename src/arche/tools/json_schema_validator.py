from collections import defaultdict
from typing import Any, Dict, Deque

from arche.readers.items import RawItems
from arche.readers.schema import Schema
import fastjsonschema
from jsonschema import FormatChecker, validators
from tqdm import tqdm_notebook


class JsonSchemaValidator:
    def __init__(self, schema: Schema):
        self.errors = defaultdict(set)
        self.schema = schema

    def run(self, raw_items: RawItems, fast: bool) -> None:
        if fast:
            self.fast_validate(raw_items)
        else:
            self.validate(raw_items)

    def fast_validate(self, raw_items: RawItems) -> None:
        """Verify items one by one. It stops after the first error in an item in most cases.
        Faster than jsonschema validation"""
        validate = fastjsonschema.compile(self.schema)
        for raw_item in tqdm_notebook(raw_items, desc="JSON Schema Validation"):
            try:
                validate(raw_items)
            except fastjsonschema.JsonSchemaException as error:
                self.errors[str(error)].add(raw_item["_key"])

    def validate(self, raw_items: RawItems) -> None:
        validator = validators.validator_for(self.schema)(self.schema)
        validator.format_checker = FormatChecker()
        for raw_item in tqdm_notebook(raw_items, desc="JSON Schema Validation"):
            self.validate_item(raw_item, validator)

    def validate_item(self, item: Dict[str, Any], validator) -> None:
        """Check a single item against jsonschema

        Args:
            item: a dict with item data
            validator: a validator instance
        """
        for e in validator.iter_errors(item):
            error = self.format_validation_message(
                e.message, e.path, e.schema_path, e.validator
            )
            self.errors[error].add(item["_key"])

    @staticmethod
    def format_validation_message(
        error_msg: str, path: Deque, schema_path: Deque, validator: str
    ) -> str:
        str_path = "/".join(p for p in path if type(p) is str)
        schema_path = "/".join(p for p in schema_path)

        if validator == "anyOf":
            if str_path:
                return f"'{str_path}' does not satisfy 'schema/{schema_path}'"
            else:
                return f"'schema/{schema_path}' failed"

        if "Additional properties are not allowed" in error_msg:
            return error_msg[: error_msg.index(" (")]

        if not str_path:
            return error_msg

        for path_message in [
            "is not of type",
            "does not match",
            "is not one of",
            "is not a",
            "is greater than the maximum of",
            "is less than the minimum of",
            "is too long",
            "is too short",
        ]:
            if path_message in error_msg:
                return f"{str_path} {error_msg[error_msg.index(path_message) :]}"

        return f"{str_path} - {error_msg}"
