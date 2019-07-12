from collections import defaultdict
import random
from typing import Any, Deque, Dict, List, Optional

from arche.readers.items import RawItems
from arche.readers.schema import Schema
from arche.schema_definitions import extension
from arche.tools import api, helpers
import fastjsonschema
from genson import SchemaBuilder
from jsonschema import FormatChecker, validators
import pandas as pd
from tqdm import tqdm_notebook


def basic_json_schema(data_source: str, items_numbers: List[int] = None) -> Schema:
    """Print a json schema based on the provided job_key and item numbers

    Args:
        data_source: a collection or job key
        items_numbers: array of item numbers to create schema from
    """
    schema = create_json_schema(data_source, items_numbers)
    return Schema(schema)


def create_json_schema(
    source_key: str, items_numbers: Optional[List[int]] = None
) -> Schema:
    if helpers.is_collection_key(source_key):
        store = api.get_collection(source_key)
        items_count = store.count()
        start_mask = ""
    elif helpers.is_job_key(source_key):
        items_count = api.get_items_count(api.get_job(source_key))
        start_mask = f"{source_key}/"
    else:
        raise ValueError(f"'{source_key}' is not a valid job or collection key")

    if items_count == 0:
        raise ValueError(f"'{source_key}' does not have any items")

    items_numbers = items_numbers or set_item_no(items_count)
    if max(items_numbers) >= items_count or min(items_numbers) < 0:
        raise ValueError(
            f"Expected values between 0 and {items_count}, got '{items_numbers}'"
        )

    samples = []
    for n in items_numbers:
        item = api.get_items(
            source_key, count=1, start_index=n, start=f"{start_mask}{n}", p_bar=None
        )[0]
        item.pop("_type", None)
        item.pop("_key", None)
        samples.append(item)

    return infer_schema(samples)


def infer_schema(samples: List[Dict[str, Any]]) -> Schema:
    builder = SchemaBuilder("http://json-schema.org/draft-07/schema#")
    for sample in samples:
        builder.add_object(sample)
    builder.add_schema(extension)

    return builder.to_schema()


def set_item_no(items_count: int) -> List[int]:
    """Generate random numbers within items_count range

    Returns:
        Random 4 numbers if items_count > 4 otherwise items numbers
    """
    if items_count <= 4:
        return [i for i in range(items_count)]
    return random.sample(range(0, items_count), 4)


def fast_validate(
    schema: Schema, raw_items: RawItems, keys: pd.Index
) -> Dict[str, set]:
    """Verify items one by one. It stops after the first error in an item in most cases.
    Faster than jsonschema validation

    Args:
        schema: a JSON schema
        raw_items: a raw data to validate one by one
        keys: keys corresponding to raw_items index

    Returns:
        A dictionary of errors with message and item keys
    """
    errors = defaultdict(set)

    validate = fastjsonschema.compile(schema)
    for i, raw_item in enumerate(
        tqdm_notebook(raw_items, desc="Fast Schema Validation")
    ):
        raw_item.pop("_type", None)
        raw_item.pop("_key", None)
        try:
            validate(raw_item)
        except fastjsonschema.JsonSchemaException as error:
            errors[str(error)].add(keys[i])
    return errors


def full_validate(
    schema: Schema, raw_items: RawItems, keys: pd.Index
) -> Dict[str, set]:
    """This function uses jsonschema validator which returns all found error per item.
    See `fast_validate()` for arguments descriptions.
    """
    errors = defaultdict(set)

    validator = validators.validator_for(schema)(schema)
    validator.format_checker = FormatChecker()
    for i, raw_item in enumerate(
        tqdm_notebook(raw_items, desc="JSON Schema Validation")
    ):
        raw_item.pop("_type", None)
        raw_item.pop("_key", None)
        for e in validator.iter_errors(raw_item):
            error = format_validation_message(
                e.message, e.path, e.schema_path, e.validator
            )
            errors[error].add(keys[i])
    return errors


def format_validation_message(
    error_msg: str, path: Deque, schema_path: Deque, validator: str
) -> str:
    str_path = "/".join(p for p in path if isinstance(p, str))
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
