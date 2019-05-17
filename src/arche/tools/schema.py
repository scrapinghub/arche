from dataclasses import dataclass
import json
import pprint
import random
from typing import Any, Dict, List, Optional

from arche.readers.schema import Schema
from arche.schema_definitions import extension
from arche.tools import api, helpers
from genson import SchemaBuilder


@dataclass
class BasicSchema:
    d: Schema

    def json(self):
        print(json.dumps(self.d, indent=4))

    def __repr__(self):
        return pprint.pformat(self.d)


def basic_json_schema(data_source: str, items_numbers: List[int] = None) -> BasicSchema:
    """Print a json schema based on the provided job_key and item numbers

    Args:
        data_source: a collection or job key
        items_numbers: array of item numbers to create schema from
    """
    schema = create_json_schema(data_source, items_numbers)
    return BasicSchema(schema)


def create_json_schema(
    source_key: str, item_numbers: Optional[List[int]] = None
) -> Schema:
    if helpers.is_collection_key(source_key):
        store = api.get_collection(source_key)
        items_count = store.count()
    elif helpers.is_job_key(source_key):
        items_count = api.get_items_count(api.get_job(source_key))
    else:
        raise ValueError(f"'{source_key}' is not a valid job or collection key")

    if items_count == 0:
        raise ValueError(f"'{source_key}' does not have any items")

    item_n_err = "{} is a bad item number, choose numbers between 0 and {}"
    if item_numbers:
        item_numbers.sort()
        if item_numbers[-1] >= items_count or item_numbers[0] < 0:
            raise ValueError(item_n_err.format(item_numbers[-1], items_count - 1))
    else:
        item_numbers = set_item_no(items_count)

    samples = []
    for n in item_numbers:
        item = api.get_items(source_key, start_index=n, count=1, p_bar=None)[0]
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
