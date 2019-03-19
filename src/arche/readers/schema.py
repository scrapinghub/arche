from collections import defaultdict
from enum import Enum
import json
from typing import Dict, List, Optional, Union
from urllib.parse import urlparse
from urllib.request import urlopen

from arche.tools import maintenance, s3
import perfect_jsonschema

EXTENDED_KEYWORDS = {"tag", "unique", "coverage_percentage"}

SchemaObject = Dict[str, Union[str, bool, int, float, None, List]]
Schema = Dict[str, SchemaObject]
SchemaSource = Union[str, Schema]
TaggedFields = Dict[str, List[str]]


def get_schema(schema_source: Optional[SchemaSource]):
    if isinstance(schema_source, str):
        schema_source = get_schema_from_url(schema_source)

    if isinstance(schema_source, dict):
        perfect_jsonschema.check(schema_source, EXTENDED_KEYWORDS)
        return schema_source
    else:
        raise ValueError(
            f"{json.dumps(str(schema_source), indent=4)} is an unidentified schema source.\n"
            f"A dict, a full s3 path or URL is expected"
        )


def get_schema_from_url(path: str) -> Schema:
    o = urlparse(path)
    netloc = o.netloc
    relative_path = o.path.lstrip("/")
    if not netloc or not relative_path:
        raise ValueError(f"'{path}' is not an s3 path or URL to a schema")
    if o.scheme == "s3":
        return json.loads(s3.get_contents_as_string(netloc, relative_path))
    else:
        return json.loads(get_contents(path))


def get_contents(url: str):
    with urlopen(url) as f:
        return f.read().decode("utf-8")


class Tag(Enum):
    unique = (0,)
    category = (1,)
    category_field = (1,)
    name_field = (2,)
    product_url_field = (3,)
    product_price_field = (4,)
    product_price_was_field = (5,)


class JsonFields:
    tags = set([name for name, _ in Tag.__members__.items()])

    def __init__(self, schema):
        self.schema = schema
        self.tagged = defaultdict(list)
        self.get_tags()

    def get_tags(self):
        if "properties" not in self.schema:
            raise ValueError("The schema does not have 'properties'")

        for key, value in self.schema["properties"].items():
            property_tags = value.get("tag", [])
            if property_tags:
                self.get_field_tags(property_tags, key)

    def get_field_tags(self, tags, field):
        tags = self.parse_tag(tags)
        if not tags:
            raise ValueError(
                f"'{tags}' tag value is invalid, should be str or list[str]"
            )

        invalid_tags = tags - self.tags
        if invalid_tags:
            raise ValueError(
                f"{invalid_tags} tag(s) are unsupported, valid tags are:\n"
                f"{', '.join(sorted(list(self.tags)))}"
            )
            tags = tags - invalid_tags

        for tag in tags:
            if tag == "category_field":
                tag = "category"
                maintenance.deprecate(
                    "'category_field' tag was deprecated in 2019.03.11 and "
                    "will be removed in 2019.04.01.",
                    replacement="Use 'category' instead",
                    gone_in="2019.04.01",
                )
            self.tagged[tag].append(field)

    @staticmethod
    def parse_tag(value):
        if isinstance(value, str):
            return set([value])
        if isinstance(value, list):
            return set(value)
        return None
