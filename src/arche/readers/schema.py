from collections import defaultdict
from enum import Enum
import json
import pprint
from typing import Dict, List, Union, Any, Set, DefaultDict

from arche.tools import s3
import perfect_jsonschema

EXTENDED_KEYWORDS = {"tag", "unique", "coverage_percentage"}

SchemaObject = Dict[str, Union[str, bool, int, float, None, List]]
RawSchema = Dict[str, SchemaObject]
SchemaSource = Union[str, RawSchema]
TaggedFields = Dict[str, List[str]]


class Tag(Enum):
    unique = (0,)
    category = (1,)
    name_field = (2,)
    product_url_field = (3,)
    product_price_field = (4,)
    product_price_was_field = (5,)


class Schema:
    allowed_tags = set([name for name, _ in Tag.__members__.items()])

    def __init__(self, source: SchemaSource):
        self.raw: RawSchema = self.read(source)
        if not self.raw.get("properties", None):
            raise ValueError("The schema does not have any 'properties'")
        self.enums: List[str] = self.get_enums()
        self.tags = self.get_tags(self.raw)

    def json(self):
        print(json.dumps(self.raw, indent=4))

    def __repr__(self):
        return pprint.pformat(self.raw)

    def get_enums(self) -> List[str]:
        enums: List[str] = []
        for k, v in self.raw["properties"].items():
            if isinstance(v, Dict) and "enum" in v.keys():
                enums.append(k)
        return enums

    @staticmethod
    def get_tags(schema: RawSchema) -> TaggedFields:
        tagged_fields: DefaultDict[str, List[str]] = defaultdict(list)
        for key, value in schema["properties"].items():
            if isinstance(value, Dict):
                property_tags = value.get("tag")
                if property_tags:
                    tagged_fields = Schema.get_field_tags(
                        property_tags, key, tagged_fields
                    )
        return dict(tagged_fields)

    @classmethod
    def get_field_tags(
        cls, tags: Set[Any], field: str, tagged_fields: DefaultDict
    ) -> DefaultDict[str, List[str]]:
        tags = cls.parse_tag(tags)
        if not tags:
            raise ValueError(
                f"'{tags}' tag value is invalid, should be str or list[str]"
            )

        invalid_tags = tags - cls.allowed_tags
        if invalid_tags:
            raise ValueError(
                f"{invalid_tags} tag(s) are unsupported, valid tags are:\n"
                f"{', '.join(sorted(list(cls.allowed_tags)))}"
            )

        for tag in tags:
            tagged_fields[tag].append(field)
        return tagged_fields

    @staticmethod
    def parse_tag(value):
        if isinstance(value, str):
            return set([value])
        if isinstance(value, list):
            return set(value)
        return None

    @staticmethod
    def read(schema_source: SchemaSource) -> RawSchema:
        if isinstance(schema_source, str):
            schema_source = Schema.from_url(schema_source)

        if isinstance(schema_source, dict):
            perfect_jsonschema.check(schema_source, EXTENDED_KEYWORDS)
            return schema_source
        else:
            raise ValueError(
                f"{json.dumps(str(schema_source), indent=4)} is an unidentified schema source."
                f"\nA dict, a full s3 path or URL is expected"
            )

    @staticmethod
    def from_url(path: str) -> RawSchema:
        return json.loads(s3.get_contents(path))
