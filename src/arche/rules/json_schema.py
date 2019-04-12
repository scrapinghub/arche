from arche.readers.schema import Schema, Tag, TaggedFields
from arche.rules.result import Result
from arche.tools.api import Items
from arche.tools.json_schema_validator import JsonSchemaValidator
import numpy as np


def validate(schema: Schema, items_dicts: Items, fast: bool = False) -> Result:
    """Run JSON schema validation against Items.

    Args:
        fast: defines if we use fastjsonschema or jsonschema validation
    """
    validator = JsonSchemaValidator(schema)
    validator.run(items_dicts, fast)
    result = Result("JSON Schema Validation")

    errors = validator.errors
    schema_result_message = (
        f"{len(items_dicts)} items were checked, {len(errors)} error(s)"
    )

    if errors:
        result.add_error(schema_result_message, errors=errors)
    else:
        result.add_info(schema_result_message)
    return result


def check_tags(
    source_columns: np.ndarray, target_columns: np.ndarray, tags: TaggedFields
) -> Result:
    result = Result("Tags")

    found_tags = sorted(list(tags))
    if found_tags:
        result.add_info(f"Used - {', '.join(found_tags)}")

    all_tags = set([name for name, _ in Tag.__members__.items()])
    not_used_tags = sorted(all_tags - set(tags))
    if not_used_tags:
        result.add_info(f"Not used - {', '.join(not_used_tags)}")

    tagged_fields = []
    for tag in tags:
        tagged_fields.extend(tags[tag])

    missing_in_source = sorted(set(tagged_fields) - set(source_columns))
    if missing_in_source:
        result.add_error(
            f"{str(missing_in_source)[1:-1]} field(s) was not found in "
            "source, but specified in schema"
        )

    if target_columns is not None:
        missing_in_target = sorted(set(tagged_fields) - set(target_columns))
        if missing_in_target:
            result.add_error(
                f"{str(missing_in_target)[1:-1]} field(s) was not found "
                "in target, but specified in schema"
            )

    if result.errors:
        result.add_error("Skipping tag rules")

    return result
