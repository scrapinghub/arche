from typing import List

from arche.readers.schema import TaggedFields
from arche.rules.result import Result, Outcome
import pandas as pd


def find_by_unique(df: pd.DataFrame, tagged_fields: TaggedFields) -> Result:
    """Verify if each item field tagged with `unique` is unique.

    Returns:
        A result containing field names and keys for non unique items
    """
    unique_fields = tagged_fields.get("unique", [])
    result = Result("Duplicates By **unique** Tag")

    if not unique_fields:
        result.add_info(Outcome.SKIPPED)
        return result

    err_keys = set()
    for field in unique_fields:
        result.items_count = df[field].count()
        duplicates = df[df.duplicated(field, keep=False)][[field]]
        errors = {}
        for _, d in duplicates.groupby([field]):
            keys = list(d.index)
            msg = f"same '{d[field].iloc[0]}' `{field}`"
            errors[msg] = keys
            err_keys = err_keys.union(keys)
        if not duplicates.empty:
            result.add_error(
                f"{field} contains {len(duplicates[field].unique())} duplicated value(s)",
                errors=errors,
            )

    result.err_items_count = len(err_keys)
    return result


def find_by(df: pd.DataFrame, columns: List[str]) -> Result:
    """Compare items rows in `df` by `columns`

    Returns:
        Any duplicates
    """
    result = Result(f"Duplicates")
    result.items_count = len(df)
    df = df.dropna(subset=columns, how="all")
    duplicates = df[df.duplicated(columns, keep=False)][columns]
    if duplicates.empty:
        return result

    result.err_items_count = len(duplicates)
    errors = {}
    for _, d in duplicates.groupby(columns):
        msgs = [f"'{d[c].iloc[0]}' `{c}`" for c in columns]
        errors[f"same {', '.join(msgs)}"] = list(d.index)

    result.add_error(
        f"{len(duplicates)} duplicate(s) with same {', '.join(columns)}", errors=errors
    )
    return result


def find_by_name_url(df: pd.DataFrame, tagged_fields: TaggedFields) -> Result:
    """Check for items with the same name and url"""

    name_fields = tagged_fields.get("name_field")
    url_fields = tagged_fields.get("product_url_field")
    name = "Duplicates By **name_field, product_url_field** Tags"
    result = Result(name)
    if not name_fields or not url_fields:
        result.add_info(Outcome.SKIPPED)
        return result
    name_field = name_fields[0]
    url_field = url_fields[0]
    result = find_by(df, [name_field, url_field])
    result.name = name
    return result
