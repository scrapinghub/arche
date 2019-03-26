from typing import List

from arche.readers.schema import TaggedFields
from arche.rules.result import Result
import pandas as pd


def check_items(df: pd.DataFrame, tagged_fields: TaggedFields) -> Result:
    """Check for items with the same name and url"""

    name_fields = tagged_fields.get("name_field")
    url_fields = tagged_fields.get("product_url_field")
    result = Result("Duplicated Items")
    if not name_fields or not url_fields:
        result.add_info(
            "'name_field' and 'product_url_field' tags were not found in schema"
        )
    else:
        result.items_count = len(df)
        errors = {}
        name_field = name_fields[0]
        url_field = url_fields[0]
        df = df[[name_field, url_field, "_key"]]
        duplicates = df[df[[name_field, url_field]].duplicated(keep=False)]
        if duplicates.empty:
            return result

        result.err_items_count = len(duplicates)
        for _, d in duplicates.groupby([name_field, url_field]):
            msg = (
                f"same '{d[name_field].iloc[0]}' name and '{d[url_field].iloc[0]}' url"
            )
            errors[msg] = list(d["_key"])
        result.add_error(
            f"{len(duplicates)} duplicate(s) with same name and url", errors=errors
        )
    return result


def check_uniqueness(df: pd.DataFrame, tagged_fields: TaggedFields) -> Result:
    """Verify if each item field tagged with `unique` is unique.

    Returns:
        A result containing field names and keys for non unique items
    """
    unique_fields = tagged_fields.get("unique", [])
    result = Result("Uniqueness")

    if not unique_fields:
        result.add_info("'unique' tag was not found in schema")
        return result

    err_keys = set()
    for field in unique_fields:
        result.items_count = df[field].count()
        duplicates = df[df[field].duplicated(keep=False)][[field, "_key"]]
        errors = {}
        for _, d in duplicates.groupby([field]):
            keys = list(d["_key"])
            msg = f"same '{d[field].iloc[0]}' {field}"
            errors[msg] = keys
            err_keys = err_keys.union(keys)
        if not duplicates.empty:
            result.add_error(
                f"'{field}' contains {len(duplicates[field].unique())} duplicated value(s)",
                errors=errors,
            )

    result.err_items_count = len(err_keys)
    return result


def find_by(df: pd.DataFrame, columns: List[str]) -> Result:
    """Compare items rows in `df` by `columns`

    Returns:
        Any duplicates
    """
    result = Result("Items Uniqueness By Columns")
    result.items_count = len(df)
    df = df.dropna(subset=columns, how="all")
    duplicates = df[df[columns].duplicated(keep=False)][columns + ["_key"]]
    if duplicates.empty:
        return result

    result.err_items_count = len(duplicates)
    errors = {}
    for _, d in duplicates.groupby(columns):
        msg = "same"
        for c in columns:
            msg = f"{msg} '{d[c].iloc[0]}' {c}"
        errors[msg] = list(d["_key"])

    result.add_error(
        f"{len(duplicates)} duplicate(s) with same {', '.join(columns)}", errors=errors
    )
    return result
