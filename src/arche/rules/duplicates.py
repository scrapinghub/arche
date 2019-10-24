from collections import Iterable
from typing import Any, Generator, List, Union

from arche.readers.schema import TaggedFields
from arche.rules.result import Result, Outcome
import pandas as pd


def find_by(df: pd.DataFrame, uniques: List[Union[str, List[str]]]) -> Result:
    """Find equal items rows in `df` by `uniques`. I.e. if two items have the same
    uniques's element value, they are considered duplicates.

    Args:
        uniques: list containing columns and list of columns to identify duplicates.
        List of columns means that all list columns values should be equal.

    Returns:
        Any duplicates
    """
    result = Result("Duplicates")
    result.items_count = len(df)

    df = df.dropna(subset=list(set(flatten(uniques))), how="all")
    for columns in uniques:
        mask = columns if isinstance(columns, list) else [columns]
        duplicates = df[df.duplicated(columns, keep=False)][mask]
        if duplicates.empty:
            continue

        errors = {}
        grouped = duplicates.groupby(columns)
        for _, d in grouped:
            msgs = [f"'{d[c].iloc[0]}' `{c}`" for c in mask]
            errors[f"same {', '.join(msgs)}"] = list(d.index)
        result.add_error(
            f"{', '.join(mask)} contains {len(grouped)} duplicated value(s)",
            errors=errors,
        )
    return result


def find_by_tags(df: pd.DataFrame, tagged_fields: TaggedFields) -> Result:
    """Check for duplicates based on schema tags. In particular, look for items with
    the same `name_field` and `product_url_field`, and for uniqueness among `unique` field"""

    name_fields = tagged_fields.get("name_field")
    url_fields = tagged_fields.get("product_url_field")
    columns_to_check: List = tagged_fields.get("unique", [])
    if (not name_fields or not url_fields) and not columns_to_check:
        result = Result("Duplicates")
        result.add_info(Outcome.SKIPPED)
        return result
    if name_fields and url_fields:
        columns_to_check.extend([[name_fields[0], url_fields[0]]])

    return find_by(df, columns_to_check)


def flatten(l: Any) -> Generator[str, None, None]:
    for el in l:
        if isinstance(el, Iterable) and not isinstance(el, str):
            yield from flatten(el)
        else:
            yield el
