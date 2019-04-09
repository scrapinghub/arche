import re

from arche.readers.items import Items
from arche.rules.result import Result
import numpy as np
import pandas as pd


def compare_boolean_fields(source_df: pd.DataFrame, target_df: pd.DataFrame):
    source_bool = source_df.select_dtypes(include="bool")
    target_bool = target_df.select_dtypes(include="bool")

    result = Result("Boolean Fields")
    if not fields_to_compare(source_bool, target_bool):
        result.add_info("SKIPPED")
        return result

    lefts = []
    for c in source_bool:
        lefts.append(source_bool[c].value_counts(normalize=True).add_prefix(f"{c}_"))
    b_ = pd.concat(lefts)

    rights = []
    for c in target_bool:
        rights.append(target_bool[c].value_counts(normalize=True).add_prefix(f"{c}_"))
    s_ = pd.concat(rights)
    bool_covs = pd.DataFrame({"source": b_, "target": s_}).fillna(0) * 100
    bool_covs.name = "Coverage for boolean fields"
    result.stats.append(bool_covs)

    difs = (bool_covs["source"] - bool_covs["target"]).abs()

    err_diffs = difs[difs > 10]
    if not err_diffs.empty:
        result.add_error(
            f"{', '.join(err_diffs.index)} relative frequencies differ "
            "by more than 10%"
        )

    warn_diffs = difs[(difs <= 10) & (difs > 5)]
    if not warn_diffs.empty:
        result.add_warning(
            f"{', '.join(warn_diffs.index)} relative frequencies differ by 5-10%"
        )
    if err_diffs.empty and warn_diffs.empty:
        result.add_info("PASSED")

    return result


def fields_to_compare(source_df, target_df):
    source_fields = source_df.columns.values
    target_fields = target_df.columns.values
    if (
        source_fields.size > 0
        and target_fields.size > 0
        and set(source_fields).intersection(target_fields)
    ):
        return True
    return False


def garbage_symbols(items: Items) -> Result:
    """Find unwanted symbols in `np.object` columns.

    Returns:
        A result containing item keys per field which contained any trash symbol
    """
    garbage = (
        r"(?P<spaces>^\s|\s$)"
        r"|(?P<html_entities>&amp|&reg)"
        r"|(?P<css>(?:(?:\.|#)[^#. ]+\s*){.+})"
        r"|(?P<html_tags></?(h\d|b|u|i|div|ul|ol|li|table|tbody|th|tr|td|p|a|br|img|sup|SUP|"
        r"blockquote)\s*/?>|<!--|-->)"
    )

    errors = {}
    row_keys = set()
    rule_result = Result("Garbage Symbols", items_count=items.size)

    for column in items.flat_df.select_dtypes([np.object]):
        matches = items.flat_df[column].str.extractall(garbage, flags=re.IGNORECASE)
        matches = matches[["spaces", "html_entities", "css", "html_tags"]]
        if not matches.empty:
            error_keys = items.flat_df.iloc[matches.unstack().index.values]["_key"]
            original_column = items.get_origin_column_name(column)
            bad_texts = matches.stack().value_counts().index.sort_values().tolist()
            error = (
                f"{len(error_keys)/items.size*100:.1f}% of '{original_column}' "
                f"values contain {[t[:20] for t in bad_texts]}"
            )
            errors[error] = list(error_keys)
            row_keys = row_keys.union(error_keys)

    if errors:
        rule_result.add_error(
            f"{len(row_keys)/items.size * 100:.1f}% ({len(row_keys)}) items affected",
            errors=errors,
        )
        rule_result.err_items_count = len(row_keys)

    return rule_result
