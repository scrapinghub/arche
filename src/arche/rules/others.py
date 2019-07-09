import codecs
import re

from arche.rules.result import Outcome, Result
import numpy as np
import pandas as pd
from tqdm import tqdm_notebook


def compare_boolean_fields(
    source_df: pd.DataFrame,
    target_df: pd.DataFrame,
    err_thr: float = 0.10,
    warn_thr: float = 0.05,
) -> Result:
    """Compare booleans distribution between two dataframes

    Returns:
        A result containing dataframe with distributions and messages if differences
        are in thresholds
    """

    source_bool = source_df.select_dtypes(include="bool")
    target_bool = target_df.select_dtypes(include="bool")

    result = Result("Boolean Fields")
    if not fields_to_compare(source_bool, target_bool):
        result.add_info(Outcome.SKIPPED)
        return result

    dummy = pd.DataFrame(columns=[True, False])
    source_counts = pd.concat(
        [dummy, source_bool.apply(pd.value_counts, normalize=True).T], sort=False
    ).fillna(0.0)
    target_counts = pd.concat(
        [dummy, target_bool.apply(pd.value_counts, normalize=True).T], sort=False
    ).fillna(0.0)
    difs = (source_counts - target_counts)[True]

    bool_covs = pd.concat(
        [
            source_counts.rename("{}_source".format),
            target_counts.rename("{}_target".format),
        ]
    ).sort_index()
    bool_covs.name = "Coverage for boolean fields"
    result.stats.append(bool_covs)

    err_diffs = difs[difs.abs() > err_thr]
    if not err_diffs.empty:
        result.add_error(
            f"{', '.join(err_diffs.index)} relative frequencies differ "
            f"by more than {err_thr:.0%}"
        )

    warn_diffs = difs[(difs.abs() > warn_thr) & (difs.abs() <= err_thr)]
    if not warn_diffs.empty:
        result.add_warning(
            f"{', '.join(warn_diffs.index)} relative frequencies differ by "
            f"{warn_thr:.0%}-{err_thr:.0%}"
        )

    return result


def fields_to_compare(source_df: pd.DataFrame, target_df: pd.DataFrame) -> bool:
    source_fields = source_df.columns.values
    target_fields = target_df.columns.values
    if (
        source_fields.size > 0
        and target_fields.size > 0
        and set(source_fields).intersection(target_fields)
    ):
        return True
    return False


def garbage_symbols(df: pd.DataFrame) -> Result:
    """Find unwanted symbols in `np.object` columns.

    Returns:
        A result containing item keys per field which contained any trash symbol
    """
    garbage = (
        r"(?P<spaces>^\s|\s$)"
        r"|(?P<html_entities>&[a-zA-Z]{2,}?;|&#\d*?;)"
        r"|(?P<css>[.#@][^\d{}#.\s][^{}#.]+?{(?:[^:;{}]+?:[^:;{}]+?;)+?\s*?})"
        r"|(?P<html_tags></??(?:h\d|b|u|i|div|ul|ol|li|table|tbody|th|tr|td|p|a|br|img|sup|SUP|"
        r"blockquote)\s*?/??>|<!--|-->)"
    )

    errors = {}
    row_keys = set()
    rule_result = Result("Garbage Symbols", items_count=len(df))

    for column in tqdm_notebook(
        df.select_dtypes([np.object]).columns, desc="Garbage Symbols"
    ):
        matches = df[column].apply(str).str.extractall(garbage, flags=re.IGNORECASE)
        if not matches.empty:
            error_keys = df.loc[matches.unstack().index.values].index
            bad_texts = matches.stack().value_counts().index.sort_values().tolist()
            # escape backslashes for markdown repr, `\n > \\n`
            bad_texts = [
                f"'{codecs.encode(bx, 'unicode_escape').decode()[:20]}'"
                for bx in bad_texts
            ]
            error = (
                f"{len(error_keys)/len(df)*100:.1f}% of '{column}' "
                f"values contain `{', '.join(bad_texts)}`"
            )

            errors[error] = list(error_keys)
            row_keys = row_keys.union(error_keys)
    if errors:
        rule_result.add_error(
            f"{len(row_keys)/len(df) * 100:.1f}% ({len(row_keys)}) items affected",
            errors=errors,
        )
        rule_result.err_items_count = len(row_keys)

    return rule_result
