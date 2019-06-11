from typing import List

from arche.rules.result import Outcome, Result
import pandas as pd


def get_difference(
    source_df: pd.DataFrame,
    target_df: pd.DataFrame,
    category_names: List[str],
    err_thr: float = 0.2,
    warn_thr: float = 0.1,
) -> Result:
    """Find and show differences between categories coverage, including `nan` values.
    Coverage means value counts divided on total size.

    Args:
        source_df: a data you want to compare
        target_df: a data you want to compare with
        category_names: list of columns which values to compare
        err_thr: sets error threshold
        warn_thr: warning threshold

    Returns:
        A result instance with messages containing significant difference defined by
        thresholds, a dataframe showing all normalized value counts in percents and
        a series containing significant difference.
    """
    source_key = "source"
    target_key = "target"
    result = Result("Category Coverage Difference")

    for c in category_names:
        cats = (
            pd.DataFrame(
                {
                    source_key: source_df[c].value_counts(dropna=False, normalize=True),
                    target_key: target_df[c].value_counts(dropna=False, normalize=True),
                }
            )
            .fillna(0)
            .sort_values(by=[source_key, target_key], kind="mergesort")
        )
        cats.name = f"Coverage for {c}"
        result.stats.append(cats)
        cat_difs = (cats[source_key] - cats[target_key]).abs()
        cat_difs = cat_difs[cat_difs > warn_thr]
        cat_difs.name = f"Coverage difference more than {warn_thr:.0%} for {c}"
        if not cat_difs.empty:
            result.stats.append(cat_difs)
        errs = cat_difs[cat_difs > err_thr]
        if not errs.empty:
            result.add_warning(
                f"The difference is greater than {err_thr:.0%} for {len(errs)} value(s) of {c}"
            )

    if not category_names:
        result.add_info(Outcome.SKIPPED)
    return result


def get_coverage_per_category(df: pd.DataFrame, category_names: List[str]) -> Result:
    """Get value counts per column, excluding nan.

    Args:
        df: a source data to assess
        category_names: list of columns which values counts to see

    Returns:
        Number of categories per field, value counts series for each field.
    """
    result = Result("Coverage For Scraped Categories")

    for c in category_names:
        value_counts = df[c].value_counts(ascending=True)
        result.add_info(f"{len(value_counts)} categories in '{c}'")
        result.stats.append(value_counts)
    if not category_names:
        result.add_info(Outcome.SKIPPED)
    return result


def get_categories(df: pd.DataFrame, max_uniques: int = 10) -> Result:
    """Find category columns. A category column is the column which holds a limited number
    of possible values, including `NAN`.

    Args:
        df: data
        max_uniques: filter which determines which columns to use. Only columns with
        the number of unique values less than or equal to `max_uniques` are category columns.

    Returns:
        A result with stats containing value counts.
    """
    result = Result("Categories")

    result.stats = [
        df[c].value_counts(dropna=False)
        for c in df
        if len(df[c].value_counts(dropna=False)) <= max_uniques
    ]
    if not result.stats:
        result.add_info("Categories were not found")
        return result
    result.add_info(f"{len(result.stats)} category field(s)")
    return result
