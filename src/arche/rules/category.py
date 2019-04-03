from typing import List

from arche.rules.result import Result
import pandas as pd


def get_difference(
    source_key: str,
    target_key: str,
    source_df: pd.DataFrame,
    target_df: pd.DataFrame,
    category_names: List,
):
    result = Result("Category Coverage Difference")

    for c in category_names:
        cats = pd.DataFrame(
            {
                "s": source_df[c].value_counts(normalize=True, dropna=False),
                "t": target_df[c].value_counts(normalize=True, dropna=False),
            }
        ).fillna(0)
        cat_difs = (
            ((cats["s"] - cats["t"]) * 100)
            .abs()
            .sort_index(kind="mergesort")
            .sort_values()
            .round(decimals=2)
        )
        cat_difs.name = (
            f"Coverage difference between {source_key}'s and {target_key}'s {c}"
        )
        cat_difs = cat_difs[cat_difs > 0]
        if cat_difs.empty:
            continue
        result.add_info(f"'{c}' PASSED", stats=cat_difs)
        errs = cat_difs[cat_difs > 20]
        if not errs.empty:
            result.add_warning(
                f"The difference is greater than 20% for {len(errs)} value(s) of {c}"
            )
    return result


def get_coverage_per_category(df: pd.DataFrame, category_names: List):
    result = Result("Coverage For Scraped Categories")

    for c in category_names:
        value_counts = df[c].value_counts(ascending=True)
        result.add_info(f"{len(value_counts)} categories in '{c}'", stats=value_counts)
    return result
