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
        cats = (
            pd.DataFrame(
                {
                    source_key: source_df[c]
                    .value_counts(dropna=False, normalize=True)
                    .mul(100)
                    .round(decimals=2),
                    target_key: target_df[c]
                    .value_counts(dropna=False, normalize=True)
                    .mul(100)
                    .round(decimals=2),
                }
            )
            .fillna(0)
            .sort_values(by=[source_key, target_key], kind="mergesort")
        )
        cats.name = f"Coverage for {c}"
        result.stats.append(cats)
        cat_difs = ((cats[source_key] - cats[target_key])).abs()
        cat_difs = cat_difs[cat_difs > 10]
        cat_difs.name = f"Coverage difference more than 10% for {c}"
        if not cat_difs.empty:
            result.stats.append(cat_difs)
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
        result.add_info(f"{len(value_counts)} categories in '{c}'")
        result.stats.append(value_counts)
    return result
