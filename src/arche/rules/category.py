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
    warn_thr = 0.10
    err_thr = 0.20

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
        cat_difs = ((cats[source_key] - cats[target_key])).abs()
        cat_difs = cat_difs[cat_difs > warn_thr]
        cat_difs.name = f"Coverage difference more than {warn_thr:.0%} for {c}"
        if not cat_difs.empty:
            result.stats.append(cat_difs)
        errs = cat_difs[cat_difs > err_thr]
        if not errs.empty:
            result.add_warning(
                f"The difference is greater than {err_thr:.0%} for {len(errs)} value(s) of {c}"
            )

    return result


def get_coverage_per_category(df: pd.DataFrame, category_names: List):
    result = Result("Coverage For Scraped Categories")

    for c in category_names:
        value_counts = df[c].value_counts(ascending=True)
        result.add_info(f"{len(value_counts)} categories in '{c}'")
        result.stats.append(value_counts)
    return result
