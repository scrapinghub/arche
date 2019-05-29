from typing import List

from arche.rules.result import Outcome, Result
import pandas as pd
import plotly.graph_objs as go


def get_difference(
    source_df: pd.DataFrame,
    target_df: pd.DataFrame,
    category_names: List[str],
    source_key: str = "source",
    target_key: str = "target",
) -> Result:
    """Find and show differences between categories coverage, including nan values.
    Coverage means value counts divided on total size.

    Args:
        source_df: a data you want to compare
        target_df: a data you want to compare with
        category_names: list of columns which values to compare
        source_key: label for `source_df`
        target_key: label for `target_df`

    Returns:
        A result instance with messages containing significant difference defined by
        thresholds, a dataframe showing all normalized value counts in percents,
        a series containing significant difference.
    """
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


def get_categorical_data(df: pd.DataFrame) -> Result:
    result = Result("Categories")
    data = []
    cat_df = df.select_dtypes(["category"])
    if cat_df.empty:
        result.add_info("Categorical data was not found")
        return result
    colors = [
        "rgb(55, 128, 191)",
        "rgb(219, 85, 108)",
        "rgb(119, 164, 82)",
        "rgb(255, 153, 102)",
        "rgb(122, 120, 168)",
        "rgb(204, 153, 0)",
        "rgb(58, 71, 80)",
        "rgb(10, 200, 225)",
        "rgb(40, 171, 0)",
        "rgb(222, 10, 10)",
    ]
    for c in cat_df:
        vc = df[c].value_counts(dropna=False)
        data.extend(
            [
                go.Bar(
                    x=[value],
                    y=[vc.name],
                    name=index,
                    orientation="h",
                    opacity=0.7,
                    marker=dict(color=colors[i]),
                )
                for i, (index, value) in enumerate(vc.items())
            ]
        )
    layout = go.Layout(
        title="Fields with less than 11 unique values",
        bargap=0.1,
        barmode="stack",
        template="seaborn",
        height=min(len(cat_df.columns) * 20, 900),
        hovermode="y",
        margin=dict(l=200, t=35),
        showlegend=False,
    )

    result._figures = [go.FigureWidget(data, layout)]
    result.add_info(f"{len(cat_df.columns)} categorical fields")
    return result
