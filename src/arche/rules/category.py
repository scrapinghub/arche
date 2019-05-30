from typing import List

from arche.rules.result import Outcome, Result
import pandas as pd
from plotly.colors import DEFAULT_PLOTLY_COLORS
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


def get_categories(
    df: pd.DataFrame, max_uniques: int = 10, max_uniques_ratio: float = 0.5
) -> Result:
    """Create a stacked bar chart showing categories including `NAN`. Categories
    are determined by `max_uniques` values. To be such column, it needs to have the size of
    value counts less or equal than `max_uniques`.

    Args:
        df: data
        max_uniques: filter which determines which columns to use. Only columns with
        the number of unique values less or equal than `max_uniques` are used.
        max_uniques_ratio: determines `df` with what ratio of maximum uniques
        to consider.

    Returns:
        A result with figure containing all plots.
    """
    result = Result("Categories")
    if df.empty or max_uniques / len(df) > max_uniques_ratio:
        result.add_info("Categorical data was not found")
        return result

    v_counts = [
        df[c].value_counts(dropna=False)
        for c in df
        if len(df[c].value_counts(dropna=False)) <= max_uniques
    ]
    data = []
    for vc in v_counts:
        data.extend(
            [
                go.Bar(
                    x=[value],
                    y=[vc.name],
                    name=index,
                    orientation="h",
                    opacity=0.6,
                    marker_color=DEFAULT_PLOTLY_COLORS[i % 10],
                )
                for i, (index, value) in enumerate(vc.items())
            ]
        )

    layout = go.Layout(
        title=f"Fields with less than {max_uniques + 1} unique values",
        bargap=0.1,
        barmode="stack",
        template="seaborn",
        height=min(len(df.columns) * 20, 900),
        hovermode="y",
        margin=dict(l=200, t=35),
        showlegend=False,
    )

    result._figures = [go.FigureWidget(data, layout)]
    result.add_info(f"{len(df.columns)} categorical field(s)")
    return result
