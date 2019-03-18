from typing import Dict, Optional, Tuple
import uuid

import pandas as pd
from tqdm import tqdm_notebook


def flatten_df(
    df: pd.DataFrame,
    i: int = 0,
    columns_map: Optional[Dict[str, str]] = None,
    p_bar: Optional[tqdm_notebook] = None,
) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """Expand lists and dicts to new columns named after list element number
    or dict key and containing respective cell values. If new name conflicts
    with an existing column, a short hash is used.
    Almost as fast as json_normalize but supports lists.

    Args:
        df: a dataframe to expand
        i: start index of columns slice, since there's no need to iterate
        twice over completely expanded column
        columns_map: a dict with old name references {new_name: old}
        p_bar: a progress bar

    Returns:
        A flat dataframe with new columns from expanded lists and dicts
        and a columns map dict with old name references {new_name: old}

    Examples:

    >>> df = pd.DataFrame({"links": [[{"im": "http://www.im.com/illinoi"},
    ...                               {"ITW website": "http://www.itw.com"}]]})

    >>> flat_df, cols_map = flatten_df(df)
    >>> flat_df
                      links_0_im links_1_ITW website
    0  http://www.im.com/illinoi  http://www.itw.com

    >>> cols_map
    {'links_0_im': 'links', 'links_1_ITW website': 'links'}
    """
    if not columns_map:
        columns_map = {}
    if not p_bar:
        p_bar = tqdm_notebook(
            total=len(df.columns), desc="Flattening df", unit="columns"
        )

    for c in df.columns[i:]:
        flattened_columns = expand_column(df, c)
        if flattened_columns.empty:
            i += 1
            p_bar.update(1)
            continue

        def name_column(x):
            new_name = f"{c}_{x}"
            if new_name in df.columns:
                new_name = f"{c}_{uuid.uuid1().hex[:5]}"

            if c in columns_map:
                columns_map[new_name] = columns_map[c]
            else:
                columns_map[new_name] = c
            return new_name

        flattened_columns = flattened_columns.rename(columns=name_column)
        df = pd.concat([df[:], flattened_columns[:]], axis=1).drop(c, axis=1)
        columns_map.pop(c, None)
        p_bar.total = len(df.columns)
        return flatten_df(df, i, columns_map, p_bar)
    return df, columns_map


def expand_column(df: pd.DataFrame, column: str) -> pd.DataFrame:
    mask = df[column].map(lambda x: (isinstance(x, list) or isinstance(x, dict)))
    collection_column = df[mask][column]
    return collection_column.apply(pd.Series)
