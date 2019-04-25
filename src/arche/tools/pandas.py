from typing import Dict, Optional, Tuple
import uuid

import pandas as pd
import json
from tqdm import tqdm_notebook


# check is the column containing lists
def is_list(df, column):
    for cell in df[column]:
        if isinstance(cell, list):
            return True
            break
    return False


# check is the column containing maps
def is_map(df, column):
    for cell in df[column]:
        if isinstance(cell, dict):
            return True
            break
    return False


# list columns by type
def listColumns(df):
    list_col = []
    map_col = []
    for col in df.columns:
        if is_map(df, col):
            map_col.append(col)
        if is_list(df, col):
            list_col.append(col)
    normal = set(df.columns) - set(list_col) - set(map_col)
    return normal, list_col, map_col


# flatten map columns one level
def flat_map_columns(df, map_cols):
    json_struct = json.loads(df.to_json(orient="records"))
    flat_df = pd.io.json.json_normalize(json_struct, sep="_")
    flat_df = flat_df.drop(map_cols, axis=1)
    return flat_df


# flatten map columns one level with apply - slower
def flat_map_list(df, col):
    return df[col].apply(pd.Series)


# flatten list columns one level
def flat_list_columns(df, col):
    s = df[col].dropna().index
    col_flat_df = pd.DataFrame(df[col].dropna().values.tolist(), index=s)
    col_flat_df.columns = [str(col) + "_" + str(coln) for coln in col_flat_df.columns]
    return col_flat_df


def df_column_uniquify(df):
    if isinstance(df, tuple):
        return df
    else:
        df_columns = df.columns
        new_columns = []
        for item in df_columns:
            counter = 0
            newitem = item
            while newitem in new_columns:
                counter += 1
                newitem = f"{item}_{uuid.uuid1().hex[:5]}" #"{}_{}".format(item, counter)

            new_columns.append(newitem)
        df.columns = new_columns
        return df

# flatten one level
def flatten_one(df):
    [cols, lists, maps] = listColumns(df)
    df_cols = df[cols]
    frames = [df_cols]
    for l in lists:
        frames.append(flat_list_columns(df, l))

    df_maps = flat_map_columns(df[maps], maps)
    frames.append(df_maps)
    result = pd.concat(frames, axis=1, sort=False)
    return result


# flatten all levels
def flatten_all(df):
    res = flatten_one(df)
    print(res.shape)
    [cols, lists, maps] = listColumns(res)
    if not lists and not maps:
        return res
    else:
        res = flatten_df(res)
        return res


# return map of derived columns of the flatten df
def get_columns(df, flat_df):
    cm = {}
    df_columns = df.columns
    flat_df_columns = set(flat_df.columns) - set(df_columns)
    for col_flat in flat_df_columns:
        for col in df_columns:
            if col in col_flat:
                cm[col_flat] = col
    return cm


def flatten_df(df: pd.DataFrame,) -> Tuple[pd.DataFrame, Dict[str, str]]:
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
    flat_df = flatten_all(df)
    df_column_uniquify(flat_df)
    column_map = get_columns(df, flat_df)
    return flat_df, column_map
