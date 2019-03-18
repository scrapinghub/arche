import re

from arche.readers.items import Items
from arche.rules.result import Result
import numpy as np


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
