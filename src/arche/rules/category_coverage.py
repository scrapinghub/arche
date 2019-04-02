from arche.readers.schema import TaggedFields
from arche.rules.result import Result
import pandas as pd


def get_difference(source_key, target_key, source_df, target_df, tagged_fields):
    result = Result("Category Coverage Difference")
    fields = tagged_fields.get("category")

    for f in fields:
        cats = pd.DataFrame(
            {
                "s": source_df[f].value_counts(normalize=True),
                "t": target_df[f].value_counts(normalize=True),
            }
        ).fillna(0)
        cat_difs = ((cats["s"] - cats["t"]) * 100).abs().sort_values().round(decimals=2)
        cat_difs.name = (
            f"Coverage difference between {source_key}'s and {target_key}'s {f}"
        )
        result.add_info("", stats=cat_difs[cat_difs > 0])
    return result


def get_coverage_per_category(df: pd.DataFrame, tagged_fields: TaggedFields):
    result = Result("Coverage For Scraped Categories")

    category_fields = tagged_fields.get("category", [])
    for f in category_fields:
        value_counts = df[f].value_counts(ascending=True)
        result.add_info(f"{len(value_counts)} categories in '{f}'", stats=value_counts)
    return result
