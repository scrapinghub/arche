from collections import OrderedDict

from arche.readers.schema import TaggedFields
from arche.rules.result import Result
from arche.tools.helpers import ratio_diff
import pandas as pd


def compare_coverage_per_category(
    source_key, target_key, source_df, target_df, tagged_fields
):
    result = Result("Compare Scraped Categories")
    tolerance = 10
    category_fields = tagged_fields.get("category")
    if not category_fields:
        result.add_info("category_field tag was not found inside the schema")
        return result
    category_field = category_fields[0]

    source_coverages = OrderedDict(
        source_df.groupby(category_field)[category_field]
        .count()
        .sort_values(ascending=False)
    )
    target_coverages = OrderedDict(
        target_df.groupby(category_field)[category_field]
        .count()
        .sort_values(ascending=False)
    )

    diff_category_coverages = []
    same_categories = OrderedDict()
    missing_categories = OrderedDict()
    new_categories = OrderedDict()
    detailed_messages = []
    for category, coverage in source_coverages.items():
        if category in target_coverages:
            same_categories[category] = [target_coverages.get(category), coverage]
            if coverage != target_coverages.get(category):
                if (
                    ratio_diff(coverage, target_coverages.get(category))
                    > tolerance / 100
                    and abs(coverage - target_coverages.get(category)) > tolerance
                ):
                    diff_category_coverages.append(category)
                    detailed_messages.append(f"Coverage for category: {category}")
                    detailed_messages.append(
                        f"Coverage in {source_key} job: {coverage} items"
                    )
                    detailed_messages.append(
                        f"Coverage in {target_key} job: "
                        f"{target_coverages.get(category)} items"
                    )
        else:
            new_categories[category] = coverage
    for category, source_category_coverage in target_coverages.items():
        if category not in source_coverages:
            missing_categories[category] = source_category_coverage

    if new_categories:
        new = (
            f"New Categories: {len(new_categories)} categories "
            f"(scraped in {source_key} but not in {target_key})"
        )
        detailed_messages.append(new)

        for category, category_coverage in new_categories.items():
            detailed_messages.append(f"{category}: {category_coverage} items")

    if missing_categories:
        missing = (
            f"Missing Categories: {len(missing_categories)} categories "
            f"(scraped in {target_key} but not in {source_key})"
        )
        detailed_messages.append(missing)

        for category, category_coverage in missing_categories.items():
            detailed_messages.append(f"{category}: {category_coverage} items")

    if len(diff_category_coverages) == 0:
        result.add_info(
            f"Similar coverage per category with {tolerance}% tolerance",
            "\n".join(detailed_messages),
        )
    else:
        result.add_error(
            f"Coverage per category difference > {tolerance}% for "
            f"{len(diff_category_coverages)} categories",
            "\n".join(detailed_messages),
        )

    res = (
        f"Same categories: {len(same_categories)}; "
        f"new categories: {len(new_categories)}; "
        f"missing categories: {len(missing_categories)}"
    )
    if len(new_categories) > 0 or len(missing_categories) > 0:
        result.add_error(res)
    else:
        result.add_info(res)

    return result


def get_coverage_per_category(df: pd.DataFrame, tagged_fields: TaggedFields):
    result = Result("Coverage For Scraped Categories")

    category_fields = tagged_fields.get("category", [])
    for f in category_fields:
        value_counts = df[f].value_counts()
        result.add_info(f"{len(value_counts)} categories in '{f}'", stats=value_counts)
    return result
