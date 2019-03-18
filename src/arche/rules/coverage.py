from arche.rules.result import Result
from arche.tools.api import get_items_count
from arche.tools.helpers import ratio_diff
import pandas as pd


def check_fields_coverage(df):
    fields_coverage = pd.DataFrame(df.count(), columns=["Values Count"])
    fields_coverage.index.name = "Field"
    fields_coverage["Percent"] = fields_coverage.apply(
        lambda row: int(row["Values Count"] / len(df) * 100), axis=1
    )

    detailed_msg = fields_coverage.sort_values(by=["Percent", "Field"]).to_string()

    empty_fields = fields_coverage[fields_coverage["Values Count"] == 0]
    result_msg = f"{len(empty_fields)} totally empty field(s)"

    result = Result("Fields Coverage")
    if empty_fields.empty:
        result.add_info(result_msg, detailed_msg)
    else:
        result.add_error(result_msg, detailed_msg)
    return result


def compare_fields_counts(source_job, target_job):
    """Compare the relative difference between field counts to items count

    Args:
        source_job: a base job, the difference is calculated from it
        target_job: a job to compare

    Returns:
        A Result instance
    """
    source_items_count = get_items_count(source_job)
    target_items_count = get_items_count(target_job)
    result = Result("Fields Counts")

    source_fields = pd.DataFrame(
        {"Count1": source_job.items.stats().get("counts", None)}
    )
    target_fields = pd.DataFrame(
        {"Count2": target_job.items.stats().get("counts", None)}
    )
    fields = pd.concat([source_fields, target_fields], axis=1, sort=True).fillna(0)
    fields["Difference, %"] = fields.apply(
        lambda row: ratio_diff(
            row["Count1"] / source_items_count, row["Count2"] / target_items_count
        )
        * 100,
        axis=1,
    )
    fields["Difference, %"] = fields["Difference, %"].astype(int)
    fields.sort_values(by=["Difference, %"], ascending=False)

    err_diffs = fields[fields["Difference, %"] > 10]
    if not err_diffs.empty:
        result.add_error(
            f"Coverage difference is greater than 10% for "
            f"{len(err_diffs)} field(s)",
            err_diffs.to_string(columns=["Difference, %"]),
        )

    warn_diffs = fields[(fields["Difference, %"] > 5) & (fields["Difference, %"] <= 10)]
    if not warn_diffs.empty:
        outcome_msg = (
            f"Coverage difference is between 5% and 10% for "
            f"{len(warn_diffs)} field(s)"
        )
        result.add_warning(outcome_msg, warn_diffs.to_string(columns=["Difference, %"]))

    return result


def compare_scraped_fields(source_df, target_df):
    source_field_coverage = dict(source_df.count().sort_values(ascending=False))
    target_field_coverage = dict(target_df.count().sort_values(ascending=False))

    result = Result("Scraped Fields")
    missing_fields = set(target_df.columns.values) - set(source_df.columns.values)
    if missing_fields:
        detailed_messages = ["Missing Fields"]
        for field in missing_fields:
            target_coverage = target_field_coverage[field] / len(target_df) * 100
            detailed_messages.append(
                f"{field} - coverage - {int(target_coverage)}% - "
                f"{target_field_coverage[field]} items"
            )
        result.add_error(
            f"{len(missing_fields)} field(s) are missing", "\n".join(detailed_messages)
        )

    new_fields = set(source_df.columns.values) - set(target_df.columns.values)
    if new_fields:
        detailed_messages = ["New Fields"]
        for field in new_fields:
            source_coverage = source_field_coverage[field] / len(source_df) * 100
            detailed_messages.append(
                f"{field} - coverage - {int(source_coverage)}% - "
                f"{source_field_coverage[field]} items"
            )
        result.add_info(
            f"{len(new_fields)} field(s) are new", "\n".join(detailed_messages)
        )

    return result
