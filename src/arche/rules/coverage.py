from arche.rules.result import Result
from arche.tools.api import get_items_count
import pandas as pd


def check_fields_coverage(df: pd.DataFrame) -> Result:
    fields_coverage = df.count().sort_values(ascending=False)
    fields_coverage.name = "Fields coverage"

    empty_fields = fields_coverage[fields_coverage == 0]

    result = Result("Fields Coverage")
    if empty_fields.empty:
        result.add_info("PASSED", stats=fields_coverage)
    else:
        result.add_error(f"{len(empty_fields)} empty field(s)", stats=fields_coverage)
    return result


def get_difference(source_job, target_job) -> Result:
    """Get difference between fields counts towards job size

    Args:
        source_job: a base job, the difference is calculated from it
        target_job: a job to compare

    Returns:
        A Result instance with messages if any and stats with fields counts coverage
    """
    result = Result("Coverage Difference")

    f_counts = (
        pd.DataFrame(
            {
                source_job.key: source_job.items.stats().get("counts", None),
                target_job.key: target_job.items.stats().get("counts", None),
            }
        )
        .mul(100)
        .fillna(0)
        .sort_values(by=[source_job.key], kind="mergesort")
    )
    f_counts[source_job.key] = f_counts[source_job.key].divide(
        get_items_count(source_job)
    )
    f_counts[target_job.key] = f_counts[target_job.key].divide(
        get_items_count(target_job)
    )
    f_counts = f_counts.round(2)

    f_counts.name = "Coverage difference in fields counts"

    coverage_difs = (f_counts[source_job.key] - f_counts[target_job.key]).abs()

    errs = coverage_difs[coverage_difs > 10]
    if not errs.empty:
        result.add_error(f"The difference is greater than 10% for {len(errs)} field(s)")
    warns = coverage_difs[(coverage_difs > 5) & (coverage_difs <= 10)]
    if not warns.empty:
        result.add_warning(
            f"The difference is between 5% and 10% for {len(warns)} field(s)"
        )
    if errs.empty and warns.empty:
        result.add_info("PASSED", stats=f_counts)
    else:
        result.add_info("", stats=f_counts)
    return result


def compare_scraped_fields(source_df: pd.DataFrame, target_df: pd.DataFrame) -> Result:
    """Find new or missing columns between source_df and target_df"""
    result = Result("Scraped Fields")
    missing_fields = target_df.columns.difference(source_df.columns)

    if missing_fields.array:
        result.add_error(f"Missing - {', '.join(missing_fields)}")

    new_fields = source_df.columns.difference(target_df.columns)
    if new_fields.array:
        result.add_info(f"New - {', '.join(new_fields)}")

    return result
