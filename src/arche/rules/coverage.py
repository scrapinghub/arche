from arche.rules.result import Result
from arche.tools.api import get_items_count
import pandas as pd
from scrapinghub.client.jobs import Job


def check_fields_coverage(df: pd.DataFrame) -> Result:
    """Get fields coverage from df. Coverage reflects the percentage of real values
    (exluding `nan`) per column.

    Args:
        df: a data to count the coverage

    Returns:
        A result with coverage for all columns in provided df. If column contains only `nan`,
        treat it as an error.
    """
    fields_coverage = df.count().sort_values(ascending=False)
    fields_coverage.name = f"Fields coverage for {len(df):_} items"

    empty_fields = fields_coverage[fields_coverage == 0]

    result = Result("Fields Coverage")
    result.stats = [fields_coverage]
    if not empty_fields.empty:
        result.add_error(f"{len(empty_fields)} empty field(s)")
    return result


def get_difference(source_job: Job, target_job: Job) -> Result:
    """Get difference between jobs coverages. The coverage is job fields counts
    divided on the job size.

    Args:
        source_job: a base job, the difference is calculated from it
        target_job: a job to compare

    Returns:
        A Result instance with huge dif and stats with fields counts coverage and dif
    """
    result = Result("Coverage Difference")
    warn_thr = 0.05
    err_thr = 0.10
    f_counts = (
        pd.DataFrame(
            {
                source_job.key: source_job.items.stats().get("counts", None),
                target_job.key: target_job.items.stats().get("counts", None),
            }
        )
        .fillna(0)
        .sort_values(by=[source_job.key], kind="mergesort")
    )
    f_counts[source_job.key] = f_counts[source_job.key].divide(
        get_items_count(source_job)
    )
    f_counts[target_job.key] = f_counts[target_job.key].divide(
        get_items_count(target_job)
    )
    f_counts.name = "Coverage from job stats fields counts"
    result.stats.append(f_counts)

    coverage_difs = (f_counts[source_job.key] - f_counts[target_job.key]).abs()
    coverage_difs = coverage_difs[coverage_difs > warn_thr].sort_values(
        kind="mergesoft"
    )
    coverage_difs.name = f"Coverage difference more than {warn_thr:.0%}"
    if not coverage_difs.empty:
        result.stats.append(coverage_difs)

    errs = coverage_difs[coverage_difs > err_thr]
    if not errs.empty:
        result.add_error(
            f"The difference is greater than {err_thr:.0%} for {len(errs)} field(s)"
        )
    warns = coverage_difs[(coverage_difs > warn_thr) & (coverage_difs <= err_thr)]
    if not warns.empty:
        result.add_warning(
            f"The difference is between {warn_thr:.0%} and {err_thr:.0%} "
            f"for {len(warns)} field(s)"
        )
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
