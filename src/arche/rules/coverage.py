from typing import List

from arche.rules.result import Result, Outcome
import arche.tools.api as api
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


def get_difference(
    source_job: Job, target_job: Job, err_thr: float = 0.10, warn_thr: float = 0.05
) -> Result:
    """Get difference between jobs coverages. The coverage is job fields counts
    divided on the job size.

    Args:
        source_job: a base job, the difference is calculated from it
        target_job: a job to compare
        err_thr: a threshold for errors
        warn_thr: a threshold for warnings

    Returns:
        A Result instance with huge dif and stats with fields counts coverage and dif
    """
    result = Result("Coverage Difference")
    f_counts = (
        pd.DataFrame(
            {
                source_job.key: api.get_counts(source_job),
                target_job.key: api.get_counts(target_job),
            }
        )
        .drop(index=["_type"])
        .fillna(0)
        .sort_values(by=[source_job.key], kind="mergesort")
    )
    f_counts[source_job.key] = f_counts[source_job.key].divide(
        api.get_items_count(source_job)
    )
    f_counts[target_job.key] = f_counts[target_job.key].divide(
        api.get_items_count(target_job)
    )
    f_counts.name = "Coverage from job stats fields counts"
    result.stats.append(f_counts)

    coverage_difs = f_counts[source_job.key] - f_counts[target_job.key]
    coverage_difs = coverage_difs[coverage_difs.abs() > warn_thr].sort_values(
        kind="mergesoft"
    )
    coverage_difs.name = f"Coverage difference more than {warn_thr:.0%}"
    if not coverage_difs.empty:
        result.stats.append(coverage_difs)

    errs = coverage_difs[coverage_difs.abs() > err_thr]
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


def anomalies(target: str, sample: List[str]) -> Result:
    """Find fields with significant deviation. Significant means `dev > 2 * std()`

    Args:
        target: where to look for anomalies
        sample: a list of jobs keys to infer metadata from

    Returns:
        A Result with a dataframe of significant deviations
    """
    result = Result("Anomalies")
    raw_stats = [job.items.stats() for job in api.get_jobs(sample + [target])]

    counts = (
        pd.DataFrame(rs.get("counts") for rs in raw_stats)
        .fillna(0)
        .drop(columns="_type")
    )
    items_len = [rs["totals"]["input_values"] for rs in raw_stats]
    stats = counts.apply(lambda x: x / items_len)
    stats.index = sample + [target]
    stats.rename(index={target: "target"}, inplace=True)
    stats.loc["mean"] = stats.loc[sample].mean()
    stats.loc["std"] = stats.loc[sample].std()
    stats = stats.T
    stats["target deviation"] = stats["target"] - stats["mean"]
    devs = stats[(stats["target deviation"].abs() > 2 * stats["std"])]
    devs.name = "Anomalies"
    errors = f"{len(devs.index)} field(s) with significant coverage deviation"
    if not devs.empty:
        result.add_error(Outcome.FAILED, detailed=errors)
        result.stats = [devs]

    return result
