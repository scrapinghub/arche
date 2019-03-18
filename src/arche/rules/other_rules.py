from arche.rules.result import Result
import pandas as pd


def compare_boolean_fields(source_df, target_df):
    source_bool = source_df.select_dtypes(include="bool")
    target_bool = target_df.select_dtypes(include="bool")

    result = Result("Boolean Fields")
    if not fields_to_compare(source_bool, target_bool):
        result.add_info("No fields to compare")
        return result

    source_relative_fr = get_bool_relative_frequency(source_bool)
    target_relative_fr = get_bool_relative_frequency(target_bool)
    relative_diffs = abs(source_relative_fr - target_relative_fr) * 100

    err_diffs = relative_diffs[(relative_diffs > 10).all(1)]
    if not err_diffs.empty:
        result.add_error(
            (
                f"{err_diffs.index.values} relative frequencies differ "
                "by more than 10%"
            ),
            err_diffs.to_string(),
        )

    warn_diffs = relative_diffs[((relative_diffs <= 10) & (relative_diffs > 5)).all(1)]
    if not warn_diffs.empty:
        result.add_warning(
            f"{warn_diffs.index.values} relative frequencies differ by 5-10%",
            warn_diffs.to_string(),
        )
    if err_diffs.empty and warn_diffs.empty:
        result.add_info(
            f"{relative_diffs.index.values} relative frequencies are equal "
            "or differ by less than 5%",
            relative_diffs.to_string(
                header=["Difference in False, %", "Difference in True, %"]
            ),
        )

    return result


def fields_to_compare(source_df, target_df):
    source_fields = source_df.columns.values
    target_fields = target_df.columns.values
    if (
        source_fields.size > 0
        and target_fields.size > 0
        and set(source_fields).intersection(target_fields)
    ):
        return True
    return False


def get_bool_relative_frequency(bool_df):
    return pd.concat(
        [
            bool_df.apply(pd.value_counts, normalize=True).T,
            pd.DataFrame(columns=[False, True]),
        ],
        sort=False,
    ).fillna(0)
