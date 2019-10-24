from typing import Tuple

from arche.readers.schema import TaggedFields
from arche.rules.result import *


MAX_MISSING_VALUES = 6


def fields(
    source_df: pd.DataFrame,
    target_df: pd.DataFrame,
    names: List[str],
    normalize: bool = False,
    err_thr: float = 0.25,
) -> Result:
    """Finds fields values difference between dataframes.

    Args:
        names - a list of field names
        normalize - if set, all fields converted to str and processed with lower() and strip()
        err_thr - sets the failure threshold for missing values

    Returns:
        Result with same, missing and new values.
    """

    def get_difference(
        left: pd.Series, right: pd.Series
    ) -> Tuple[pd.Series, pd.Series, pd.Series]:
        return (
            left[left.isin(right)],
            left[~(left.isin(right))],
            right[~(right.isin(left))],
        )

    result = Result("Fields Difference")
    for field in names:
        source = source_df[field].dropna()
        target = target_df[field].dropna()
        if normalize:
            source = source.astype(str).str.lower().str.strip()
            target = target.astype(str).str.lower().str.strip()
        try:
            same, new, missing = get_difference(source, target)
        except SystemError:
            source = source.astype(str)
            target = target.astype(str)
            same, new, missing = get_difference(source, target)

        same.name, new.name, missing.name = (None, None, None)
        result.more_stats.update(
            {f"{field}": {"same": same, "new": new, "missing": missing}}
        )
        result.add_info(
            f"{len(source)} `non NaN {field}s` - {len(new)} new, {len(same)} same"
        )
        if len(missing) == 0:
            continue

        if len(missing) < MAX_MISSING_VALUES:
            msg = ", ".join(missing.unique().astype(str))
        else:
            msg = f"{', '.join(missing.unique()[:5].astype(str))}..."
        msg = f"{msg} `{field}s` are missing"
        if len(missing) / len(target_df) >= err_thr:
            result.add_error(
                f"{len(missing)} `{field}s` are missing",
                errors={msg: set(missing.index)},
            )
        else:
            result.add_info(
                f"{len(missing)} `{field}s` are missing",
                errors={msg: set(missing.index)},
            )
    return result


def tagged_fields(
    source_df: pd.DataFrame,
    target_df: pd.DataFrame,
    tagged_fields: TaggedFields,
    tags: List[str],
) -> Result:
    """Compare fields tagged with `tags` between two dataframes."""
    name = f"{', '.join(tags)} Fields Difference"
    result = Result(name)
    fields_names: List[str] = list()
    for tag in tags:
        tag_fields = tagged_fields.get(tag)
        if tag_fields:
            fields_names.extend(tag_fields)
    if not fields_names:
        result.outcome = Outcome.SKIPPED
        return result
    result = fields(source_df, target_df, fields_names)
    result.name = name
    return result
