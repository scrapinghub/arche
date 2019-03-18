from typing import Optional
import warnings

from arche import __version__ as current_version


def deprecate(
    reason: str, replacement: Optional[str] = None, gone_in: Optional[str] = None
):
    message = "DEPRECATION: " + reason
    if replacement:
        message += f"\n{replacement}"

    if gone_in and current_version >= gone_in:
        raise FutureWarning
    warnings.warn(message, FutureWarning, stacklevel=2)
