import re


def make_urls_clickable(val):
    if isinstance(val, str) and re.search("^https?://", val):
        return f'<a target="_blank" href="{val}">{val}</a>'
    else:
        return val


def _repr_html_(self, *args, **kwargs):
    styler = self.style.format(make_urls_clickable)
    return styler.render(*args, **kwargs)
