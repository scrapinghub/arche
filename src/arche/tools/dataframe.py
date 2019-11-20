from io import StringIO
from typing import Optional

from pandas._config import get_option, config
from pandas.io.formats import format as fmt


pc_render_links_doc = """
: bool
    This sets if URLs in DataFrame should be rendered as clickable anchors.
"""

# Register `render_links` option
with config.config_prefix("display"):
    config.register_option(
        "render_links", True, pc_render_links_doc, validator=config.is_bool
    )


def _repr_html_(self) -> Optional[str]:
    """
    Return a html representation for a particular DataFrame.
    Mainly for IPython notebook.
    """
    if self._info_repr():
        buf = StringIO("")
        self.info(buf=buf)
        # need to escape the <class>, should be the first line.
        val = buf.getvalue().replace("<", r"&lt;", 1)
        val = val.replace(">", r"&gt;", 1)
        return "<pre>" + val + "</pre>"

    if get_option("display.notebook_repr_html"):
        max_rows = get_option("display.max_rows")
        min_rows = get_option("display.min_rows")
        max_cols = get_option("display.max_columns")
        show_dimensions = get_option("display.show_dimensions")
        render_links = get_option("display.render_links")

        formatter = fmt.DataFrameFormatter(
            self,
            columns=None,
            col_space=None,
            na_rep="NaN",
            formatters=None,
            float_format=None,
            sparsify=None,
            justify=None,
            index_names=True,
            header=True,
            index=True,
            bold_rows=True,
            escape=True,
            max_rows=max_rows,
            min_rows=min_rows,
            max_cols=max_cols,
            show_dimensions=show_dimensions,
            decimal=".",
            table_id=None,
            render_links=render_links,
        )
        formatter.to_html(notebook=True)
        return formatter.buf.getvalue()
    else:
        return None
