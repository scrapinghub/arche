import pandas as pd
import pytest


@pytest.fixture()
def df_with_urls():
    data = {"col1": [1, 2], "col2": ["http://foo.com", "https://bar.com"]}
    return pd.DataFrame(data)


def test_df_has_clickable_urls(df_with_urls):
    html = df_with_urls._repr_html_()
    assert '<a target="_blank" href="http://foo.com">http://foo.com</a>' in html
    assert '<a target="_blank" href="https://bar.com">https://bar.com</a>' in html


def test_derivaded_df_has_clickable_urls(df_with_urls):
    html = df_with_urls.head()._repr_html_()
    assert '<a target="_blank" href="http://foo.com">http://foo.com</a>' in html
    assert '<a target="_blank" href="https://bar.com">https://bar.com</a>' in html


def test_arche_df_does_not_add_links_if_no_url_found():
    df = pd.DataFrame({"col1": [1, 2], "col2": ["foo", "bar"]})
    html = df._repr_html_()
    assert "<a href=" not in html


def test_large_repr(df_with_urls):
    df_with_urls._info_repr = lambda: True
    html = df_with_urls._repr_html_()

    assert "<pre>&lt;class 'pandas.core.frame.DataFrame'&gt;\n" in html
    assert "<a href=" not in html
