from arche.rules.category_coverage import get_coverage_per_category
from arche.rules.result import Level
from conftest import create_result
import pandas as pd
import pytest


@pytest.mark.parametrize(
    "data, tags, expected_messages",
    [
        (
            {"sex": ["male", "female", "male"], "country": ["uk", "uk", "uk"]},
            {"category": ["sex", "country"]},
            {
                Level.INFO: [
                    (
                        "2 categories in 'sex'",
                        None,
                        None,
                        pd.Series({"male": 2, "female": 1}, name="sex"),
                    ),
                    (
                        "1 categories in 'country'",
                        None,
                        None,
                        pd.Series({"uk": 3}, name="country"),
                    ),
                ]
            },
        )
    ],
)
def test_get_coverage_per_category(data, tags, expected_messages):
    assert get_coverage_per_category(pd.DataFrame(data), tags) == create_result(
        "Coverage For Scraped Categories", expected_messages
    )
