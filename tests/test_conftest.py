from conftest import *


@pytest.mark.parametrize(
    "source, target",
    [
        (
            pd.Series([0, 1], index=["f", "l"], name="n"),
            pd.Series([0, 1], index=["f", "l"], name="n"),
        ),
        (pd.DataFrame([0, 1]), pd.DataFrame([0, 1])),
    ],
)
def test_assert_tensors_equal(source, target):
    assert_tensors_equal(source, target)


@pytest.mark.parametrize(
    "source, target",
    [
        (
            pd.Series([0, 1], index=["f", "l"], name="s"),
            pd.Series([0, 1], index=["f", "l"], name="n"),
        ),
        (pd.DataFrame([0, 1]), pd.DataFrame([0, 1], index=["m", "s"])),
    ],
)
def test_assert_tensors_not_equal(source, target):
    with pytest.raises(AssertionError):
        assert_tensors_equal(source, target)
