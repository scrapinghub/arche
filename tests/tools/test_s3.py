import arche.tools.s3 as s3
import pytest


def test_get_contents(mocker):
    contents = "something"
    mocker.patch(
        "arche.tools.s3.get_contents_from_bucket", return_value=contents, autospec=True
    )
    assert s3.get_contents("s3://bucket/file") == contents


def test_get_contents_fails_on_bad_file(mocker):
    cm = mocker.MagicMock()
    cm.__enter__.return_value = cm
    cm.read.side_effect = ValueError("'ValueError' object has no attribute 'decode'")
    mocker.patch(
        "arche.tools.s3.urllib.request.urlopen", return_value=cm, autospec=True
    )
    with pytest.raises(ValueError) as excinfo:
        s3.get_contents("https://example.com/schema.json")
    assert str(excinfo.value) == "'ValueError' object has no attribute 'decode'"


@pytest.mark.parametrize(
    "path, expected_message",
    [
        ("s3://bucket", "'s3://bucket' is not a valid s3 or URL path to a file"),
        ("bucket/file", "'bucket/file' is not a valid s3 or URL path to a file"),
        ("file://bucket/file", "'file://' scheme is not allowed"),
        ("http://bucket/file", "'http://' scheme is not allowed"),
    ],
)
def test_get_contents_fails_on_bad_path(path, expected_message):
    with pytest.raises(ValueError) as excinfo:
        s3.get_contents(path)
    assert str(excinfo.value) == expected_message
