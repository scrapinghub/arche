import arche.tools.s3 as s3
import pytest


def test_get_contents():
    with pytest.raises(ValueError):
        s3.get_contents("file:///you.never.know")
