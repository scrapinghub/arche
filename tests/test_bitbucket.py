from arche.tools import bitbucket
import pytest


urls = [
    (
        "https://bitbucket.org/scrapinghub/customer/src/master/customer/schemas/ecommerce.json",
        "https://api.bitbucket.org/2.0/repositories/scrapinghub/customer/src/master"
        "/customer/schemas/ecommerce.json",
    ),
    (
        "https://bitbucket.org/scrapinghub/customer/raw/"
        "9c4b0bf46f2012ab38bc066e1ebe774d72856013/customer/schemas/ecommerce.json",
        "https://api.bitbucket.org/2.0/repositories/scrapinghub/customer/src/"
        "9c4b0bf46f2012ab38bc066e1ebe774d72856013/customer/schemas/ecommerce.json",
    ),
]


@pytest.mark.parametrize(
    ["url", "expected"],
    [
        (
            "https://bitbucket.org/scrapinghub/customer/src/master/customer/schemas/"
            "ecommerce.json",
            "https://api.bitbucket.org/2.0/repositories/scrapinghub/customer/src/"
            "master/customer/schemas/ecommerce.json",
        ),
        (
            "https://bitbucket.org/scrapinghub/customer/raw/"
            "9c4b0bf46f2012ab38bc066e1ebe774d72856013/customer/schemas/"
            "ecommerce.json",
            "https://api.bitbucket.org/2.0/repositories/scrapinghub/customer/src/"
            "9c4b0bf46f2012ab38bc066e1ebe774d72856013/customer/schemas/"
            "ecommerce.json",
        ),
    ],
)
def test_convert_to_api_url(url, expected):
    api_url = bitbucket.convert_to_api_url(url, bitbucket.NETLOC, bitbucket.API_NETLOC)
    assert api_url == expected


@pytest.mark.parametrize(
    "url",
    [
        "https://bitbucket.org/ecommerce.json",
        "https://bitbucket.org/user/ecommerce.json",
        "https://bitbucket.org/user/repo/ecommerce.json",
        "https://bitbucket.org/user/repo/foobar/ecommerce.json",
    ],
)
def test_convert_to_api_url_using_an_invalid_url(url):
    with pytest.raises(ValueError):
        bitbucket.convert_to_api_url(url, bitbucket.NETLOC, bitbucket.API_NETLOC)


@pytest.mark.parametrize(
    "credentials,expected",
    [(("foo", "bar"), "Zm9vOmJhcg=="), (("alice", "secret"), "YWxpY2U6c2VjcmV0")],
)
def test_get_auth_header(credentials, expected):
    assert bitbucket.get_auth_header(*credentials) == {
        "Authorization": f"Basic {expected}"
    }


def test_prepare_request():
    bitbucket.USER = "foo"
    bitbucket.PASS = "bar"

    url = (
        "https://bitbucket.org/scrapinghub/customer/src/master/customer/schemas/"
        "ecommerce.json"
    )
    req = bitbucket.prepare_request(url)

    assert "api.bitbucket.org" == req.host
    assert "Authorization" in req.headers


def test_prepare_request_raises_an_error_when_no_credentials_found():
    bitbucket.USER = bitbucket.PASS = None

    with pytest.raises(ValueError):
        bitbucket.prepare_request("foo")
