import base64
import logging
import os
import re
import urllib


logger = logging.getLogger("arche")


NETLOC = os.getenv("BITBUCKET_NETLOC") or "bitbucket.org"
API_NETLOC = os.getenv("BITBUCKET_API_NETLOC") or "api.bitbucket.org"
USER = os.getenv("BITBUCKET_USER")
PASS = os.getenv("BITBUCKET_PASSWORD")


def prepare_request(url):
    if not USER or not PASS:
        msg = "Credentials not found"
        logger.error(
            f"{msg}."
            "\nMake it sure to set both `BITBUCKET_USER` and `BITBUCKET_PASSWORD` "
            "environment variables."
        )
        raise RuntimeError(msg)

    api_url = convert_to_api_url(url, NETLOC, API_NETLOC)
    return urllib.request.Request(api_url, headers=get_auth_header(USER, PASS))


def convert_to_api_url(url, netloc, api_netloc):
    """Support both regular and raw URLs"""
    user, repo, path = re.search(
        f"https://{netloc}/(.*?)/(.*?)/(?:raw|src)/(.*)", url
    ).groups()
    return f"https://{api_netloc}/2.0/repositories/{user}/{repo}/src/{path}"


def get_auth_header(username, password):
    base64string = base64.b64encode(f"{username}:{password}".encode())
    return {"Authorization": f"Basic {base64string.decode()}"}
