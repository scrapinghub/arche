import base64
import os
import re
from typing import Dict
from urllib.request import Request


NETLOC = os.getenv("BITBUCKET_NETLOC") or "bitbucket.org"
API_NETLOC = os.getenv("BITBUCKET_API_NETLOC") or "api.bitbucket.org"
USER = os.getenv("BITBUCKET_USER")
PASS = os.getenv("BITBUCKET_PASSWORD")


def prepare_request(url: str) -> Request:
    if not USER or not PASS:
        msg = "Credentials not found: `BITBUCKET_USER` or `BITBUCKET_PASSWORD` not set."
        raise ValueError(msg)

    api_url = convert_to_api_url(url, NETLOC, API_NETLOC)
    return Request(api_url, headers=get_auth_header(USER, PASS))


def convert_to_api_url(url: str, netloc: str, api_netloc: str) -> str:
    """Support both regular and raw URLs"""
    match = re.search(f"https://{netloc}/(.*?)/(.*?)/(?:raw|src)/(.*)", url)
    if match:
        user, repo, path = match.groups()
    else:
        raise ValueError("Not a valid bitbucket URL: {url}")
    return f"https://{api_netloc}/2.0/repositories/{user}/{repo}/src/{path}"


def get_auth_header(username: str, password: str) -> Dict[str, str]:
    base64string = base64.b64encode(f"{username}:{password}".encode())
    return {"Authorization": f"Basic {base64string.decode()}"}
