"""AWS methods. Credentials are expected to be set in environment"""
import io
from urllib.parse import quote, urlparse
import urllib.request

import boto3


def upload_str_stream(bucket: str, key: str, stream: io.StringIO) -> str:
    """Upload a whole StringIO to S3 bucket as file.

    Args:
        bucket: bucket name
        key: destination path
        stream: stream object

    Returns:
        public url to uploaded file
    """
    stream.seek(0)
    session = boto3.Session()
    client = session.client("s3")
    client.put_object(Bucket=bucket, Key=key, Body=stream.read(), ACL="public-read")

    return f"https://{bucket}.s3.amazonaws.com/{quote(key)}"


def get_contents_from_bucket(bucket: str, filepath: str) -> str:
    """Fetch file contents from s3 bucket.

    Args:
        bucket: bucket name
        filepath: a relative path to file

    Returns:
        utf-8 decoded string
    """
    session = boto3.Session()
    client = session.client("s3")
    obj = client.get_object(Bucket=bucket, Key=filepath)
    return obj["Body"].read().decode("utf-8")


def get_contents(url: str) -> str:
    """Fetch file contents from the remote source.

    Args:
        url: uri path to a file. Allows only s3, https.
    Returns:
        utf-8 decoded string
    """
    o = urlparse(url)
    netloc = o.netloc
    relative_path = o.path.lstrip("/")
    if not netloc or not relative_path:
        raise ValueError(f"'{url}' is not a valid s3 or URL path to a file")
    if o.scheme == "s3":
        return get_contents_from_bucket(netloc, relative_path)
    if o.scheme == "https":
        with urllib.request.urlopen(url) as f:
            return f.read().decode("utf-8")
    raise ValueError(f"'{o.scheme}://' scheme is not allowed")
