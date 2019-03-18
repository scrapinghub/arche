"""AWS methods. Credentials are expected to be set in environment"""
import io
from urllib.parse import quote

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


def get_contents_as_string(bucket: str, filepath: str) -> str:
    """Fetch file contents as a utf-8 decoded string.

    Args:
        bucket: bucket name
        filepath: a relevant path to file

    Returns:
        File contents
    """
    session = boto3.Session()
    client = session.client("s3")
    obj = client.get_object(Bucket=bucket, Key=filepath)
    return obj["Body"].read().decode("utf-8")
