"""S3 client factory."""

import os


def create_s3_client():
    """Create a boto3 S3 client from environment variables."""
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=f"https://{os.environ['S3_ENDPOINT']}",
        aws_access_key_id=os.environ["S3_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["S3_SECRET_ACCESS_KEY"],
        region_name="auto",
    )
