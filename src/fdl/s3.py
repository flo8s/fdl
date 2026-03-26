"""S3 client factory."""

from fdl.config import s3_access_key_id, s3_endpoint, s3_secret_access_key


def create_s3_client():
    """Create a boto3 S3 client."""
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=s3_endpoint(),
        aws_access_key_id=s3_access_key_id(),
        aws_secret_access_key=s3_secret_access_key(),
        region_name="auto",
    )
