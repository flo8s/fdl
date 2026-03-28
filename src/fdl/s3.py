"""S3 client and config."""

from dataclasses import dataclass


@dataclass(frozen=True)
class S3Config:
    """S3 connection config for a target."""

    bucket: str
    endpoint: str
    access_key_id: str
    secret_access_key: str

    @property
    def endpoint_host(self) -> str:
        """Endpoint hostname without scheme (for DuckDB s3_endpoint setting)."""
        return self.endpoint.removeprefix("https://").removeprefix("http://")


def configure_duckdb_s3(conn, s3: S3Config) -> None:
    """Configure a DuckDB connection for S3 access."""
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute(f"""
        SET s3_url_style = 'path';
        SET s3_access_key_id = '{s3.access_key_id}';
        SET s3_secret_access_key = '{s3.secret_access_key}';
        SET s3_endpoint = '{s3.endpoint_host}';
        SET s3_region = 'auto';
    """)


def create_s3_client(s3: S3Config):
    """Create a boto3 S3 client from S3Config."""
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=s3.endpoint,
        aws_access_key_id=s3.access_key_id,
        aws_secret_access_key=s3.secret_access_key,
        region_name="auto",
    )
