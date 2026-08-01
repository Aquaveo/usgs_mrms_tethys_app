"""Shared S3 access config for the app's data helpers.

Works in two deployments without code changes, driven entirely by env:

  * Standalone (original): static KEY/SECRET in the app .env file, data in the
    bucket ``usgs-mrms-explorer`` at the bucket root.
  * CIROH portal: no keys stored -- credentials come from the default boto3
    chain (IRSA / instance role), and the data lives under a prefix in the
    shared portal bucket ``ciroh-portal-assets``.

Env:
  USGS_MRMS_S3_BUCKET  bucket holding the MRMS data   (default usgs-mrms-explorer)
  USGS_MRMS_S3_PREFIX  key prefix within that bucket  (default "" -- bucket root)
  USGS_MRMS_S3_REGION  bucket region                  (default us-east-1)
  KEY / SECRET         optional static creds; when either is unset the default
                       boto3 credential chain is used (IRSA / instance / env)
"""

import os

import boto3

# .env is optional: present in the standalone deployment, absent in the portal
# (where creds come from IRSA and python-dotenv may not be installed).
try:
    from dotenv import load_dotenv

    load_dotenv(os.path.join(os.path.dirname(__file__), "../../.env"))
except Exception:
    pass


def bucket_name():
    return os.getenv("USGS_MRMS_S3_BUCKET", "usgs-mrms-explorer")


def _prefix():
    p = os.getenv("USGS_MRMS_S3_PREFIX", "").strip("/")
    return f"{p}/" if p else ""


def full_key(key):
    """Map a logical object key/prefix to its full key under the configured prefix."""
    return f"{_prefix()}{key.lstrip('/')}"


def get_bucket():
    region = os.getenv("USGS_MRMS_S3_REGION", "us-east-1")
    key = os.getenv("KEY")
    secret = os.getenv("SECRET")

    if key and secret:
        s3 = boto3.resource(
            "s3",
            aws_access_key_id=key,
            aws_secret_access_key=secret,
            region_name=region,
        )
    else:
        # Default credential chain: IRSA / instance role / ambient env creds.
        s3 = boto3.resource("s3", region_name=region)

    return s3.Bucket(bucket_name())
