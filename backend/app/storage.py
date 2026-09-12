"""
File operations backing Volumes (browsable arbitrary-file storage, as opposed to the
tabular Table catalog). Uses MinIO (S3-compatible), the same storage already running in
docker-compose.yml for the data lake - Volumes just live in a separate bucket/prefix
space so they don't get mixed up with Delta table data.

"Directories" are the standard S3 convention: a zero-byte object whose key ends in `/`.
Real cloud object stores (and MinIO) don't have true directories - this is how every
S3-based file browser (including Databricks Volumes, under the hood) represents them.
"""
import os

import boto3
from botocore.client import Config

BUCKET = "atlasflow-volumes"

_endpoint = os.getenv("MLFLOW_S3_ENDPOINT_URL", "http://minio:9000")
_access_key = os.getenv("AWS_ACCESS_KEY_ID", "admin")
_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "admin12345")

_s3 = boto3.client(
    "s3", endpoint_url=_endpoint, aws_access_key_id=_access_key, aws_secret_access_key=_secret_key,
    config=Config(signature_version="s3v4"), region_name="us-east-1",
)


def ensure_bucket():
    """Called on backend startup - creates the volumes bucket if it doesn't exist yet."""
    try:
        _s3.head_bucket(Bucket=BUCKET)
    except Exception:
        try:
            _s3.create_bucket(Bucket=BUCKET)
        except Exception:
            pass  # another replica may have created it concurrently, or MinIO isn't up yet


def _prefix(volume_name: str, path: str = "") -> str:
    path = (path or "").strip("/")
    return f"{volume_name}/{path}/" if path else f"{volume_name}/"


def list_dir(volume_name: str, path: str = "") -> list:
    """Returns immediate children (files and subdirectories) at `path` within a volume."""
    prefix = _prefix(volume_name, path)
    resp = _s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, Delimiter="/")

    entries = []
    for cp in resp.get("CommonPrefixes", []):
        name = cp["Prefix"][len(prefix):].rstrip("/")
        if name:
            entries.append({"name": name, "is_dir": True, "size": None, "last_modified": None})
    for obj in resp.get("Contents", []):
        name = obj["Key"][len(prefix):]
        if name and not name.endswith("/"):  # skip the directory marker object itself
            entries.append({
                "name": name, "is_dir": False, "size": obj["Size"],
                "last_modified": obj["LastModified"].isoformat(),
            })
    entries.sort(key=lambda e: (not e["is_dir"], e["name"].lower()))
    return entries


def mkdir(volume_name: str, path: str):
    key = _prefix(volume_name, path)
    _s3.put_object(Bucket=BUCKET, Key=key, Body=b"")


def upload(volume_name: str, path: str, filename: str, fileobj):
    key = f"{volume_name}/{path.strip('/')}/{filename}" if path.strip("/") else f"{volume_name}/{filename}"
    _s3.upload_fileobj(fileobj, BUCKET, key)


def delete(volume_name: str, path: str, is_dir: bool):
    if is_dir:
        prefix = _prefix(volume_name, path)
        resp = _s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        keys = [{"Key": o["Key"]} for o in resp.get("Contents", [])]
        if keys:
            _s3.delete_objects(Bucket=BUCKET, Delete={"Objects": keys})
    else:
        key = f"{volume_name}/{path.strip('/')}"
        _s3.delete_object(Bucket=BUCKET, Key=key)


def download_url(volume_name: str, path: str, expires_seconds: int = 3600) -> str:
    key = f"{volume_name}/{path.strip('/')}"
    return _s3.generate_presigned_url(
        "get_object", Params={"Bucket": BUCKET, "Key": key}, ExpiresIn=expires_seconds,
    )


def delete_volume_prefix(volume_name: str):
    """Called when a whole Volume is deleted - removes every object under it."""
    prefix = f"{volume_name}/"
    paginator = _s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if keys:
            _s3.delete_objects(Bucket=BUCKET, Delete={"Objects": keys})
