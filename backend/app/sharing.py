"""
Server-side implementation of the real, open Delta Sharing REST protocol
(github.com/delta-io/delta-sharing) - a standard `delta-sharing` Python/Spark client, or
Power BI's Delta Sharing connector, can read from AtlasFlow's shares directly, not just
AtlasFlow itself.

Scoped honestly: single fixed schema per share ("default"), snapshot reads only (no
change-data-feed / historical version querying), and file URLs point back at AtlasFlow's
own file-serving endpoint rather than cloud-storage presigned URLs, since AtlasFlow's
Delta tables live on local/shared-volume disk, not S3 - functionally equivalent for a
client (it's still just a URL to GET), just not literally an S3 presigned URL.
"""
import os
from datetime import datetime, timedelta

from deltalake import DeltaTable
from jose import jwt

FILE_TOKEN_SECRET = os.getenv("ATLASFLOW_JWT_SECRET", "dev-only-insecure-secret-change-me")
FILE_TOKEN_TTL_SECONDS = 600


def table_snapshot(table_path: str) -> dict:
    """Returns {schema_string, files: [{path, size}], version}."""
    dt = DeltaTable(table_path)
    schema_string = dt.schema().to_json()
    files = []
    for add_action in dt.get_add_actions(flatten=True).to_pylist():
        files.append({
            "path": add_action["path"],
            "size": add_action.get("size_bytes") or add_action.get("size", 0),
        })
    return {"schema_string": schema_string, "files": files, "version": dt.version()}


def make_file_token(table_path: str, relative_path: str) -> str:
    payload = {
        "purpose": "delta_share_file", "table_path": table_path, "relative_path": relative_path,
        "exp": datetime.utcnow() + timedelta(seconds=FILE_TOKEN_TTL_SECONDS),
    }
    return jwt.encode(payload, FILE_TOKEN_SECRET, algorithm="HS256")


def verify_file_token(token: str) -> dict:
    payload = jwt.decode(token, FILE_TOKEN_SECRET, algorithms=["HS256"])
    if payload.get("purpose") != "delta_share_file":
        raise ValueError("Invalid token purpose")
    return payload


def build_file_url(base_url: str, table_path: str, relative_path: str) -> str:
    token = make_file_token(table_path, relative_path)
    return f"{base_url}/delta-sharing/files/{token}"
