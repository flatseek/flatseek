#!/usr/bin/env python3
"""Upload local index files to Vercel Blob using httpx."""

import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

import httpx

VERCEL_BLOB_API_URL = "https://vercel.com/api/blob"
API_VERSION = "2024-06-06"


def get_token():
    """Get Vercel Blob token from env or fail."""
    token = os.environ.get("BLOB_READ_WRITE_TOKEN", "").strip()
    if not token:
        raise ValueError(
            "BLOB_READ_WRITE_TOKEN environment variable is not set. "
            "Get your token from Vercel dashboard and set it with:\n"
            "  export BLOB_READ_WRITE_TOKEN='vercel_blob_rw_...'"
        )
    return token


def get_bucket_from_token(token: str) -> str:
    """Extract bucket/store ID from token."""
    # Token format: vercel_blob_rw_{storeId}_{random}
    parts = token.split("_")
    if len(parts) >= 4:
        return parts[3].lower()
    raise ValueError(f"Invalid token format: {token!r}")


def upload_file(token: str, bucket: str, pathname: str, data: bytes) -> None:
    """Upload a single file to Vercel Blob."""
    url = f"{VERCEL_BLOB_API_URL}/?pathname={pathname}"
    headers = {
        "Authorization": f"Bearer {token}",
        "x-api-version": API_VERSION,
        "Content-Type": "application/octet-stream",
        "x-allow-overwrite": "1",
    }
    response = httpx.put(url, content=data, headers=headers, timeout=60.0)
    response.raise_for_status()


def main():
    local_index_dir = Path("./data/article_1k/index")
    base_path = "article_1k"

    # Get token and bucket
    token = get_token()
    bucket = get_bucket_from_token(token)

    print(f"Uploading files from {local_index_dir}")
    print(f"Bucket: {bucket}")
    print(f"Base path: {base_path}")

    # Get all files to upload
    files = list(local_index_dir.rglob("*"))
    files = [f for f in files if f.is_file()]
    print(f"Found {len(files)} files to upload")

    uploaded = 0
    errors = 0

    for f in files:
        rel_path = str(f.relative_to(local_index_dir))
        pathname = f"{base_path}/{rel_path}" if base_path else rel_path

        try:
            data = f.read_bytes()
            upload_file(token, bucket, pathname, data)
            uploaded += 1
            if uploaded % 500 == 0:
                print(f"Uploaded {uploaded}/{len(files)}")
        except Exception as e:
            errors += 1
            print(f"Error uploading {rel_path}: {e}")

    print(f"\nDone: {uploaded} uploaded, {errors} errors")


if __name__ == "__main__":
    main()