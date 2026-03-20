from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Any, Dict, Optional
from uuid import uuid4


class DocumentIntelligenceStorage:
    """Persist uploads and generated artifacts on local disk or S3."""

    def __init__(
        self,
        *,
        backend: str,
        local_storage_dir: str,
        s3_client=None,
        bucket: Optional[str] = None,
        key_prefix: str = "doc_intel",
        presign_expiry_seconds: int = 3600,
    ) -> None:
        self.backend = (backend or "local").strip().lower()
        self.local_storage_dir = Path(local_storage_dir)
        self.local_storage_dir.mkdir(parents=True, exist_ok=True)
        self.s3_client = s3_client
        self.bucket = bucket
        self.key_prefix = (key_prefix or "doc_intel").strip().strip("/")
        self.presign_expiry_seconds = int(presign_expiry_seconds)

        if self.backend == "s3" and (self.s3_client is None or not self.bucket):
            raise ValueError("s3_client and bucket are required for S3 doc-intel storage.")

    def save_upload(self, file_storage, *, session_id: str, prefix: str = "uploads") -> Dict[str, Any]:
        filename = Path(getattr(file_storage, "filename", "") or "upload.bin").name
        ext = Path(filename).suffix
        object_id = uuid4().hex
        key = self._build_key(session_id=session_id, prefix=prefix, filename=f"{object_id}{ext}")

        stream = getattr(file_storage, "stream", file_storage)
        self._reset_stream(stream)

        if self.backend == "s3":
            self.s3_client.upload_fileobj(stream, self.bucket, key)
            return {
                "storage_backend": self.backend,
                "storage_bucket": self.bucket,
                "storage_key": key,
                "file_path": "",
                "original_filename": filename,
                "file_size_bytes": getattr(file_storage, "content_length", None),
            }

        destination = self.local_storage_dir / key.replace("/", os.sep)
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open("wb") as handle:
            shutil.copyfileobj(stream, handle, length=1024 * 1024)
        return {
            "storage_backend": self.backend,
            "storage_bucket": None,
            "storage_key": key,
            "file_path": str(destination),
            "original_filename": filename,
            "file_size_bytes": destination.stat().st_size if destination.exists() else None,
        }

    def upload_local_artifact(
        self,
        local_path: str,
        *,
        session_id: str,
        prefix: str = "artifacts",
        download_filename: Optional[str] = None,
    ) -> Dict[str, Any]:
        source = Path(local_path)
        filename = download_filename or source.name
        ext = source.suffix
        key = self._build_key(session_id=session_id, prefix=prefix, filename=f"{uuid4().hex}{ext}")

        if self.backend == "s3":
            self.s3_client.upload_file(str(source), self.bucket, key)
            return {
                "storage_backend": self.backend,
                "storage_bucket": self.bucket,
                "storage_key": key,
                "file_path": "",
                "original_filename": filename,
                "file_size_bytes": source.stat().st_size if source.exists() else None,
            }

        destination = self.local_storage_dir / key.replace("/", os.sep)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
        return {
            "storage_backend": self.backend,
            "storage_bucket": None,
            "storage_key": key,
            "file_path": str(destination),
            "original_filename": filename,
            "file_size_bytes": destination.stat().st_size if destination.exists() else None,
        }

    def download_to_local(self, ref: Dict[str, Any], *, work_dir: str) -> str:
        local_path = (ref or {}).get("file_path")
        if self.backend == "local" and local_path and Path(local_path).exists():
            return local_path

        storage_key = (ref or {}).get("storage_key")
        if not storage_key:
            raise ValueError("storage_key is required to download the file.")

        filename = Path((ref or {}).get("original_filename") or storage_key).name
        destination = Path(work_dir) / f"{uuid4().hex}_{filename}"
        destination.parent.mkdir(parents=True, exist_ok=True)

        if self.backend == "s3":
            bucket = (ref or {}).get("storage_bucket") or self.bucket
            self.s3_client.download_file(bucket, storage_key, str(destination))
            return str(destination)

        source = self.local_storage_dir / storage_key.replace("/", os.sep)
        shutil.copy2(source, destination)
        return str(destination)

    def delete(self, ref: Dict[str, Any]) -> None:
        if not ref:
            return
        try:
            if self.backend == "s3":
                storage_key = ref.get("storage_key")
                bucket = ref.get("storage_bucket") or self.bucket
                if storage_key and bucket:
                    self.s3_client.delete_object(Bucket=bucket, Key=storage_key)
                return

            file_path = ref.get("file_path")
            if file_path and Path(file_path).exists():
                Path(file_path).unlink(missing_ok=True)
        except Exception:
            return

    def build_download_url(self, ref: Dict[str, Any], *, download_filename: Optional[str] = None) -> Optional[str]:
        if not ref:
            return None
        if self.backend != "s3":
            return None

        storage_key = ref.get("storage_key")
        bucket = ref.get("storage_bucket") or self.bucket
        if not storage_key or not bucket:
            return None

        params = {"Bucket": bucket, "Key": storage_key}
        if download_filename:
            params["ResponseContentDisposition"] = f'attachment; filename="{download_filename}"'

        return self.s3_client.generate_presigned_url(
            "get_object",
            Params=params,
            ExpiresIn=self.presign_expiry_seconds,
        )

    def resolve_local_path(self, ref: Dict[str, Any]) -> Optional[str]:
        if self.backend != "local":
            return None
        file_path = (ref or {}).get("file_path")
        if file_path and Path(file_path).exists():
            return file_path
        storage_key = (ref or {}).get("storage_key")
        if not storage_key:
            return None
        candidate = self.local_storage_dir / storage_key.replace("/", os.sep)
        return str(candidate) if candidate.exists() else None

    def _build_key(self, *, session_id: str, prefix: str, filename: str) -> str:
        safe_session = (session_id or "session").strip()
        safe_prefix = (prefix or "uploads").strip().strip("/")
        safe_filename = Path(filename).name
        parts = [part for part in [self.key_prefix, safe_session, safe_prefix, safe_filename] if part]
        return "/".join(parts)

    @staticmethod
    def _reset_stream(stream: Any) -> None:
        seek = getattr(stream, "seek", None)
        if callable(seek):
            seek(0)
