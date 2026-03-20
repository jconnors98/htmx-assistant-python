from __future__ import annotations

import hashlib
import mimetypes
import os
import shutil
import tempfile
import zipfile
from pathlib import Path
from typing import Dict, Iterator, List, Optional

try:
    import magic  # type: ignore
except ImportError:  # pragma: no cover
    magic = None


class ZipExtractionError(ValueError):
    """Raised when a ZIP archive exceeds configured safety limits."""


def iter_extract_zip(
    zip_file_path: str,
    output_dir: Optional[str] = None,
    *,
    max_members: Optional[int] = None,
    max_member_size_bytes: Optional[int] = None,
    max_total_size_bytes: Optional[int] = None,
    max_compression_ratio: Optional[float] = None,
    chunk_size: int = 1024 * 1024,
) -> Iterator[str]:
    """
    Extract a ZIP file incrementally and yield extracted file paths.

    Limits are enforced against the archive metadata before each member is copied
    so large or highly-compressed uploads fail fast.
    """
    destination = Path(output_dir or tempfile.mkdtemp(prefix="doc_intel_zip_")).resolve()
    destination.mkdir(parents=True, exist_ok=True)

    extracted_members = 0
    total_uncompressed = 0

    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        for member in zip_ref.infolist():
            if member.is_dir():
                continue

            extracted_members += 1
            if max_members and extracted_members > max_members:
                raise ZipExtractionError(f"ZIP contains more than {max_members} files.")

            member_size = max(int(member.file_size or 0), 0)
            compressed_size = max(int(member.compress_size or 0), 0)
            total_uncompressed += member_size

            if max_member_size_bytes and member_size > max_member_size_bytes:
                raise ZipExtractionError(
                    f"ZIP member '{member.filename}' exceeds the {max_member_size_bytes} byte limit."
                )
            if max_total_size_bytes and total_uncompressed > max_total_size_bytes:
                raise ZipExtractionError(
                    f"ZIP expands beyond the {max_total_size_bytes} byte total limit."
                )
            if (
                max_compression_ratio
                and compressed_size > 0
                and (member_size / compressed_size) > max_compression_ratio
            ):
                raise ZipExtractionError(
                    f"ZIP member '{member.filename}' exceeds the compression ratio limit."
                )

            safe_path = _safe_destination_path(destination, member.filename)
            safe_path.parent.mkdir(parents=True, exist_ok=True)

            with zip_ref.open(member) as src, safe_path.open("wb") as dst:
                shutil.copyfileobj(src, dst, length=chunk_size)

            if safe_path.is_file():
                yield str(safe_path)


def extract_zip(zip_file_path: str, output_dir: Optional[str] = None, **kwargs) -> List[str]:
    """
    Extracts a ZIP file and returns the list of extracted file paths.
    """
    return list(iter_extract_zip(zip_file_path, output_dir, **kwargs))


def detect_file_type(file_path: str) -> Dict[str, Optional[str]]:
    """
    Detects the MIME type and extension of a file using python-magic when available.
    Falls back to mimetypes based on file extension.
    """
    mime_type = None
    file_extension = Path(file_path).suffix.lower().lstrip(".")

    if magic:
        try:
            mime = magic.Magic(mime=True)  # type: ignore
            mime_type = mime.from_file(file_path)
        except Exception:  # noqa: BLE001
            mime_type = None

    if not mime_type:
        mime_type, _ = mimetypes.guess_type(file_path)

    is_image = bool(mime_type and mime_type.startswith("image/"))
    is_pdf = mime_type == "application/pdf" or file_extension == "pdf"
    is_excel = file_extension in {"xls", "xlsx", "csv"}
    is_word = file_extension in {"doc", "docx"}

    checksum = _checksum(file_path)

    return {
        "mime_type": mime_type or "application/octet-stream",
        "file_extension": file_extension,
        "is_image": is_image,
        "is_pdf": is_pdf,
        "is_excel": is_excel,
        "is_word": is_word,
        "checksum": checksum,
    }


def _checksum(file_path: str) -> Optional[str]:
    if not os.path.exists(file_path):
        return None
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def _safe_destination_path(destination: Path, member_name: str) -> Path:
    member_path = Path(member_name)
    if member_path.is_absolute():
        raise ZipExtractionError(f"ZIP member '{member_name}' uses an absolute path.")

    normalized_parts = [part for part in member_path.parts if part not in {"", "."}]
    if any(part == ".." for part in normalized_parts):
        raise ZipExtractionError(f"ZIP member '{member_name}' attempts path traversal.")

    safe_path = (destination / Path(*normalized_parts)).resolve()
    try:
        safe_path.relative_to(destination)
    except ValueError as exc:
        raise ZipExtractionError(f"ZIP member '{member_name}' escapes extraction root.") from exc
    return safe_path

