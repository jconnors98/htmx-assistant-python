from __future__ import annotations

import hashlib
import mimetypes
import os
import tempfile
import zipfile
from pathlib import Path
from typing import Dict, List, Optional

try:
    import magic  # type: ignore
except ImportError:  # pragma: no cover
    magic = None


def extract_zip(zip_file_path: str, output_dir: Optional[str] = None) -> List[str]:
    """
    Extracts a ZIP file and returns the list of extracted file paths.
    """
    destination = Path(output_dir or tempfile.mkdtemp(prefix="doc_intel_zip_"))
    destination.mkdir(parents=True, exist_ok=True)
    extracted_files: List[str] = []

    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        for member in zip_ref.infolist():
            # Skip directory entries to prevent blocking file creation
            if member.is_dir():
                continue

            # Guard against Zip Slip attacks
            member_path = Path(member.filename)
            if member_path.is_absolute():
                continue
                
            safe_path = destination / member_path
            safe_path.parent.mkdir(parents=True, exist_ok=True)
            
            with zip_ref.open(member) as src, open(safe_path, "wb") as dst:
                dst.write(src.read())
            
            if safe_path.is_file():
                extracted_files.append(str(safe_path))

    return extracted_files


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

