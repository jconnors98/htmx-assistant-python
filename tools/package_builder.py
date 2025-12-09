from __future__ import annotations

import os
import tempfile
import zipfile
import io
from pathlib import Path
from typing import Dict, List, Optional

from PyPDF2 import PdfReader, PdfWriter  # type: ignore
from PIL import Image
import logging

logger = logging.getLogger(__name__)


def build_pdf_package(plan_dict: Dict[str, any], output_dir: Optional[str] = None) -> str:
    """
    Builds a combined PDF based on the supplied plan definition.
    plan_dict format:
    {
        "package_title": "Mechanical Bid Package",
        "sections": [
            {
                "title": "Division 23 - Mechanical",
                "items": [
                    {"source_file": "/path/specs.pdf", "pages": [200, 201]},
                    {"source_file": "/path/M1.1.pdf"}
                ]
            }
        ]
    }
    """
    package_title = plan_dict.get("package_title", "Bid Package")
    sections: List[Dict[str, any]] = plan_dict.get("sections", [])

    writer = PdfWriter()
    # Keep references to open file objects and readers to prevent GC/closing before write
    open_files = [] 
    
    try:
        for section in sections:
            for item in section.get("items", []):
                source = item.get("source_file")
                if not source or not Path(source).exists():
                    logger.warning(
                        "DocIntel: source file missing during PDF build",
                        extra={"source": source, "section": section.get("title")},
                    )
                    continue

                # Determine how to handle the file based on extension/content
                lower_source = source.lower()
                reader = None

                try:
                    if lower_source.endswith(".pdf"):
                        # Explicitly open file and keep reference
                        f = open(source, "rb")
                        open_files.append(f)
                        reader = PdfReader(f)
                    elif lower_source.endswith((".png", ".jpg", ".jpeg", ".tiff", ".bmp", ".gif")):
                        # Convert image to PDF in-memory
                        try:
                            with Image.open(source) as img:
                                if img.mode != "RGB":
                                    img = img.convert("RGB")
                                pdf_bytes = io.BytesIO()
                                img.save(pdf_bytes, format="PDF")
                                pdf_bytes.seek(0)
                                # Keep the bytes buffer alive
                                open_files.append(pdf_bytes)
                                reader = PdfReader(pdf_bytes)
                        except Exception as e:
                            logger.error(
                                "DocIntel: failed to convert image to PDF",
                                extra={"source": source, "error": str(e)},
                            )
                            continue
                    else:
                        logger.warning(
                            "DocIntel: skipping unsupported file type",
                            extra={"source": source, "section": section.get("title")},
                        )
                        continue

                    if not reader:
                        continue

                    pages = item.get("pages")
                    if pages:
                        for page_number in pages:
                            index = max(0, min(page_number - 1, len(reader.pages) - 1))
                            writer.add_page(reader.pages[index])
                            logger.debug(
                                "DocIntel: added PDF page",
                                extra={
                                    "source": source,
                                    "page_number": page_number,
                                    "section": section.get("title"),
                                },
                            )
                    else:
                        for page in reader.pages:
                            writer.add_page(page)
                        logger.debug(
                            "DocIntel: added entire PDF",
                            extra={"source": source, "page_count": len(reader.pages)},
                        )

                except Exception as e:
                    logger.error(
                        "DocIntel: error processing file for package",
                        extra={"source": source, "error": str(e)},
                    )
                    continue

        output_path = Path(output_dir or tempfile.mkdtemp(prefix="bid_package_"))
        output_path.mkdir(parents=True, exist_ok=True)
        package_path = output_path / f"{_safe_filename(package_title)}.pdf"
        
        try:
            with package_path.open("wb") as stream:
                writer.write(stream)
            logger.info(
                "DocIntel: PDF package written",
                extra={
                    "output_path": str(package_path),
                    "package_title": package_title,
                    "section_count": len(sections),
                    "total_pages": len(writer.pages),
                },
            )
        except Exception as e:
            logger.error(f"DocIntel: Failed to write final PDF package: {e}", exc_info=True)
            # Re-raise or handle gracefully? 
            # If we fail to write, we should probably raise so the user knows.
            raise
            
        return str(package_path)

    finally:
        # Close all open files
        for f in open_files:
            try:
                f.close()
            except Exception:
                pass


def build_zip_package(plan_dict: Dict[str, any], output_dir: Optional[str] = None) -> str:
    """
    Bundles the referenced source files into a single ZIP archive.
    """
    package_title = plan_dict.get("package_title", "Bid Package")
    sections: List[Dict[str, any]] = plan_dict.get("sections", [])

    output_path = Path(output_dir or tempfile.mkdtemp(prefix="bid_package_zip_"))
    output_path.mkdir(parents=True, exist_ok=True)
    archive_path = output_path / f"{_safe_filename(package_title)}.zip"

    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for section in sections:
            for item in section.get("items", []):
                source = item.get("source_file")
                if source and Path(source).exists():
                    arcname = Path(section.get("title", "Section")) / Path(source).name
                    archive.write(source, arcname=str(arcname))
                    logger.debug(
                        "DocIntel: added file to ZIP package",
                        extra={"source": source, "archive_name": str(arcname)},
                    )
                else:
                    logger.warning(
                        "DocIntel: source file missing during ZIP build",
                        extra={"source": source, "section": section.get("title")},
                    )

    logger.info(
        "DocIntel: ZIP package written",
        extra={
            "output_path": str(archive_path),
            "package_title": package_title,
            "section_count": len(sections),
        },
    )

    return str(archive_path)


def _safe_filename(value: str) -> str:
    sanitized = "".join(char if char.isalnum() or char in (" ", "-", "_") else "_" for char in value).strip()
    return sanitized or "package"
