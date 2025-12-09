from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Dict, List

import pdfplumber  # type: ignore


def parse_pdf(file_path: str) -> Dict[str, List[str]]:
    """
    Extracts text (and basic image references) from a PDF file using pdfplumber.
    Returns a dictionary with `text` and `images`.
    """
    text_chunks: List[str] = []
    extracted_images: List[str] = []

    with pdfplumber.open(file_path) as pdf:
        for page_index, page in enumerate(pdf.pages, start=1):
            try:
                text = page.extract_text() or ""
                if text.strip():
                    text_chunks.append(text.strip())
            except Exception:  # noqa: BLE001
                continue

            if page.images:
                extracted_images.extend(
                    [
                        f"page:{page_index}:bbox({img.get('x0')},{img.get('top')},{img.get('x1')},{img.get('bottom')})"
                        for img in page.images
                    ]
                )

    return {
        "text": "\n\n".join(text_chunks),
        "images": extracted_images,
    }

